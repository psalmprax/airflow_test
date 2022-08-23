"""
Data Ingestion for Adamant and WKFS
"""
import os
import csv
import yaml
import shutil
import logging
from docker_job.config import *
from datetime import datetime, timedelta  # , datetime
# import sqlalchemy  # pylint: disable=import-error
from sqlalchemy import create_engine, inspect, MetaData  # pylint: disable=import-error
from airflow import DAG  # pylint: disable=import-error
from airflow.hooks.base_hook import BaseHook  # pylint: disable=import-error
from airflow.hooks.postgres_hook import PostgresHook  # pylint: disable=import-error
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator  # pylint: disable=import-error
from airflow.providers.mysql.hooks.mysql import MySqlHook  # pylint: disable=import-error
from airflow.hooks.S3_hook import S3Hook
from airflow.providers.postgres.operators.postgres import PostgresOperator  # pylint: disable=import-error
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
# from airflow.providers.amazon.aws.operators.s3_delete_objects import S3DeleteObjectsOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator, S3DeleteObjectsOperator


def create_table_from_pandas(
		redshift_cursor,
		conn,
		schema,
		table,
		strategy
):
	import pandas as pd
	column_lists = list()
	columns_dict = dict()
	column_lists_type = list()
	
	src_engine = create_engine(BaseHook.get_hook(conn_id=conn).get_uri())  # pylint: disable=bad-indentation
	src_engine._metadata = MetaData(bind=src_engine)  # pylint: disable=protected-access
	src_engine._metadata.reflect(src_engine)  # pylint: disable=protected-access
	insp = inspect(src_engine)
	columns_table = insp.get_columns(table_name=table)
	if strategy["table_name_with_cotation"]:
		table += "s"
	qry = f"create table if not exists {schema}.{table} ("
	for cols in columns_table:
		column_lists.append(cols['name'])
		if cols['name'] not in strategy["too_long_size_column"]:
			column_lists_type.append(str(type(cols['type'])).split(".")[-1].replace("'>", ""))
			columns_dict[cols['name']] = str(type(cols['type'])).split(".")[-1].replace("'>", "")
	
	logging.debug("##################################### START ******** ########################################")
	logging.debug("COLUMN_LIST: ", column_lists)
	logging.debug("TOO_LONG_SIZE_COLUMN: ", strategy["too_long_size_column"])
	logging.debug("COLUMN_LIST_TYPE: ", column_lists_type)
	logging.debug("COLUMN_DICTIONARY: ", columns_dict)
	logging.debug("##################################### END ******** ########################################")
	for value in airflow_job_metadata:
		columns_dict[value] = 'VARCHAR'
		column_lists.append(value)
	for k, v in columns_dict.items():
		qry += f"{k} VARCHAR(MAX) ENCODE lzo NULL,"
	
	qry = qry.replace(
		f"{airflow_job_metadata[-1]} VARCHAR(MAX) ENCODE lzo NULL,",
		f"{airflow_job_metadata[-1]} varchar(MAX) ENCODE lzo NULL)"
	)
	qry_schema = f"create schema if not exists {schema};"
	logging.debug("create DDL for table: ", qry)
	logging.debug("query to correct quoted column: ", qry_schema)
	redshift_cursor.execute(qry_schema)
	redshift_cursor.execute("COMMIT")
	try:
		redshift_cursor.execute(qry)
		redshift_cursor.execute("COMMIT")
	except Exception as ex:
		logging.debug(ex, " ====> correct quoted column: ", str(ex).split()[5])
		err = str(ex).split()[5].replace('"', '')
		qry = qry.replace(err, str(ex).split()[5])
		redshift_cursor.execute(qry)
		redshift_cursor.execute("COMMIT")
	
	return column_lists


def download_datatable(mysql_cursor, filename_location, query, columns, kwargs, strategy=None) -> None:
	"""

	:param strategy:
	:param kwargs:
	:param columns:
	:param mysql_cursor:
	:param filename_location:
	:param query:
	"""
	import pandas as pd
	import pyarrow as pa
	import pyarrow.parquet as pq
	from fastparquet import write
	import pyarrow.orc as orc
	import numpy as np
	mysql_cursor.execute(query)
	rows = mysql_cursor.fetchall()
	airflow_job_metadata_values = list()
	for meta in airflow_job_metadata:
		airflow_job_metadata_values.append(kwargs[meta])
		print(kwargs[meta])
	
	if rows:
		lists = [[*i, *airflow_job_metadata_values] for i in rows]
		df = pd.DataFrame(lists, columns=columns)
		df.replace(r'^\s*$', '', regex=True, inplace=True)
		df.replace(
			{
				pd.NaT: '',
				np.NaN: '',
				None: '',
				'nan': '',
				'None': '',
				'Nat': '',
				'nan': ''
			},
			inplace=True
		)
		df = df.astype(str)
		if strategy["too_long_size_column"]:
			for coll in strategy["too_long_size_column"]:
				df.drop(coll, axis=1, inplace=True)
		table_from_pandas = pa.Table.from_pandas(df, preserve_index=False)
		pq.write_table(
			table_from_pandas,
			filename_location.replace(".csv", ".parquet"),
			compression='SNAPPY'
		)
		return True
	return False


def mkdir_for_task(table_name, dag_id_folder):
	# Parent and Task Directory path
	directory = table_name
	parent_dir = "/tmp"
	task_dir = os.path.join(parent_dir, dag_id_folder)
	try:
		os.mkdir(task_dir)
	except Exception as ex:
		logging.debug(ex)
		logging.debug(f"The file {task_dir} Exist already")
	
	path = os.path.join(task_dir, directory)
	
	try:
		os.mkdir(path)
	except Exception as ex:
		logging.debug(ex)
		logging.debug(f"The file {path} Exist already")
	
	logging.debug(path)
	return path


def get_data_to_temp(strategy, kwargs, redshift_cursor, mysql_cursor):
	last_id = -10
	start_row_count, end_row_count = False, False
	response = dict()
	response["strategy_qry"] = None
	response["strategy_qry_count"] = None
	response["last_id"] = last_id
	response["start_row_count"] = start_row_count
	response["end_row_count"] = end_row_count
	response["last_id_val"] = -10
	table = None
	
	if strategy["table_name_with_cotation"]:
		table = kwargs["table"] + "s"
	else:
		table = kwargs["table"]
	
	insert_query_cnt = query['source_or_target_last_record_id'].format(
		strategy["primary_key"],
		kwargs['schema'],
		table,
		strategy["primary_key"]
	)
	
	updated_at_insert_query_cnt = query['updated_at_source_or_target_last_record_id'].format(
		strategy["primary_key"],
		kwargs['schema'],
		kwargs["table"],
		strategy["primary_key"],
		strategy["primary_key"]
	
	)
	
	if strategy["strategy"] == "ID_STRATEGY":
		try:
			redshift_cursor.execute(insert_query_cnt)
			last_id_val = int(redshift_cursor.fetchall()[0][0])
			response["last_id_val"] = last_id_val
			logging.debug("last_id_val: ", last_id_val)
			
			last_id_qry = query['last_id_qry'].format(
				kwargs['source_schema'],
				kwargs["table"],
				strategy["primary_key"],
				last_id_val
			)
			mysql_cursor.execute(last_id_qry)
			last_id = int(mysql_cursor.fetchall()[0][0])
			logging.debug("last_id: ", last_id)
		except Exception as ex:
			redshift_cursor.execute("ROLLBACK")
			redshift_cursor.execute("COMMIT")
			logging.debug("No record available for ID_STRATEGY")
			insert_query = f"""select count(*) from {kwargs['source_schema']}.{kwargs["table"]}"""
			logging.debug(insert_query)
			start_row_count = 0
			mysql_cursor.execute(insert_query)
			end_row_count = mysql_cursor.fetchall()[0][0]
			response["last_id"] = last_id
			response["start_row_count"] = start_row_count
			response["end_row_count"] = end_row_count
		if last_id > 0:
			start_row_count = 0
			end_row_count = last_id
			response["last_id"] = last_id
			response["start_row_count"] = start_row_count
			response["end_row_count"] = end_row_count
	
	if strategy["strategy"] == "DATE_STRATEGY":
		
		logging.debug("updated_at_insert_query_cnt: ", updated_at_insert_query_cnt)
		try:
			
			redshift_cursor.execute(updated_at_insert_query_cnt)
			date_filter = redshift_cursor.fetchall()[0][0]
			logging.debug("date_filter: ", date_filter)
			
			source_qry = query['source_fetch_new_update_data_qry'].format(
				kwargs['source_schema'],
				kwargs["table"],
				strategy["primary_key"],
				date_filter,
			)
			source_qry_count = query['source_fetch_new_update_data_qry_count'].format(
				kwargs['source_schema'],
				kwargs["table"],
				strategy["primary_key"],
				date_filter,
			)
			logging.debug("STRATEGY SOURCE QUERY: ", source_qry)
			logging.debug("STRATEGY SOURCE QUERY: ", source_qry_count)
			response["strategy_qry"] = source_qry
			response["strategy_qry_count"] = source_qry_count
		
		except Exception as ex:
			logging.debug("DATE_STRATEGY ERROR: ", ex)
			redshift_cursor.execute("ROLLBACK")
			redshift_cursor.execute("COMMIT")
			logging.debug("No record available for DATE_STRATEGY")
			insert_query = f"""select count(*) from {kwargs['source_schema']}.{kwargs["table"]}"""
			start_row_count = 0
			mysql_cursor.execute(insert_query)
			end_row_count = mysql_cursor.fetchall()[0][0]
			response["last_id"] = last_id
			response["start_row_count"] = start_row_count
			response["end_row_count"] = end_row_count
	
	if strategy["strategy"] == "FULL_STRATEGY":
		insert_query = f"""select count(*) from {kwargs['source_schema']}.{kwargs["table"]}"""
		mysql_cursor.execute(insert_query)
		end_row_count = mysql_cursor.fetchall()[0][0]
		response["strategy_qry"] = None
		response["strategy_qry_count"] = None
		response["last_id"] = 0
		response["start_row_count"] = 0
		response["end_row_count"] = end_row_count
		response["last_id_val"] = -10
		
	return response


def ingestion_process(**kwargs):  # pylint: disable=too-many-locals
	"""
	# Count number of records and use that data download in batch of 100000 if
	# data count is more than 100000 otherwise download all data at once
	:param kwargs:
	"""
	import os
	
	print(kwargs["ti"])
	print(kwargs['conn_id'])
	mysql = MySqlHook(mysql_conn_id=kwargs['conn_id'])
	mysql_connection = mysql.get_conn()
	mysql_cursor = mysql_connection.cursor()
	redshift = PostgresHook(postgres_conn_id="psql_conn")
	redshift = redshift.get_conn()
	redshift_cursor = redshift.cursor()
	strategy = kwargs["strategy"]
	update_query_type = kwargs["strategy"]["update_query_type"]
	logging.debug("STRATEGY_CONFIG: ", str(strategy["strategy"]))
	logging.debug("PRIMARY KEY: ", strategy['primary_key'])
	
	get_data_config = get_data_to_temp(strategy, kwargs, redshift_cursor, mysql_cursor)
	get_download_location_config = mkdir_for_task(kwargs["table"], kwargs["task_id"])
	
	file_name = None
	files = list()
	columns_list_copy = create_table_from_pandas(
		redshift_cursor,
		kwargs['conn_id'],
		kwargs['schema'],
		kwargs['table'],
		strategy
	)
	
	# download data to local folder in the container
	insert_query = None
	try:
		logging.debug(
			"start_row_count: ", get_data_config["start_row_count"], " -----> ",
			"end_row_count: ", get_data_config["end_row_count"]
		)
		logging.debug(
			"last_id: ", get_data_config["last_id"], " -----> ",
			"last_id_val: ", get_data_config["last_id_val"]
		)
		
		if isinstance(get_data_config["start_row_count"], int) and isinstance(get_data_config["end_row_count"], int):
			if update_query_type == "NORMAL":
				logging.debug("UPDATE_QUERY_TYPE: ", update_query_type)
				for start_limit in range(get_data_config["start_row_count"], get_data_config["end_row_count"], 100000):
					logging.debug("STRATEGY_TYPE: ", strategy["strategy"])
					
					logging.debug(
						"LAST_ID_TYPE: ", type(get_data_config["last_id"]), " -----> ",
						"LAST_ID_VALUE_TYPE: ", type(get_data_config["last_id_val"])
					)
					
					if get_data_config["last_id"] >= 0 and get_data_config["last_id_val"] > -10:
						logging.debug("STRATEGY_TYPE: ", strategy["strategy"])
						insert_query = query['last_id_OR_last_id_val'].format(
							kwargs['source_schema'],
							kwargs["table"],
							strategy['primary_key'],
							get_data_config["last_id_val"],
							start_limit
						)
					elif get_data_config["last_id_val"] > -10:
						logging.debug("STRATEGY_TYPE: ", strategy["strategy"])
						insert_query = query['last_id_OR_last_id_val'].format(
							kwargs['source_schema'],
							kwargs["table"],
							strategy['primary_key'],
							get_data_config["last_id_val"],
							start_limit
						)
					elif get_data_config["last_id_val"] == -10:
						insert_query = query['last_id_val_full_refresh'].format(
							kwargs['source_schema'],
							kwargs["table"],
							strategy['primary_key'],
							start_limit
						)
					print(insert_query)
					if insert_query:
						file_name = f"{get_download_location_config}/{kwargs['table']}_{start_limit}.csv"
						status = kwargs['download_tb'](
							mysql_cursor,
							file_name,
							insert_query,
							columns_list_copy,
							kwargs,
							strategy
						)
						print(status)
						if not status:
							break
						files.append(file_name)
			
			else:
				logging.debug("UPDATE_QUERY_TYPE: ", update_query_type)
				for start_limit in range(get_data_config["start_row_count"], get_data_config["end_row_count"], 10000):
					if get_data_config["last_id"] > 0 and get_data_config["last_id_val"] > -10:
						logging.debug("UPDATE_QUERY_TYPE: ", update_query_type)
						insert_query = query['last_id_OR_last_id_val_SPECIAL'].format(
							kwargs['source_schema'],
							kwargs["table"],
							strategy['primary_key'],
							get_data_config["last_id_val"],
							start_limit
						)
					elif get_data_config["last_id_val"] == -10:
						logging.debug("UPDATE_QUERY_TYPE: ", update_query_type)
						insert_query = query['last_id_val_full_refresh_SPECIAL'].format(
							kwargs['source_schema'],
							kwargs["table"],
							start_limit
						)
						logging.debug("INSERT_QUERY: ", insert_query)
					
					if insert_query:
						file_name = f"{get_download_location_config}/{kwargs['table']}_{start_limit}.csv"
						status = kwargs['download_tb'](
							mysql_cursor,
							file_name,
							insert_query,
							columns_list_copy,
							kwargs,
							strategy
						)
						print(status)
						if not status:
							break
						files.append(file_name)
		if isinstance(get_data_config["start_row_count"], bool) \
				and isinstance(get_data_config["end_row_count"], bool):
			if get_data_config["last_id_val"] == -10:
				logging.debug("STRATEGY_QUERY: ", get_data_config["strategy_qry"])
				mysql_cursor.execute(get_data_config["strategy_qry_count"])
				record_count = mysql_cursor.fetchall()[0][0]
				for count_value in range(0, record_count, 100000):
					file_name = f"{get_download_location_config}/{kwargs['table']}_{count_value}_update.csv"
					status = kwargs['download_tb'](
						mysql_cursor, file_name,
						get_data_config["strategy_qry"] + """ limit {}, 100000""".format(count_value),
						columns_list_copy, kwargs, strategy
					)
					if status:
						files.append(file_name)
			else:
				pass
	except Exception as ex:
		logging.debug("DOWNLOAD_ERROR: ", ex)
	
	try:
		extract_strategy = "UPSERT"
		table_name = None
		if strategy["table_name_with_cotation"]:
			table_name = f'{kwargs["table"]}s'
		else:
			table_name = kwargs["table"]
		for file in files:
			create_local_to_s3_job = LocalFilesystemToS3Operator(
				task_id=f"create-local-{file.split('_')[1].replace('/', '-')}-to-s3-job",
				filename=file.replace(".csv", ".parquet"),
				aws_conn_id="s3_conn",
				dest_key=file.replace(".csv", ".parquet"),
				dest_bucket="airsamtest",
				replace=True,
			)
			task_transfer_s3_to_redshift = S3ToRedshiftOperator(
				s3_bucket="airsamtest",
				s3_key=file.replace(".csv", ".parquet"),
				aws_conn_id="s3_conn",
				redshift_conn_id="psql_conn",
				schema=kwargs['schema'],
				table=table_name,
				copy_options=['parquet'],
				method=extract_strategy,
				upsert_keys=strategy["update_column"],
				task_id=f"transfer-s3-to-{file.split('_')[1].replace('/', '-')}-redshift",
			)
			delete_s3_bucket = S3DeleteObjectsOperator(
				task_id=f"delete-s3-{file.split('_')[1].replace('/', '-')}-from-s3",
				bucket="airsamtest",
				keys=file.replace(".csv", ".parquet"),
				aws_conn_id="s3_conn",
			)
			delete_local_data = BashOperator(
				task_id=f"start_clean-local-data-{file.split('_')[1].replace('/', '-')}",
				bash_command=f"rm -rf {file.replace('.csv', '.parquet')}"
			)
			create_local_to_s3_job.execute(dict())
			delete_local_data.execute(dict())
			task_transfer_s3_to_redshift.execute(dict())
			delete_s3_bucket.execute(dict())
	except Exception as ex:
		logging.debug("DATA_WAREHOUSE_LOAD_ERROR: ", ex)
	shutil.rmtree(get_download_location_config)


def create_dag(  # pylint: disable=redefined-outer-name
		dag_id,  # pylint: disable=redefined-outer-name
		schedule,  # pylint: disable=redefined-outer-name
		task_id,  # pylint: disable=redefined-outer-name
		description,  # pylint: disable=redefined-outer-name
		start_date,  # pylint: disable=redefined-outer-name
		default_args,  # pylint: disable=redefined-outer-name
		my_func,  # pylint: disable=redefined-outer-name
		dload,  # pylint: disable=redefined-outer-name
		schema,  # pylint: disable=redefined-outer-name
		source_schema,
		tables,
		conn_id,
		primary_id
):  # pylint: disable=too-many-arguments
	"""

	:param dag_id:
	:param schedule:
	:param task_id:
	:param description:
	:param start_date:
	:param default_args:
	:param my_func:
	:param dload:
	:return:
	"""
	sql = """
			SELECT usename AS role_name,
			CASE
			WHEN usesuper AND usecreatedb THEN
			CAST('superuser, create database' AS pg_catalog.text)
			WHEN usesuper THEN
			CAST('superuser' AS pg_catalog.text)
			WHEN usecreatedb THEN
			CAST('create database' AS pg_catalog.text)
			ELSE
			CAST('' AS pg_catalog.text)
			END role_attributes
			FROM pg_catalog.pg_user
			ORDER BY role_name desc;
	"""
	dag = DAG(
		schedule_interval=schedule,
		default_args=default_args,
		description=description,
		dag_id=dag_id,
		start_date=start_date,
		catchup=False,
		template_searchpath=['/opt/airflow/dags']
	)
	with dag:
		create_product_price_table = PostgresOperator(
			task_id=f"table_{task_id}_creation",
			postgres_conn_id="psql_conn",
			sql=sql
		)
		parent_dir = "/tmp"
		task_dir = os.path.join(parent_dir, task_id)
		for table_name in tables:
			fetch_data_from_adamant = PythonOperator(
				task_id=f"get_{table_name}_from_{source_schema}_dwh",
				provide_context=True,
				python_callable=my_func,
				op_kwargs={
					"table": table_name,
					"task_id": task_id,
					"download_tb": dload,
					"schema": schema,
					"source_schema": source_schema,
					"task_dir": task_dir,
					"conn_id": conn_id,
					"strategy": primary_id[table_name]
					
				}
			)
			fetch_data_from_adamant >> create_product_price_table
	
	return dag


task_id = "test_dwh_orc_v2"

default_args = {
	'depends_on_past': False,
	'email': ['samuel.momoh-olle@asgoodasnew.com'],
	'email_on_failure': False,
	'email_on_retry': False,
	'retries': 40,
	'retry_delay': timedelta(minutes=3),
	'execution_timeout': timedelta(hours=16),
}
schedule_interval = timedelta(minutes=60)
start_date = datetime(2022, 8, 23)

with open("dags/docker_job/data_sources.yml") as file_loc:
	source_table = yaml.load_all(file_loc, Loader=yaml.FullLoader)
	for tbl in source_table:
		for key, val in tbl.items():
			print(key, " -> ", val['table_name'])
			dag_id = f'Job-{key}-orc-v2'
			description = f'A {key} DAG '
			
			globals()[dag_id] = create_dag(
				dag_id=dag_id,
				schedule=schedule_interval,
				task_id=task_id,
				description=description,
				start_date=start_date,
				default_args=default_args,
				my_func=ingestion_process,
				dload=download_datatable,
				schema=val["schema"],
				source_schema=val['source_schema'],
				tables=val['table_name'],
				conn_id=val['conn_id'],
				primary_id=tbl[key]["table_name"]
			
			)
