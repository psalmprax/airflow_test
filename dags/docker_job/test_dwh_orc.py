"""
Data Ingestion for Adamant and WKFS
"""
import csv
import os
import shutil
from datetime import datetime, timedelta  # , datetime
# import sqlalchemy  # pylint: disable=import-error
from sqlalchemy import create_engine, inspect, MetaData  # pylint: disable=import-error
from airflow import DAG  # pylint: disable=import-error
from airflow.hooks.base_hook import BaseHook  # pylint: disable=import-error
from airflow.hooks.postgres_hook import PostgresHook  # pylint: disable=import-error
from airflow.operators.python_operator import PythonOperator  # pylint: disable=import-error
from airflow.providers.mysql.hooks.mysql import MySqlHook  # pylint: disable=import-error
from airflow.hooks.S3_hook import S3Hook
from airflow.providers.postgres.operators.postgres import PostgresOperator  # pylint: disable=import-error
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.amazon.aws.operators.s3_delete_objects import S3DeleteObjectsOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator


def create_table_from_pandas(query_for_schema, mysql_cursor, redshift_cursor, conn, schema, table):
	import pandas as pd
	column_lists = list()
	columns_dict = dict()
	column_lists_type = list()
	# data_types = {
	# 	'Int64': 'BIGINT ENCODE lzo',
	# 	'Float64': 'DOUBLE PRECISION ENCODE',
	# 	'string': 'varchar(MAX) ENCODE lzo',
	# 	'datetime64[ns]': 'DATETIME',
	# 	'object': 'varchar(MAX) ENCODE lzo'
	# }
	
	data_types = {
		'INTEGER': 'BIGINT ENCODE lzo',
		'TINYINT': 'BIGINT ENCODE lzo',
		'SMALLINT': 'BIGINT ENCODE lzo',
		'MEDIUMINT': 'BIGINT ENCODE lzo',
		'INT': 'BIGINT ENCODE lzo',
		'BIGINT': 'BIGINT ENCODE lzo',
		'DECIMAL': 'DOUBLE PRECISION ENCODE lzo',
		'FLOAT': 'REAL ENCODE lzo',
		'DOUBLE': 'DOUBLE PRECISION ENCODE',
		'CHAR': 'CHAR ENCODE lzo',
		'YEAR': 'VARCHAR(MAX) ENCODE lzo',
		'VARCHAR': 'VARCHAR(MAX) ENCODE lzo',
		'BIT': 'VARCHAR(MAX) ENCODE lzo',
		'BINARY': 'VARCHAR(MAX) ENCODE lzo',
		'VARBINARY': 'VARCHAR(MAX) ENCODE lzo',
		'TINYTEXT': 'VARCHAR(MAX) ENCODE lzo',
		'MEDIUMTEXT': 'VARCHAR(MAX) ENCODE lzo',
		'TEXT': 'VARCHAR(MAX) ENCODE lzo',
		'ENUM': 'VARCHAR(MAX) ENCODE lzo',
		'SET': 'VARCHAR(MAX) ENCODE lzo',
		'SPATIAL': 'VARCHAR(MAX) ENCODE lzo',
		'DATETIME': 'DATETIME ENCODE lzo',
		'DATE': 'DATE ENCODE lzo',
		'TIME': 'TIME ENCODE lzo',
		'TIMESTAMP': 'TIMESTAMP ENCODE lzo',

	}
	src_engine = create_engine(BaseHook.get_hook(conn_id=conn).get_uri())  # pylint: disable=bad-indentation
	src_engine._metadata = MetaData(bind=src_engine)  # pylint: disable=protected-access
	src_engine._metadata.reflect(src_engine)  # pylint: disable=protected-access
	insp = inspect(src_engine)
	columns_table = insp.get_columns(table_name=table)
	qry = f"create table if not exists {schema}.{table} ("
	for cols in columns_table:
		column_lists.append(cols['name'])
		column_lists_type.append(str(type(cols['type'])).split(".")[-1].replace("'>", ""))
		columns_dict[cols['name']] = str(type(cols['type'])).split(".")[-1].replace("'>", "")
	
	columns_dict['agan_ingestion_date'] = 'VARCHAR'
	column_lists.append("agan_ingestion_date")
	mysql_cursor.execute(query_for_schema)
	rows = mysql_cursor.fetchall()
	
	print("##################################### START ******** ########################################")
	print(column_lists)
	print(column_lists_type)
	print(columns_dict)
	print("##################################### END ******** ########################################")
	
	lists = [[*i, datetime.today().strftime("%Y-%m-%d %H:%M:%S%Z")] for i in rows]
	print(lists[:5])
	df = pd.DataFrame(lists, columns=column_lists)
	
	for k, v in df.dtypes.items():
		# qry += f"{list(k)[0]} {data_types[str(v)]} NULL,"
		print(f"{k} @@@@@@@@@@@@@@@@@@@@@@@@@  ********************** @@@@@@@@@@@@@@@@@@@@@@@ {v} NULL,")
	# df = df.convert_dtypes()
	
	# for k, v in df.dtypes.items():
	# 	# print(f"{k} @@@@@@@@@@@@@@@@@@@@@@@@@  ********************** @@@@@@@@@@@@@@@@@@@@@@@ {v} NULL,")
	#
	# 	print(f"{k} {data_types[str(v)]} NULL,")
	# 	qry += f"{k} {data_types[str(v)]} NULL,"
	
	for k , v in columns_dict.items():
		print(f"{k} {v} NULL,")
		qry += f"{k} {data_types[str(v)]} NULL,"
		
		print(k, " ======================> ", v)
	# del(df)
	# qry += "agan_ingestion_date VARCHAR(MAX) NULL )"
	# change here when adding additional metal data
	qry = qry.replace("agan_ingestion_date VARCHAR(MAX) ENCODE lzo NULL,", "agan_ingestion_date varchar(MAX) ENCODE lzo NULL)")
	qry_schema = f"create schema if not exists {schema};"
	print(qry)
	print(qry_schema)
	redshift_cursor.execute(qry_schema)
	redshift_cursor.execute("COMMIT")
	try:
		redshift_cursor.execute(qry)
		redshift_cursor.execute("COMMIT")
	except Exception as ex:
		print(ex)
		print(str(ex).split()[5])
		err = str(ex).split()[5].replace('"', '')
		qry = qry.replace(err, str(ex).split()[5])
		redshift_cursor.execute(qry)
		redshift_cursor.execute("COMMIT")
	
	return column_lists


# from airflow.hooks.postgres_hook import PostgresHook
def get_schema_create_table_rs(table, conn, pgsql_cursor, schema=None) -> None:
	# pylint: disable=bad-indentation
	"""
	:param schema:
	:param table:
	:param conn:
	:param pgsql_cursor:
	"""
	# change this for your source database
	src_engine = create_engine(BaseHook.get_hook(conn_id=conn).get_uri())  # pylint: disable=bad-indentation
	src_engine._metadata = MetaData(bind=src_engine)  # pylint: disable=protected-access
	# noinspection PyProtectedMember
	# get columns from existing table
	column_lists = list()
	src_engine._metadata.reflect(src_engine)  # pylint: disable=protected-access
	insp = inspect(src_engine)
	columns_table = insp.get_columns(table_name=table)
	qry = f"create table if not exists {schema}.{table} ("
	for cols in columns_table:
		if "INT11" in str(type(cols['type'])):
			qry += f"{cols['name']} BIGINT NULL,"
		# elif "INTEGER" in str(type(cols['type'])):
		# 	qry += f"{cols['name']} INTEGER NULL,"
		elif "INT" in str(type(cols['type'])) or "BIT" in str(type(cols['type'])):
			qry += f"{cols['name']} INT8 NULL,"
		elif "VARCHAR(255)" in str(type(cols['type'])):
			qry += f"{cols['name']} VARCHAR(MAX) NULL,"
		elif "VARCHAR(200)" in str(type(cols['type'])):
			qry += f"{cols['name']} VARCHAR(200) NULL,"
		
		# elif "CHAR" in str(type(cols['type'])) or "TEXT" in str(type(cols['type'])):
		# 	qry += f"{cols['name']} VARCHAR(MAX) NULL,"
		elif "LONGTEXT" in str(type(cols['type'])):
			qry += f"{cols['name']} VARCHAR(MAX) NULL,"
		elif "DECIMAL" in str(type(cols['type'])) \
				or "FLOAT" in str(type(cols['type'])) \
				or "DOUBLE" in str(type(cols['type'])):
			qry += f"{cols['name']} NUMERIC NULL,"
		elif "DATE" in str(type(cols['type'])) \
				or "TIME" in str(type(cols['type'])) \
				or "TIMESTAMP" in str(type(cols['type'])) \
				or "YEAR" in str(type(cols['type'])) \
				or "DATETIME" in str(type(cols['type'])):
			qry += f"{cols['name']} DATETIME NULL,"
		column_lists.append(cols["name"])
		print(cols['name'], " ", cols['type'])
	qry += "agan_ingestion_date VARCHAR(MAX) NULL )"
	column_lists.append("agan_ingestion_date")
	print(qry)
	qry_schema = f"create schema if not exists {schema};"
	print(qry_schema)
	pgsql_cursor.execute(qry_schema)
	pgsql_cursor.execute("COMMIT")
	try:
		pgsql_cursor.execute(qry)
		pgsql_cursor.execute("COMMIT")
	except Exception as ex:
		print(ex)
		print(str(ex).split()[5])
		err = str(ex).split()[5].replace('"', '')
		qry = qry.replace(err, str(ex).split()[5])
		pgsql_cursor.execute(qry)
		pgsql_cursor.execute("COMMIT")
	# try:
	# 	column_lists.remove("updated_at")
	# except Exception as ex:
	# 	print(ex)
	return column_lists


def download_datatable(mysql_cursor, filename_location, query, columns) -> None:
	"""

	:param columns:
	:param mysql_cursor:
	:param filename_location:
	:param query:
	"""
	import pandas as pd
	import pyarrow as pa
	import pyarrow.parquet as pq
	from fastparquet import write
	mysql_cursor.execute(query)
	rows = mysql_cursor.fetchall()
	print("##################################### START ########################################")
	print(columns)
	print("##################################### END ########################################")
	
	if rows:
		list = [[*i, datetime.today().strftime("%Y-%m-%d %H:%M:%S%Z")] for i in rows]
		# print(list[:10])
		df = pd.DataFrame(list, columns=columns)
		# write(filename_location.replace(".csv", ".parq"), df)
		df = df.convert_dtypes()
		
		print(df.head(5))
		table_from_pandas = pa.Table.from_pandas(df, preserve_index=False)
		pq.write_table(table_from_pandas, filename_location.replace(".csv", ".parquet"))
		print(df.dtypes)
		for k, v in df.dtypes.items():
			print(k, " ======================> ", v)
		print(pq.ParquetFile(filename_location.replace(".csv", ".parquet")).schema)
		print(pq.ParquetFile(filename_location.replace(".csv", ".parquet")).read_row_group(0))
		# print(pq.ParquetFile(filename_location.replace(".csv", ".parq")).schema)
		# print(pq.ParquetFile(filename_location.replace(".csv", ".parq")).read_row_group(0))
		return True
	return False


# with open(filename_location, "w", newline="") as file:
# 	writer = csv.writer(file)
# 	writer.writerows(list)


def test(**kwargs):
	print(kwargs.keys())
	for k in kwargs.keys():
		print(k, " -----------------------> ", kwargs[k])


def mkdir_for_task(table_name, dag_id_folder):
	# Parent and Task Directory path
	directory = table_name
	parent_dir = "/tmp"
	task_dir = os.path.join(parent_dir, dag_id_folder)
	try:
		os.mkdir(task_dir)
	except Exception as ex:
		print(f"The file {task_dir} Exist already")
	
	path = os.path.join(task_dir, directory)
	
	try:
		os.mkdir(path)
	except Exception as ex:
		print(f"The file {path} Exist already")
	
	print(path)
	return path


def get_data_to_temp(strategy, kwargs, redshift_cursor, mysql_cursor):
	last_id = -10
	start_row_count, end_row_count = False, False
	response = dict()
	response["strategy_qry"] = None
	response["last_id"] = last_id
	response["start_row_count"] = start_row_count
	response["end_row_count"] = end_row_count
	response["last_id_val"] = -10
	
	insert_query = f"""select {strategy["primary_key"]} from {kwargs['source_schema']}.{kwargs["table"]} order by
					{strategy["primary_key"]} desc limit 1"""  ## select from source table
	if strategy["strategy"] == "ID_STRATEGY":
		try:
			insert_query_cnt = f"""select {strategy["primary_key"]} from {kwargs['schema']}.
								{kwargs["table"]} order by {strategy["primary_key"]} desc limit
								1"""  ## select from target dwh table
			redshift_cursor.execute(insert_query_cnt)
			last_id_val = redshift_cursor.fetchall()[0][0]
			response["last_id_val"] = last_id_val
			print("last_id_val: ", last_id_val)
			last_id_qry = f"""select count(*) from {kwargs['source_schema']}.{kwargs["table"].split('.')[0]}
								where {strategy["primary_key"]}  > {last_id_val}"""
			mysql_cursor.execute(last_id_qry)
			last_id = mysql_cursor.fetchall()[0][0]
			print("last_id: ", last_id)
		# response["last_id"] = last_id
		
		except Exception as ex:
			redshift_cursor.execute("ROLLBACK")
			redshift_cursor.execute("COMMIT")
			print("No record available for ID_STRATEGY")
			start_row_count = 0
			mysql_cursor.execute(insert_query)
			end_row_count = mysql_cursor.fetchall()[0][0]
			response["last_id"] = last_id
			response["start_row_count"] = start_row_count
			response["end_row_count"] = end_row_count
		print(start_row_count, " ----------------- > ", end_row_count)
		if last_id > 0:
			start_row_count = 0
			end_row_count = last_id
			response["last_id"] = last_id
			response["start_row_count"] = start_row_count
			response["end_row_count"] = end_row_count
		print(start_row_count, " ----------------- > ", end_row_count)
	
	if strategy["strategy"] == "DATE_STRATEGY":
		
		try:
			
			insert_query_cnt = f"""select {strategy["primary_key"]} from {kwargs['schema']}.
								{kwargs["table"]} order by
								{strategy["primary_key"]} desc limit 1"""
			redshift_cursor.execute(insert_query_cnt)
			date_filter = redshift_cursor.fetchall()[0][0]
			
			source_qry = f"""select * from {kwargs['source_schema']}.{kwargs["table"]} where
							{strategy["primary_key"]} > '{date_filter}'
							order by {strategy["primary_key"]}"""
			print("STRATEGY SOURCE QUERY: ", source_qry)
			response["strategy_qry"] = source_qry
		
		except Exception as ex:
			print(ex)
			redshift_cursor.execute("ROLLBACK")
			redshift_cursor.execute("COMMIT")
			print("No record available for DATE_STRATEGY")
			insert_query = f"""select count(*) from {kwargs['source_schema']}.{kwargs["table"]}"""
			start_row_count = 0
			mysql_cursor.execute(insert_query)
			end_row_count = mysql_cursor.fetchall()[0][0]
			response["last_id"] = last_id
			response["start_row_count"] = start_row_count
			response["end_row_count"] = end_row_count
	
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
	print(type(strategy))
	print(strategy['primary_key'])
	
	get_data_config = get_data_to_temp(strategy, kwargs, redshift_cursor, mysql_cursor)
	get_download_location_config = mkdir_for_task(kwargs["table"], kwargs["task_id"])
	
	file_name = None
	files = list()
	# columns_list_copy = kwargs['get_create_schema'](
	# 	kwargs['table'], kwargs['conn_id'],
	# 	redshift_cursor,
	# 	kwargs['schema'])
	
	query_for_schema = f"""select * from {kwargs['source_schema']}.{kwargs['table']} limit 10"""
	columns_list_copy = create_table_from_pandas(
		query_for_schema,
		mysql_cursor,
		redshift_cursor,
		kwargs['conn_id'],
		kwargs['schema'],
		kwargs['table']
	)
	
	# download data to local folder in the container
	insert_query = None
	try:
		print(get_data_config["start_row_count"], " -----> ", get_data_config["end_row_count"])
		if isinstance(get_data_config["start_row_count"], int) and isinstance(get_data_config["end_row_count"], int):
			print("Using ID_STRATEGY !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
			
			for start_limit in range(get_data_config["start_row_count"], get_data_config["end_row_count"], 100000):
				if get_data_config["last_id"] > 0 and get_data_config["last_id_val"] > -10:
					insert_query = f"""select * from (select * from {kwargs['source_schema']}.{kwargs["table"]}
									where {strategy['primary_key']} > {get_data_config["last_id_val"]} order by
									{strategy['primary_key']} asc)  as tbl limit 0,
									{get_data_config["end_row_count"]}"""
				elif get_data_config["last_id_val"] == -10:
					insert_query = f"""select * from (select * from {kwargs['source_schema']}.{kwargs["table"]}
					order by {strategy['primary_key']} asc)  as tbl limit {start_limit}, 100000 """
				
				if insert_query:
					print(insert_query)
					# print(file_name)
					file_name = f"{get_download_location_config}/{kwargs['table']}_{start_limit}.csv"
					status = kwargs['download_tb'](mysql_cursor, file_name, insert_query, columns_list_copy)
					print(status)
					if status:
						files.append(file_name)
		print("####################### STAAAARTTTTTTTT ##########################")
		if isinstance(get_data_config["start_row_count"], bool) \
				and isinstance(get_data_config["end_row_count"], bool) \
				and get_data_config["last_id_val"] == -10:
			print("Using DATE_STRATEGY !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
			print(get_data_config["strategy_qry"])
			# file_name = f"{get_download_location_config}/{kwargs['table']}_update.csv"
			# files.append(file_name)
			file_name = f"{get_download_location_config}/{kwargs['table']}_update.csv"
			status = kwargs['download_tb'](mysql_cursor, file_name, get_data_config["strategy_qry"], columns_list_copy)
			if status:
				files.append(file_name)
		print(file_name)
		print(files, " STRATEGY: ", extract_strategy)
	
	except Exception as ex:
		print(ex, "ERRRRORRRRRRRRRR")
		print(get_data_config["strategy_qry"])
	
	# file_name = f"{get_download_location_config}/{kwargs['table']}_update.csv"
	# files.append(file_name)
	# kwargs['download_tb'](mysql_cursor, file_name, get_data_config["strategy_qry"])
	try:
		extract_strategy = "UPSERT"
		for file in files:
			print(file.split("/"), " STRATEGY: ", extract_strategy)
			create_local_to_s3_job = LocalFilesystemToS3Operator(
				task_id=f"create-local-{file.split('_')[1].replace('/', '-')}-to-s3-job",
				filename=file.replace(".csv", ".parquet"),
				aws_conn_id="s3_conn",
				dest_key=file.replace(".csv", ".parquet"),
				dest_bucket="airsamtest",
				replace=True,
			)
			if isinstance(get_data_config["start_row_count"], int) \
					and isinstance(get_data_config["end_row_count"], int):
				task_transfer_s3_to_redshift = S3ToRedshiftOperator(
					s3_bucket="airsamtest",
					s3_key=file.replace(".csv", ".parquet"),
					aws_conn_id="s3_conn",
					redshift_conn_id="psql_conn",
					schema=kwargs['schema'],
					table=kwargs['table'],
					copy_options=['parquet'],
					# method="UPSERT",
					# upsert_keys=columns_list_copy,
					task_id=f"transfer-s3-to-{file.split('_')[1].replace('/', '-')}-redshift",
				)
			if isinstance(get_data_config["start_row_count"], bool) \
					and isinstance(get_data_config["end_row_count"], bool):
				task_transfer_s3_to_redshift = S3ToRedshiftOperator(
					s3_bucket="airsamtest",
					s3_key=file.replace(".csv", ".parquet"),
					aws_conn_id="s3_conn",
					redshift_conn_id="psql_conn",
					schema=kwargs['schema'],
					table=kwargs['table'],
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
			create_local_to_s3_job.execute(dict())
			task_transfer_s3_to_redshift.execute(dict())
			delete_s3_bucket.execute(dict())
	except Exception as ex:
		print(ex, "NOOOOOOOOOOOOOOOOOOOOOOOOOO")
	shutil.rmtree(get_download_location_config)


def create_dag(  # pylint: disable=redefined-outer-name
		dag_id,  # pylint: disable=redefined-outer-name
		schedule,  # pylint: disable=redefined-outer-name
		task_id,  # pylint: disable=redefined-outer-name
		description,  # pylint: disable=redefined-outer-name
		start_date,  # pylint: disable=redefined-outer-name
		default_args,  # pylint: disable=redefined-outer-name
		my_func,  # pylint: disable=redefined-outer-name
		create_schema,  # pylint: disable=redefined-outer-name
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
	:param create_schema:
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
		for table_name in tables:  # ["address", "business_channel", "device", "invoice"]:
			fetch_data_from_adamant = PythonOperator(
				task_id=f"get_{table_name}_from_adamant_dwh",
				provide_context=True,
				python_callable=my_func,
				op_kwargs={
					"table": table_name,
					"task_id": task_id,
					"get_create_schema": create_schema,
					"download_tb": dload,
					"schema": schema,
					"source_schema": source_schema,
					"task_dir": task_dir,
					"conn_id": conn_id,
					"strategy": primary_id[table_name]  # ,
					# "strategy": primary_id[table_name]["strategy"]
					
				}
			)
			fetch_data_from_adamant >> create_product_price_table
	# shutil.rmtree(os.path.join(task_dir, table_name.split('.')[0]))
	
	return dag


task_id = "test_dwh_orc"

default_args = {
	'depends_on_past': False,
	'email': ['samuel.momoh-olle@asgoodasnew.com'],
	'email_on_failure': False,
	'email_on_retry': False,
	'retries': 10,
	'retry_delay': timedelta(minutes=3),
	'execution_timeout': timedelta(seconds=200000),
}
schedule_interval = timedelta(minutes=150)
start_date = datetime(2022, 7, 5)

import yaml

with open("dags/docker_job/data_sources.yml") as file_loc:
	source_table = yaml.load_all(file_loc, Loader=yaml.FullLoader)
	for tbl in source_table:
		for key, val in tbl.items():
			print(key, " -> ", val['table_name'])
			dag_id = f'Job-{key}-orc'
			description = f'A {key} DAG '
			
			globals()[dag_id] = create_dag(
				dag_id=dag_id,
				schedule=schedule_interval,
				task_id=task_id,
				description=description,
				start_date=start_date,
				default_args=default_args,
				my_func=ingestion_process,  # mysql_conn,
				create_schema=get_schema_create_table_rs,
				dload=download_datatable,
				schema=val["schema"],
				source_schema=val['source_schema'],
				tables=val['table_name'],
				conn_id=val['conn_id'],
				primary_id=tbl[key]["table_name"]
			
			)
