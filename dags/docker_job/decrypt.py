import yaml
import os
import logging
from datetime import datetime, timedelta  # , datetime
from airflow.models import DAG
from airflow.providers.amazon.aws.transfers.redshift_to_s3 import RedshiftToS3Operator
from hello_operator import HelloOperator
from s3_to_local_system import S3toLocalSystemOperator


def query_maker(table_name, schema, not_null_column):
	column = str()
	for col in range(len(not_null_column)):
		if (col + 1) != len(not_null_column):
			column += not_null_column[col] + " is not null and "
		else:
			column += not_null_column[col] + " is not null"
	qry = f"select * from {schema}.{table_name} where " + column
	return qry


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


def create_dag(  # pylint: disable=redefined-outer-name
		dag_id,  # pylint: disable=redefined-outer-name
		schedule,  # pylint: disable=redefined-outer-name
		task_id,  # pylint: disable=redefined-outer-name
		description,  # pylint: disable=redefined-outer-name
		start_date,  # pylint: disable=redefined-outer-name
		default_args,  # pylint: disable=redefined-outer-name
		schema,  # pylint: disable=redefined-outer-name
		column_to_decrypt,
		column_with_decrypt_value,
		tables,
		conn_id,
		s3_conn_id,  # primary_id
):  # pylint: disable=too-many-arguments
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
		for table_name in tables:
			prefix = "Decrypt"
			id = f'{prefix}000{schema.replace("_", "000")}' \
					f'{table_name.replace(".", "000").replace("_", "000")}'.replace("\"", "")
			print(query_maker(table_name, schema, column_with_decrypt_value))
			# redshift_to_s3 = RedshiftToS3Operator(
			# 	task_id=f'{id}',
			# 	s3_bucket='airsamtest',
			# 	s3_key=f'Decrypt/{schema}/{table_name}',
			# 	# schema=f'{schema}',
			# 	# table=f'{table_name}',
			# 	select_query=query_maker(table_name, schema, column_with_decrypt_value),
			# 	redshift_conn_id='psql_conn',
			# 	aws_conn_id='s3_conn',
			# 	table_as_file_name=False,
			# 	unload_options=[
			# 		"FORMAT AS PARQUET",
			# 		"ALLOWOVERWRITE",
			# 		"PARALLEL OFF",
			# 	]
			# )
			path = mkdir_for_task(f"{table_name}", f"{schema}")
			prefix = "Decrypt"
			s3_to_local = S3toLocalSystemOperator(
				task_id=f'download-{id}-to-local',
				s3_conn_id='s3_conn',
				s3_bucket='airsamtest',
				s3_key='Decrypt/analytics_objects/"obj_wkfs.payment_direct_debit_swift_cleaned"000.parquet',  #
				# f'Decrypt/{schema}/{table_name}',
				local_file_path=path
			)
			# redshift_to_s3.execute(dict())
			s3_to_local.execute(dict())
		
		# hello_task = HelloOperator(task_id="sample-task", name="foo_bar")
	
	return dag


task_id = "decrypt"

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
start_date = datetime(2022, 9, 5)

with open("dags/docker_job/decrypt.yml") as file_loc:
	# with open("decrypt.yml") as file_loc:
	source_table = yaml.load_all(file_loc, Loader=yaml.FullLoader)
	for tbl in source_table:
		for key, val in tbl.items():
			print(key, " -> ", val['table_name'])
			print(key, " -> ", val['schema'])
			dag_id = f'Job-{key}-payment-decrypt'
			description = f'A {key} Payment Decrypt DAG '
			for tbl in val['table_name']:
				print(key, " -> ", tbl)
				print(list(val['table_name'].values())[0].get("column_to_decrypt"))
			
			globals()[dag_id] = create_dag(
				dag_id=dag_id,
				schedule=schedule_interval,
				task_id=task_id,
				description=description,
				start_date=start_date,
				default_args=default_args,
				schema=val["schema"],
				column_to_decrypt=list(val['table_name'].values())[0].get("column_to_decrypt"),
				column_with_decrypt_value=list(val['table_name'].values())[0].get("column_with_decrypt_value"),
				tables=val['table_name'],
				conn_id=val['conn_id'],
				s3_conn_id=val['s3_conn_id'],
				# primary_id=tbl[key]["table_name"]
			
			)
