import yaml
from datetime import datetime, timedelta  # , datetime

from airflow.models import DAG
from airflow.providers.amazon.aws.transfers.redshift_to_s3 import RedshiftToS3Operator


def query_maker():
	pass


def create_dag(  # pylint: disable=redefined-outer-name
		dag_id,  # pylint: disable=redefined-outer-name
		schedule,  # pylint: disable=redefined-outer-name
		task_id,  # pylint: disable=redefined-outer-name
		description,  # pylint: disable=redefined-outer-name
		start_date,  # pylint: disable=redefined-outer-name
		default_args,  # pylint: disable=redefined-outer-name
		# my_func,  # pylint: disable=redefined-outer-name
		# dload,  # pylint: disable=redefined-outer-name
		schema,  # pylint: disable=redefined-outer-name
		column_to_decrypt,
		# source_schema,
		tables,
		# conn_id,
		# primary_id
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
			redshift_to_s3 = RedshiftToS3Operator(
				task_id=f'{id}',
				s3_bucket='airsamtest',
				s3_key=f'Decrypt/{schema}/{table_name}',
				schema=f'{schema}',
				table=f'{table_name}',
				# select_query
				redshift_conn_id='psql_conn',
				aws_conn_id='s3_conn',
				table_as_file_name=False,
				unload_options=[
					"FORMAT AS PARQUET",
					"ALLOWOVERWRITE",
					"PARALLEL OFF",
				]
			)
			redshift_to_s3.execute(dict())
	
	return dag
	pass


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
				# my_func=ingestion_process,
				# dload=download_datatable,
				schema=val["schema"],
				column_to_decrypt=list(val['table_name'].values())[0].get("column_to_decrypt"),
				# source_schema=val['source_schema'],
				tables=val['table_name'],
				# conn_id=val['conn_id'],
				# primary_id=tbl[key]["table_name"]
			
			)
