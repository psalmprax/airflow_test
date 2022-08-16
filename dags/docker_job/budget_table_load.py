from __future__ import print_function

import random
import time
from datetime import timedelta, datetime, date

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator


def create_dag(
		dag_id,
		schedule,
		task_id,
		description,
		start_date,
		default_args,
		file,
		schema,
		table_name
		# my_func
):
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
		task_transfer_s3_to_redshift = S3ToRedshiftOperator(
			s3_bucket="airsamtest",
			s3_key="budget_tables/" + file,
			aws_conn_id="s3_conn",
			redshift_conn_id="psql_conn",
			schema=schema,
			table=table_name,
			copy_options=['parquet'],
			method=extract_strategy,
			upsert_keys=strategy["update_column"],
			task_id=f"transfer-s3-to-{file.split('_')[1].replace('/', '-')}-redshift",
		)
		pass
	return dag


task_id = "budget_table_load"

default_args = {
	'depends_on_past': False,
	'email': ['samuel.momoh-olle@asgoodasnew.com'],
	'email_on_failure': False,
	'email_on_retry': False,
	'retries': 250,
	'retry_delay': timedelta(minutes=3),
	'execution_timeout': timedelta(seconds=200000),
}
description = f'A {task_id} DAG '
schedule_interval = timedelta(minutes=150)
dag_id = f'Job-{task_id}'
start_date = datetime(2022, 7, 2)
file_location = [
	"budget_2022_brand_targets",
	"budget_2022_category_targets",
	"budget_2022_channel_targets",
	"budget_2022_condition_targets",
	"budget_2022_monthly_target_new",
	"budget_2022_monthly_targets"
]
for file_name in file_location:
	globals()[dag_id] = create_dag(
		dag_id=dag_id,
		schedule=schedule_interval,
		task_id=task_id,
		description=description,
		start_date=start_date,
		default_args=default_args,
		file=file_name,
		schema=schema,
		table_name=table_name
		# my_func=run_from_class
	)
