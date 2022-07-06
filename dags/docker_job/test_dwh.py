import time
from datetime import timedelta, datetime, date

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator


# from airflow.hooks.postgres_hook import PostgresHook

def create_dag(
        dag_id,
        schedule,
        task_id,
        description,
        start_date,
        default_args,
        my_func
):
    sql = """SELECT usename AS role_name,
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
    dag = DAG(schedule_interval=schedule,
              default_args=default_args,
              description=description,
              dag_id=dag_id,
              start_date=start_date,
              catchup=False,
              template_searchpath=['/opt/airflow/dags'])
    with dag:
        create_product_price_table = PostgresOperator(
            task_id=f"table_{task_id}_creation",
            postgres_conn_id="new_dwh_conn",
            sql=sql
        )
        create_product_price_table
    return dag


task_id = "test_dwh"

default_args = {
    'depends_on_past': False,
    'email': ['samuel.momoh-olle@asgoodasnew.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
    'execution_timeout': timedelta(seconds=200000),
}
description = f'A {task_id} DAG '
schedule_interval = timedelta(minutes=150)
dag_id = f'Job-{task_id}'
start_date = datetime(2022, 7, 5)

globals()[dag_id] = create_dag(dag_id=dag_id,
                               schedule=schedule_interval,
                               task_id=task_id,
                               description=description,
                               start_date=start_date,
                               default_args=default_args,
                               my_func='run_from_class')
