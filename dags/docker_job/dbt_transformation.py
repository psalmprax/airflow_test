from __future__ import print_function

from datetime import datetime, timedelta
from airflow.hooks.base import BaseHook
from airflow import DAG
from airflow.operators.bash_operator import BashOperator


# https://stackoverflow.com/questions/45280650/store-and-access-password-using-apache-airflow

def create_dag(dag_id,
               schedule,
               dbt_model,
               description,
               start_date,
               default_args):
    # files_to_copy = list()
    Mygit = BaseHook.get_connection('profiles_key')

    dag = DAG(schedule_interval=schedule,
              default_args=default_args,
              description=description,
              dag_id=dag_id,
              start_date=start_date,
              catchup=False,
              template_searchpath=['/opt/airflow/dag']
              )

    with dag:
        dbt_clone_and_transform = BashOperator(
            task_id=f'dbt_clone_and_transform-{dbt_model}',
            bash_command=f" rm -rf /tmp/{dbt_model} && mkdir /tmp/{dbt_model}\
                            && cp -r /tmp/dbt /tmp/{dbt_model}/dbt \
                            && cd /tmp/{dbt_model}/dbt && python3 -m venv venv \
                            && unzip -o -j -P {Mygit.password} profiles.zip \
                            && export PIP_USER=false \
                            && source /tmp/{dbt_model}/dbt/venv/bin/activate \
                            && pip  install --quiet --upgrade pip setuptools \
                            && pip3 install --quiet 'MarkupSafe<=2.0.1' 'dbt-redshift' 'dbt-core' \
                            && dbt --version && deactivate \
                            && cd /tmp/{dbt_model}/dbt \
                            && source /tmp/{dbt_model}/dbt/venv/bin/activate \
                            && dbt deps --profiles-dir /tmp/{dbt_model}/dbt/ && dbt seed --profiles-dir /tmp/{dbt_model}/dbt/ \
                            && dbt run --models {dbt_model} --profiles-dir /tmp/{dbt_model}/dbt/"
        )

        dbt_cleanup = BashOperator(
            task_id=f'dbt_cleanup-{dbt_model}',
            bash_command=f"rm -rf /tmp/{dbt_model} && rm -rf /tmp/tmp* 2>/dev/null || exit 0"
        )

        dbt_clone_and_transform >> dbt_cleanup

    return dag


dbt_models = ["example",
              "objects",
              "calculations",
              "reporting",
              "reports",
              "objects_long_run",
              "reporting_long_run"]
for dbt_mdl in dbt_models:
    default_args = {
        'depends_on_past': False,
        'email': ['samuelolle@yahoo.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 40,
        'retry_delay': timedelta(minutes=2),
        'execution_timeout': timedelta(seconds=20000),
    }
    description = f'A {dbt_mdl} DAG '
    schedule_interval = timedelta(hours=5)
    dag_id = f'T-dbt-job-{dbt_mdl}'
    start_date = datetime(2022, 4, 1)

    globals()[dag_id] = create_dag(dag_id=dag_id,
                                   schedule=schedule_interval,
                                   dbt_model=dbt_mdl,
                                   description=description,
                                   start_date=start_date,
                                   default_args=default_args)

