#!/bin/bash

sudo docker exec -it airflow_test_airflow-worker_1 \
airflow connections add 'mysql_conn' \
    --conn-type 'mysql' \
    --conn-login 'data-analytics' \
    --conn-host 'db.warehouse.asgoodasnew.com' \
    --conn-port '3306' \
    --conn-schema 'adamant';

sudo docker exec -it airflow_test_airflow-worker_1 \
airflow connections add 'mysql_wkfs_conn' \
    --conn-type 'mysql' \
    --conn-login 'data-analytics' \
    --conn-host 'wkfs-production-slave3.cfkok039naoz.eu-central-1.rds.amazonaws.com' \
    --conn-port '3306' \
    --conn-schema 'symfony1';

sudo docker exec -it airflow_test_airflow-worker_1 \
airflow connections add 'mysql_scanpoint_conn' \
    --conn-type 'mysql' \
    --conn-login 'data-analytics' \
    --conn-host 'wkfs-production-slave3.cfkok039naoz.eu-central-1.rds.amazonaws.com' \
    --conn-port '3306' \
    --conn-schema 'scanpoint';

sudo docker exec -it airflow_test_airflow-worker_1 \
airflow connections add 'psql_conn' \
    --conn-type 'redshift' \
    --conn-login 'master' \
    --conn-host '11.3.0.226' \
    --conn-port '5432' \
    --conn-schema 'dwh';

sudo docker exec -it airflow_test_airflow-worker_1 \
airflow connections add 'profiles_key' \
    --conn-type 'fs' \
    --conn-login 'profiles_key';

sudo docker exec -it airflow_test_airflow-worker_1 \
airflow connections add 's3_conn' \
    --conn-type 's3';