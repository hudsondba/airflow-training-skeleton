from datetime import timedelta, datetime
import pendulum

import airflow
from airflow.models import DAG
from airflow.contrib.operators.postgres_to_gcs_operator import PostgresToGoogleCloudStorageOperator

default_args = {"owner": "hudson", "email": "hudson.santos@cg.nl"}


with airflow.DAG(
    dag_id="test_dag_4",
    start_date=datetime(2019, 9, 28),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
) as dag:

    sql  = "select * from land_registry_price_paid_uk"
    bucket_name = 'airflow-training-data-hudson'

    op = PostgresToGoogleCloudStorageOperator(
        task_id='land_registry_price_paid_uk',
        postgres_conn_id='trainingdb',
        sql=sql,
        bucket=bucket_name,
        export_format='CSV',
        filename='land_registry_price_paid_uk.csv')
