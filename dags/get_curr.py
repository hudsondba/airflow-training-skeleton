from operators import http_to_gcs_operator
from datetime import timedelta, datetime
import airflow


import pendulum

default_args = {"owner": "hudson", "email": "hudson.santos@cg.nl"}


with airflow.DAG(
    dag_id="currency_dag",
    start_date=datetime(2019, 9, 28),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
) as dag:

    bucket_name = "airflow-training-data-hudson"

    currency = "EUR"

    START_DATE = pendulum.yesterday().to_datetime_string()
    UNTILL_DATE = pendulum.today().to_datetime_string()

    http_to_gcs_operator.HttpToGcsOperator(
        task_id="get_currency_" + currency,
        method="GET",
        #endpoint="/history?start_at={START_DATE}&end_at={UNTILL_DATE}&symbols={currency}&base=GBP".format(START_DATE=START_DATE,UNTILL_DATE=UNTILL_DATE,currency=currency),
        endpoint="/history?start_at={{yesterday_ds}}&end_at={{ds}}&symbols={currency}&base=GBP",
        http_conn_id="airflow-training-currency-http",
        gcs_path="currency/{{ ds }}-" + currency + ".json",
        gcs_bucket="airflow-training-data",
        dag=dag,
    )
