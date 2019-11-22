from datetime import timedelta, datetime

import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator



default_args = {
    "owner": "hudson",
    "email": "hudson.santos@cg.nl"
}

def print_current_dt():
    print(datetime.today())



tasks_list = []

with airflow.DAG(
    dag_id="test_dag_3",
    start_date=datetime(2019, 11, 17),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
) as dag:

    task_pd = PythonOperator(
        dag=dag, task_id="printcurrentdat", python_callable=print_current_dt
    )

    waiters = ['sleep 1','sleep 5','sleep 10']

    end_t = DummyOperator(task_id="end_task", dag=dag, )

    for index, wait in enumerate(waiters):
        tasks_list.append(
            BashOperator(task_id='wait_{ind}'.format(ind=index), bash_command=wait, dag=dag)
        )

        if index != 0:
            task_pd >> tasks_list[index] >> end_t
        else:
            task_pd >> tasks_list[0] >> end_t









