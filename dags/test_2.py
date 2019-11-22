from datetime import timedelta, datetime

import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator



args = {
    'owner': 'Airflow',
    'start_date': datetime(2019, 10, 5, 7)
}

dag = DAG(
    dag_id='test_dag_2',
    default_args=args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=60),
)


def print_current_dt():
    print(datetime.today())


task_pd = PythonOperator(
    dag=dag, task_id="printcurrentdat", python_callable=print_current_dt
)

wait1 = BashOperator(
    task_id='wait_1',
    bash_command='sleep 1',
    dag=dag,
)

wait5 = BashOperator(
    task_id='wait_5',
    bash_command='sleep 1',
    dag=dag,
)

wait10 = BashOperator(
    task_id='wait_10',
    bash_command='sleep 1',
    dag=dag,
)

end_t = DummyOperator(task_id="end_task", dag=dag,)



task_pd >> [wait1, wait5, wait10] >> end_t
