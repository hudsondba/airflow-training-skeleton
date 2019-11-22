from datetime import timedelta, datetime
import pendulum

import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator


default_args = {"owner": "hudson", "email": "hudson.santos@cg.nl"}


def print_current_dt():
    print(datetime.today())


tasks_list = []

weekday_person_to_email = {
    0: "Bob",  # Monday
    1: "Joe",  # Tuesday
    2: "Alice",  # Wednesday
    3: "Joe",  # Thursday
    4: "Alice",  # Friday
    5: "Alice",  # Saturday
    6: "Alice",  # Sunday
}


def branch_fnc(**kwargs):
    return "task_for_" + weekday_person_to_email[pendulum.today().weekday()]


def pweedd():
    print(pendulum.today().weekday())


with airflow.DAG(
    dag_id="test_dag_4",
    start_date=datetime(2019, 11, 17),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
) as dag:

    final_task = DummyOperator(task_id="final_task", dag=dag)

    for day in range(0, 6):
        printd = PythonOperator(
            dag=dag, task_id="print_week_day", python_callable=pweedd
        )

        bop = BranchPythonOperator(
            task_id="branching",
            python_callable=branch_fnc,
            provide_context=True,
            dag=dag,
        )
        dumm_t = DummyOperator(
            task_id="task_for_" + weekday_person_to_email[day] + day, dag=dag
        )

        printd >> bop >> dumm_t >> final_task
