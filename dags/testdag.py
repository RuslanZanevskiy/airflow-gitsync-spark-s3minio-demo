import datetime

from airflow.sdk import dag, task
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.bash import BashOperator


@dag(start_date=datetime.datetime(2025, 1, 1), schedule="@daily")
def generate_dag():
    hello = BashOperator(task_id="hello", bash_command="echo hello")
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")


    @task(task_id='test_task')
    def test_task():
        print('hello')
        return 1

    hello >> start >> test_task() >> end

generate_dag()
