from datetime import datetime, timedelta
from airflow.decorators import dag, task

default_args = {
    'owner': 'fdodino',
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

numbers = [1, 2, 3, 7, 4, 8, 10, 11]


@dag(
    dag_id='01_numbers',
    description='DAG for numbers operation',
    start_date=datetime(2022, 3, 1, 2),
    schedule_interval='@daily'
)
def numbers_dag():

    @task
    def numbers_task():
        evens = [number * 2 for number in numbers if number % 2 == 0]
        print(evens)

    numbers_task()


numbers_dag()
