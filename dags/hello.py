from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'datamasterylab',
    'start-date': datetime(2026, 5, 10),
    'catchup': False
}

dag = DAG(
    'hello_world',
    default_args = default_args,
    schedule=timedelta(days=1)
)

task1 = BashOperator(
    task_id = 'hello_world',
    bash_command = 'echo "Hello World',
    dag = dag
)

task2 = BashOperator(
    task_id = 'hello_dml',
    bash_command = 'echo "Hello Data Mastery Lab',
    dag = dag
)

task1 >> task2