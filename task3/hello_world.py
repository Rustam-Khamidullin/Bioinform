from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime

dag = DAG(
    'hello_world',
    start_date=datetime(2025, 1, 1),
    schedule="@once",
)

task = BashOperator(
    task_id='print_hello',
    bash_command='./script.sh',
    dag=dag,
)

