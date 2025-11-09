from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

with DAG(
    'exchange_rates',
    default_args=default_args,
    schedule='0 8 * * *',
    catchup=False,
    is_paused_upon_creation=False
) as dag:
    run_script = BashOperator(
        task_id='execute_main',
        bash_command='python3 /opt/airflow/scripts/main.py'
    )