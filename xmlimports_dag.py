from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import mmenacer_script

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': '2023-04-03',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'my_python_script',
    default_args=default_args,
    schedule_interval='*/5 * * * *',
    catchup=False
)

script_path = './xmlimports_python_script.py'

run_my_script = PythonOperator(
    task_id='run_my_script',
    python_callable=mmenacer_script.main,
    op_kwargs={'script_path': script_path},
    dag=dag,
)

run_my_script
