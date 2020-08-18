from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.notebook_plugin import NotebookToGitOperator
from include.helpers.astro import create_file
from datetime import datetime, timedelta

default_args = {
    'retries': 3,
    'email_on_retry': False,
    'email_on_failure': False,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 1)
}

with DAG('git_dag', default_args=default_args,
    description='Simple push to git',
    schedule_interval='*/10 * * * *',
    catchup=False) as dag:

    start = DummyOperator(
        task_id='start'
    )

    for i in range(0,5):
        create = PythonOperator(
                task_id='create_file_{0}'.format(i),
                python_callable=create_file,
                pool='training_pool',
                op_kwargs= {'number': i}
        ) 

        start >> create

    

