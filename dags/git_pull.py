from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.notebook_plugin import NotebookToGitOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from include.helpers.astro import pull_file, upload_to_s3
from datetime import datetime, timedelta

default_args = {
    'retries': 3,
    'email_on_retry': False,
    'email_on_failure': False,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 1)
}

with DAG('git_to_s3', default_args=default_args,
    description='pulls from git and pushes to S3',
    schedule_interval='*/10 * * * *',
    catchup=False) as dag:

    start = DummyOperator(
        task_id='start'
    )

    pull = PythonOperator(
        task_id='pull_file',
        python_callable=pull_file,
        pool='training_pool',
        op_kwargs= {'my_file': 'info2.txt'}
    ) 

    sense_file = FileSensor(
        task_id='sense_file',
        fs_conn_id='fs_default',
        filepath='info2.txt',
        poke_interval=15
    )

    upload_to_s3 = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3,
        pool='training_pool',
        op_kwargs={'file_name': '/tmp/info2.txt'}

    )

    start >> pull >> sense_file >> upload_to_s3