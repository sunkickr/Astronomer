from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from airflow.hooks import S3Hook
from datetime import datetime, timedelta
import os


S3_CONN_ID='astro-s3-workshop'
BUCKET='astro-workshop-bucket'


"""
Simulates files getting dropped into an S3 bucket.
Also shows taking an arbitrary python function and turning it into an Airflow task.
"""

def upload_to_s3(file_name, **context):

    # Instanstiaute

    execution_date =context['ds']
    
    s3_hook=S3Hook(aws_conn_id=S3_CONN_ID) 


    # Create file
    
    sample_file = "file_{0}_{1}.txt".format(file_name, execution_date)
    example_file = open(sample_file, "w+")
    example_file.write("Putting some data in for task {0}".format(file_name))
    example_file.close()
    
    s3_hook.load_file(sample_file, 'workshop/{0}'.format(sample_file), bucket_name=BUCKET, replace=True)


# Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


# Using a DAG context manager, you don't have to specify the dag property of each task
with DAG('s3_upload',
         start_date=datetime(2019, 1, 1),
         max_active_runs=1,
         schedule_interval='@daily',  # https://airflow.apache.org/docs/stable/scheduler.html#dag-runs
         default_args=default_args,
         catchup=False # enable if you don't want historical dag runs to run
         ) as dag:

    t0 = DummyOperator(task_id='start')

    for i in range(0,5): # generates 10 tasks
        generate_files=PythonOperator(
            task_id='generate_file_{0}'.format(i), # task id is generated dynamically
            python_callable=upload_to_s3,
            provide_context=True,
            op_kwargs= {'file_name': i}
        )

        t0 >> generate_files