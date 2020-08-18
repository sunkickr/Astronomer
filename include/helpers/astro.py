import urllib.request
import os
from airflow.models import Variable 
from notebook_plugin.hooks.git_hook import GitHook
from airflow.hooks import S3Hook

import shutil

def download_dataset():
    dataset = Variable.get("avocado_dag_dataset_settings", deserialize_json=True)
    output = dataset['filepath'] + dataset['filename']
    urllib.request.urlretrieve(dataset['url'], filename=output)
    return os.path.getsize(output)

def check_dataset(**kwargs):
    ti = kwargs['ti']
    filesize = ti.xcom_pull(key=None, task_ids='downloading_data')
    if filesize <= 0:
        raise ValueError('Dataset is empty')

def create_file(number, **context):
    name = 'david' 
    

    hook = GitHook(conn_id='git2')

    end_file = "{0}_{1}.txt".format(name, number)
    f = open("/tmp/" + end_file, "w+")
    f.write("{0} sending file{1}".format(name, number))
    f.close()

    shutil.copyfile(src='/tmp/{0}'.format(end_file), 
                        dst='{0}/{1}'.format(hook.path, end_file))
    
    hook.push()

def pull_file(my_file, **context):
    hook = GitHook(conn_id='git2')

    hook.pull()

    shutil.copyfile(src='{0}/{1}'.format(hook.path, my_file), 
                        dst='/tmp/{0}'.format(my_file))

def upload_to_s3(file_name, **context):
    s3_hook=S3Hook(aws_conn_id='astro-s3-workshop') 

    s3_hook.load_file(file_name, 'workshop/{0}'.format(file_name), bucket_name='astro-workshop-bucket', replace=True)


