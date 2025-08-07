import datetime

from airflow.sdk import dag, task
from airflow.providers.standard.operators.empty import EmptyOperator

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

import logging

logging.info("This is a log message")


@task()
def s3_list():
    minio_hook = S3Hook(aws_conn_id="s3minio_conn")
    bucket_name = 'test'

    logging.info('created s3hook')
    print('created s3hook')

    files = minio_hook.list_keys(bucket_name=bucket_name)

    logging.info(files)

    print(files)


@dag(start_date=datetime.datetime(2025, 1, 1), schedule="@daily")
def s3_dag():
    print(1)
    
    logging.info("This is a log message 2")
    EmptyOperator(task_id="task")

    print(2)

    s3_list()



s3_dag()
