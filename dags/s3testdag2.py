import datetime
import logging

from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


@task()
def s3_list():
    """
    Подключается к S3 и выводит список файлов в бакете.
    """
    # Создаем подключение к S3-совместимому хранилищу, используя conn_id
    hook = S3Hook(aws_conn_id='s3minio_conn')
    bucket_name = 'test'

    logging.info(f"Подключение к бакету '{bucket_name}'...")

    # Используем правильное имя переменной - 'hook'
    files = hook.list_keys(bucket_name=bucket_name)

    if files:
        logging.info("Найденные файлы:")
        for file in files:
            logging.info(f"- {file}")
    else:
        logging.info(f"Бакет '{bucket_name}' пуст или не найден.")

    return files


@dag(
    dag_id='s3_dag_final', # Рекомендуется дать уникальное имя
    start_date=datetime.datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=['minio', 's3'],
)
def s3_dag():
    """
    DAG для демонстрации работы с S3/MinIO.
    """
    s3_list()

# Создаем экземпляр DAG
s3_dag()
