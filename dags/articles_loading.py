import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from google.cloud import bigquery
from google.oauth2 import service_account


def load_to_bq(**kwargs):
    key_path = os.path.join(os.getcwd(), 'GCP_service_account_key.json')
    credentials = service_account.Credentials.from_service_account_file(
        key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    table_id = kwargs["table_id"]
    schema = [
        bigquery.SchemaField('date', 'DATE', mode='REQUIRED', description='Date of data extraction'),
        bigquery.SchemaField('author', 'STRING', mode='REQUIRED', description='Author of the article'),
        bigquery.SchemaField('headline', 'STRING', mode='REQUIRED', description='Title of the article'),
        bigquery.SchemaField('body', 'STRING', mode='REQUIRED', description='Full article text')
    ]
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        field_delimiter=';',
        skip_leading_rows=1,
        schema=schema,
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE'
    )
    with open(kwargs["file"], "rb") as source_file:
        job = client.load_table_from_file(source_file, table_id, job_config=job_config)
    job.result()

default_args = {
    "start_date": datetime(2023, 7, 1),
    "retries": 2,
    "retry_delay": timedelta(seconds=10),
    "depends_on_past": False
}

dag = DAG(
    dag_id="articles_loading",
    default_args=default_args,
    description="Loading articles from local to BQ",
    schedule_interval="@once"
)

start_task = DummyOperator(task_id="start_task", dag=dag)
end_task = DummyOperator(task_id="end_task", dag=dag)

wait_for_crawling = ExternalTaskSensor(
    task_id="wait_for_crawling",
    dag=dag,
    external_dag_id="articles_crawling",
    external_task_id="end_task",
    allowed_states=["success"]
)
start_task >> wait_for_crawling

# Here I would normally use a local to GCS operator to keep a version of the file as cold storage that would allow for idempotent backfills
# But the free version of BigQuery that I use for the project does not include bucket storage so I instead use this cheap python operator to load directly to BQ
articles_loading = PythonOperator(
    task_id="articles_loading",
    dag=dag,
    python_callable=load_to_bq,
    op_kwargs={
        "table_id": "test-lbc-391414.dwh_tmp.articles_{{ ds_nodash }}",
        "file": os.path.join(os.getcwd(), 'articles.csv')
    }
)
wait_for_crawling >> articles_loading >> end_task