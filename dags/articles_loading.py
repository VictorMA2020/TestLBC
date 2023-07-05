import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from google.cloud import bigquery
from google.oauth2 import service_account


def load_to_bq(**kwargs):
    key_path = os.path.join(os.getcwd(), "GCP_service_account_key.json")
    credentials = service_account.Credentials.from_service_account_file(
        key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    table_id = kwargs["table_id"]
    schema = [
        bigquery.SchemaField("date", "DATE", mode="REQUIRED", description="Date of data extraction"),
        bigquery.SchemaField("author", "STRING", mode="NULLABLE", description="Author of the article"),
        bigquery.SchemaField("headline", "STRING", mode="NULLABLE", description="Title of the article"),
        bigquery.SchemaField("type", "STRING", mode="NULLABLE", description="Type of article (Review, Analysis, Interview etc.)"),
        bigquery.SchemaField("category", "STRING", mode="NULLABLE", description="Article category (Film, Culture, Sport etc.)"),
        bigquery.SchemaField("rating", "STRING", mode="NULLABLE", description="Rating of the subject reviewed (for type = Review only)"),
        bigquery.SchemaField("genre", "STRING", mode="NULLABLE", description="Genre of film (for category = film only)"),
        bigquery.SchemaField("body", "STRING", mode="NULLABLE", description="Full article text"),
        bigquery.SchemaField("url", "STRING", mode="NULLABLE", description="Link to article on theguardian website"),
    ]
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        field_delimiter=";",
        skip_leading_rows=1,
        schema=schema,
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE"
    )
    with open(kwargs["file"], "rb") as source_file:
        job = client.load_table_from_file(source_file, table_id, job_config=job_config)
    job.result()
    os.remove(kwargs["file"])

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
loading_to_bq = PythonOperator(
    task_id="loading_to_bq",
    dag=dag,
    python_callable=load_to_bq,
    op_kwargs={
        "table_id": "test-lbc-391414.dwh_tmp.articles_raw_{{ ds_nodash }}",
        "file": os.path.join(os.getcwd(), "articles.csv")
    }
)
wait_for_crawling >> loading_to_bq

creating_table = BigQueryOperator(
    task_id="creating_table",
    dag=dag,
    sql="sql/create_articles_table.sql",
    bigquery_conn_id="gcp",
    use_legacy_sql=False
)
start_task >> creating_table

cleaning_data = BigQueryOperator(
    task_id="cleaning_data",
    dag=dag,
    sql="sql/articles_cleaning.sql",
    destination_dataset_table="dwh_tmp.articles_cleaned_{{ ds_nodash }}",
    bigquery_conn_id="gcp",
    use_legacy_sql=False,
    allow_large_results=True,
    write_disposition="WRITE_TRUNCATE",
    create_disposition="CREATE_IF_NEEDED"
)
loading_to_bq >> cleaning_data

# Apparently, deleting rows from table is a paying feature, but this is what I would've done to avoid duplicating data if the pipeline is replayed

# delete_partition = BigQueryOperator(
#     task_id="delete_partition",
#     dag=dag,
#     sql="DELETE FROM dwh.articles WHERE date = {{ ds }}",
#     use_legacy_sql=False,
#     bigquery_conn_id="gcp"
# )
# creating_table >> delete_partition

# Also apparently, inserting rows into a table is another paying feature, so this task will fail, but I'll leave it here still
# We can use the pre-insertion table dwh_tmp.articles_cleaned_{{ ds_nodash }} for our queries instead
load_into_final_table = BigQueryOperator(
    task_id="load_into_final_table",
    dag=dag,
    sql="sql/load_articles_table.sql",
    bigquery_conn_id="gcp",
    use_legacy_sql=False
)
[cleaning_data, creating_table] >> load_into_final_table >> end_task