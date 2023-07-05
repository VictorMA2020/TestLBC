from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

default_args = {
    "start_date": datetime(2023, 7, 1),
    "retries": 2,
    "retry_delay": timedelta(seconds=10),
    "depends_on_past": False
}

dag = DAG(
    dag_id="articles_crawling",
    default_args=default_args,
    description="Crawling articles from the website theguardian.com and storing the results on a local tmp file",
    schedule_interval="@once" # Scheduled once for the purpose of the exercise, but it could be scheduled daily for example
)

start_task = DummyOperator(task_id="start_task", dag=dag)
end_task = DummyOperator(task_id="end_task", dag=dag)

articles_crawling = BashOperator(
    task_id="articles_crawling",
    dag=dag,
    bash_command="scrapy crawl articles",
    cwd="." # Define the current directory as working directory in the container, otherwise it uses a tmp directory outside the scrapy project
)
start_task >> articles_crawling >> end_task