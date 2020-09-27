from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from datetime import datetime, timedelta

# Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

api_key = os.getenv('news_api_key')

# Using a DAG context manager, you don't have to specify the dag property of each task
with DAG('ddod_dag',
         start_date=datetime(2020, 1, 1),
         max_active_runs=3,
         schedule_interval='0 0 * * *',  # https://airflow.apache.org/docs/stable/scheduler.html#dag-runs
         default_args=default_args,
         catchup=False # enable if you don't want historical dag runs to run
         ) as dag:

    get_news_from_api_task = SimpleHttpOperator(
      task_id='get_news_from_api_task',
      method='GET',
      endpoint='/v2/everything',
      http_conn_id='news_api',
      data={
        "apiKey": api_key,
        "q": "disease",
        "pageSize": 100
      },
      headers={
        "Content-Type": "application/json",
        "X-Api-Key": api_key,

      },
      response_check=lambda response: response.json()['articles'],
      dag=dag,
      log_response=True
    )

    get_news_from_api_task
