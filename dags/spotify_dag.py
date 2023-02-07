from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime,timedelta

from data_feed import spotify


default_args={'owner':'airflow',
    'depends_on_past':False,
    'start_date':datetime(2023,2,6),
    'email': 'shsithas501@gmail.com',
    'email_on_failure':True,
    'email_on_retries':False,
    'retries':2,
    'retry_delay':timedelta(minutes=5)
    }

dag = DAG(
    dag_id='spotify_dag1',
    default_args=default_args,
    schedule_interval='@daily'
)



run_tag=PythonOperator(
    task_id='data_feed',
    python_callable=spotify,
    dag=dag
)

run_tag 