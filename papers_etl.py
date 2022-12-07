from sys import exec_prefix
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
#from python.papers_staging import papers_etl, progressbar
from datetime import datetime
from airflow.utils.dates import days_ago

with DAG(dag_id='paper_staging_etl',
         default_args={'owner': 'airflow'},
         schedule_interval='@daily',
         start_date=days_ago(1)
    ) as dag:

    api_to_psql_job = BashOperator(
        task_id = 'api_to_psql_job',
        bash_command = 'python /opt/airflow/dags/python/arxiv_test.py'
    )

    execute_sql_function_insert_to_table = PostgresOperator(
        task_id='execut_sql_refresh_mv_runtimeiqx',
        postgres_conn_id = 'localPSQL',
        sql='''
        call insert_arxiv();
        '''
    )

    api_to_psql_job >> execute_sql_function_insert_to_table