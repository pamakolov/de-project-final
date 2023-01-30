from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.decorators import dag
from datetime import datetime, timedelta
import vertica_python
from getpass import getpass
import pandas as pd
import boto3
import psycopg2
from airflow.models import Variable
from airflow.operators.python import get_current_context
from psycopg2.extensions import AsIs

psycopg2.autocommit = True

# создаем таблицу currency в PG
def ddl_stg_currency_pg():
    
    # creds for PG
    conn = psycopg2.connect(
                                host="localhost",
                                port=5432,
                                dbname="de",
                                user="jovyan",
                                password="jovyan"
                            )
    cur = conn.cursor()
    
    with open('/project/sql/pg_stg_create_schema.sql') as f:
        cur.execute(f.read())
    with open('/project/sql/pg_stg_ddl_currency.sql') as f:
        cur.execute(f.read())

    conn.commit()
    cur.close()
    conn.close()


# создаем таблицу transaction в PG
def ddl_stg_transaction_pg():
    
    # creds for PG
    conn = psycopg2.connect(
                                host="localhost",
                                port=5432,
                                dbname="de",
                                user="jovyan",
                                password="jovyan"
                            )
    cur = conn.cursor()
    
    with open('/project/sql/pg_stg_create_schema.sql') as f:
        cur.execute(f.read())
    with open('/project/sql/pg_stg_ddl_transaction.sql') as f:
        cur.execute(f.read())

    conn.commit()
    cur.close()
    conn.close()

def _print_execution_date(ds):
  print(f"The execution date of this flow is {ds}")


with DAG('1st_data_import_from_source', 
        tags=['DE_final_project_10'], 
        start_date=datetime(2022, 10, 1), 
        catchup=False, #True (прогон каждый день с 2022-10-01 до 2022-12-01)
        schedule_interval="@daily",
        max_active_runs=2,
        concurrency=10,
        #end_date=datetime(2022, 12, 1)
        ) as dag:

    start = DummyOperator(task_id="start")

    etl_from_kafka_to_pg = BashOperator(
        task_id='etl_from_kafka_to_pg',
        bash_command='python3 /project/py/etl_from_kafka_to_pg.py')

    ddl_stg_currency_pg = PythonOperator(
        task_id='ddl_stg_currency_pg',
        python_callable=ddl_stg_currency_pg)

    ddl_stg_transaction_pg = PythonOperator(
        task_id='ddl_stg_transaction_pg',
        python_callable=ddl_stg_transaction_pg)

    
    _print_execution_date = PythonOperator(
        task_id='_print_execution_date',
        python_callable=_print_execution_date)

    end = DummyOperator(task_id="end")


start >> _print_execution_date >> [ddl_stg_currency_pg, ddl_stg_transaction_pg]
[ddl_stg_currency_pg, ddl_stg_transaction_pg] >> etl_from_kafka_to_pg >> end