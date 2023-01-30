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
from airflow.sensors.external_task_sensor import ExternalTaskSensor



# creds for vertica
conn_info = {
                'host': '51.250.75.20', 
                'port': 5433,
                'user': 'UNKNOWNPAVELYANDEXRU',    
                'password': 'kWMT59eg4FpmnLV',    
                'database': 'dwh',
                'autocommit': True
            }



# создаем таблицу currency + проекции в слое STG
def ddl_stg_currency():
    
    vertica_conn = vertica_python.connect(**conn_info)
    cur = vertica_conn.cursor()
    with open('/project/sql/vertica_stg_currency.sql') as f:
        cur.execute(f.read())
    
    vertica_conn.commit()
    vertica_conn.close()



# создаем таблицу transaction + проекции в слое STG
def ddl_stg_transaction():

    vertica_conn = vertica_python.connect(**conn_info)
    cur = vertica_conn.cursor()
    with open('/project/sql/vertica_stg_transaction.sql') as f:
        cur.execute(f.read())
    
    vertica_conn.commit()
    vertica_conn.close()


# чтение currency из Postgresql и заливка в Vertica
def currency_to_vertica(*args, **kwargs):
    import io

    # creds for PG
    conn = psycopg2.connect(
                                host="localhost",
                                port=5432,
                                dbname="de",
                                user="jovyan",
                                password="jovyan"
                            )
    cur = conn.cursor()

    cur.execute("select distinct object_id, object_type, sent_dttm, date_update, currency_code, currency_code_with,currency_with_div from stg.input_kafka_currency")
    
    # transfer to pandas df
    df = pd.DataFrame(
                        cur.fetchall(), 
                        columns=['object_id', 'object_type', 'sent_dttm', 'date_update', 'currency_code', 'currency_code_with','currency_with_div']
                     )
    conn.close()

    
    # loading to vertica from bytes stream pandas df
    with vertica_python.connect(**conn_info) as vertica_conn:

        cur = vertica_conn.cursor()
        fields = ', '.join(df.columns)
        stream = io.BytesIO()
        df.to_csv(stream, sep=';', index=False, header=False, encoding='utf-8')
        stream.seek(0)
        cur.execute(""" truncate table UNKNOWNPAVELYANDEXRU__STAGING.currencies_raw;""")
        copy_string = "COPY {}({}) FROM STDIN DELIMITER ';'".format('UNKNOWNPAVELYANDEXRU__STAGING.currencies_raw', fields)
        cur.copy(copy_string, stream.getvalue())
        cur.execute(
                        """
                        select distinct object_id, object_type, sent_dttm, date_update, currency_code, currency_code_with,currency_with_div
                        from UNKNOWNPAVELYANDEXRU__STAGING.currencies_raw
                        where hash(object_id,object_type,sent_dttm,date_update,currency_code,currency_code_with,currency_with_div) 
                        not in (
                                 select hash(object_id,object_type,sent_dttm,date_update,currency_code,currency_code_with,currency_with_div) 
                                 from UNKNOWNPAVELYANDEXRU__STAGING.currencies
                               )
                        ;"""

        )
        cur.execute("select analyze_statistics ('UNKNOWNPAVELYANDEXRU__STAGING.currencies')")


# чтение transaction из Postgresql и заливка в Vertica
def transaction_to_vertica(*args, **kwargs):
    import io

    # creds for PG
    conn = psycopg2.connect(
                                host="localhost",
                                port=5432,
                                dbname="de",
                                user="jovyan",
                                password="jovyan"
                            )
    cur = conn.cursor()

    cur.execute("select distinct object_id,object_type,sent_dttm,operation_id,account_number_from,account_number_to,currency_code,country,status,transaction_type,amount,transaction_dt from stg.input_kafka_TRANSACTION")
    
    # transfer to pandas df
    df = pd.DataFrame(
                        cur.fetchall(), 
                        columns=['object_id','object_type','sent_dttm','operation_id','account_number_from','account_number_to','currency_code','country',
                                 'status','transaction_type','amount','transaction_dt']
                     )
    conn.close()

    # loading to vertica from bytes stream pandas df
    with vertica_python.connect(**conn_info) as vertica_conn:

        cur = vertica_conn.cursor()
        fields = ', '.join(df.columns)
        stream = io.BytesIO()
        df.to_csv(stream, sep=';', index=False, header=False, encoding='utf-8')
        stream.seek(0)
        cur.execute(""" truncate table UNKNOWNPAVELYANDEXRU__STAGING.transactions_raw;""")
        copy_string = "COPY {}({}) FROM STDIN DELIMITER ';'".format('UNKNOWNPAVELYANDEXRU__STAGING.transactions_raw', fields)
        cur.copy(copy_string, stream.getvalue())
        
        cur.execute(
                        """
                        select distinct object_id,object_type,sent_dttm,operation_id,account_number_from,account_number_to,currency_code,
                        country,status,transaction_type,amount,transaction_dt
                        from UNKNOWNPAVELYANDEXRU__STAGING.transactions_raw
                        where hash(object_id,object_type,sent_dttm,operation_id,account_number_from,account_number_to,currency_code,country,status,transaction_type,amount,transaction_dt) 
                        not in (
                                 select hash(object_id,object_type,sent_dttm,operation_id,account_number_from,account_number_to,currency_code,country,status,transaction_type,amount,transaction_dt) 
                                 from UNKNOWNPAVELYANDEXRU__STAGING.transactions
                               )
                        ;"""

        )
        cur.execute("select analyze_statistics ('UNKNOWNPAVELYANDEXRU__STAGING.transactions')")


# создаем таблицу global_metrics в слое DWH
def ddl_dwh_global_metrics():
    
    vertica_conn = vertica_python.connect(**conn_info)
    cur = vertica_conn.cursor()
    cur.execute(
                    """
                    --- Создание таблицы витрины global_metrics
                    CREATE TABLE if not exists UNKNOWNPAVELYANDEXRU__DWH.global_metrics (
                        date_update date not null ,
                        currency_from varchar not null,
                        amount_total numeric(16,2)
                        ,cnt_transactions numeric(16,2)
                        ,avg_transactions_per_account int
                        ,cnt_accounts_make_transactions int
                    )
                    order by date_update
                    unsegmented all nodes
                    PARTITION BY date_trunc('month',date_update)
                    ;"""
                )
    vertica_conn.commit()
    vertica_conn.close()

def _print_execution_date(ds):
  print(f"The execution date of this flow is {ds}")

# создаем таблицу global_metrics в слое DWH
def loading_dwh_global_metrics(ds):

    EXEC_DATE = ds
    print('The execution date of this flow is ------->', ds)
    vertica_conn = vertica_python.connect(**conn_info)
    cur = vertica_conn.cursor()
    cur.execute(
                    f"""
                    delete from UNKNOWNPAVELYANDEXRU__DWH.global_metrics where date_update::date = '{ds}'::date - 1;
                    insert into UNKNOWNPAVELYANDEXRU__DWH.global_metrics

                    select --t.country, 
                    t.sent_dttm::date, t.currency_code, 
                    sum(t.amount*c.currency_with_div)::numeric(15,2) as amount_total 
                    ,sum(t.amount) cnt_transactions 
                    ,(sum(t.amount*c.currency_with_div) / count(distinct t.operation_id))::int as avg_transactions_per_account 
                    ,count(distinct case when t.currency_code=420 then t.account_number_from else null end) as cnt_accounts_make_transactions
                    from UNKNOWNPAVELYANDEXRU__STAGING.transactions t
                    left join UNKNOWNPAVELYANDEXRU__STAGING.currencies c on c.currency_code = t.currency_code and t.sent_dttm::Date=c.sent_dttm::date
                    where t.sent_dttm::Date = '{ds}'::date - 1 and
                    t.account_number_from>0 
                    --and t.country='russia' 
                    and t.status='done' 
                    and t.transaction_type like '%comin%'
                    --and t.object_id = 'fb564efc-cb2d-4b32-9751-55eff5730f3c'
                    --and c.currency_code_with=420
                    group by 1,2 --,3 
                    order by 1,2 --,3
                    ;"""
                )

    vertica_conn.commit()
    vertica_conn.close()

with DAG('2nd_data_loading_to_dwh', 
        tags=['DE_final_project_10'], 
        start_date=datetime(2022, 10, 1), 
        catchup=False,
        schedule_interval="@daily",
        max_active_runs=2,
        concurrency=10,
        #end_date=datetime(2022, 11, 30)
        ) as dag:

    start = DummyOperator(task_id="start")

    ExternalTaskSensor = ExternalTaskSensor(
        task_id='ExternalTaskSensor', 
        external_dag_id='1st_data_import_from_source', # название dag
        #external_task_id=’first_task’, # ждем полностью весь даг
        poke_interval = 30, # каждые 30сек проверям статус внешнего дага
        timeout=3*60*60, # ждем 3ч
        dag=dag)


    ddl_stg_currency = PythonOperator(
        task_id='ddl_stg_currency',
        python_callable=ddl_stg_currency)
    
    ddl_stg_transaction = PythonOperator(
        task_id='ddl_stg_transaction',
        python_callable=ddl_stg_transaction)
    
    currency_to_vertica = PythonOperator(
        task_id='currency_to_vertica',
        python_callable=currency_to_vertica)
    
    transaction_to_vertica = PythonOperator(
        task_id='transaction_to_vertica',
        python_callable=transaction_to_vertica)

    ddl_dwh_global_metrics = PythonOperator(
        task_id='ddl_dwh_global_metrics',
        python_callable=ddl_dwh_global_metrics)

    loading_dwh_global_metrics = PythonOperator(
        task_id='loading_dwh_global_metrics',
        python_callable=loading_dwh_global_metrics)

    _print_execution_date = PythonOperator(
        task_id='_print_execution_date',
        python_callable=_print_execution_date)
    
    end = DummyOperator(task_id="end")
    


start >> _print_execution_date >> ExternalTaskSensor >> [ ddl_stg_currency, ddl_stg_transaction]
[ddl_stg_currency] >> currency_to_vertica
[ddl_stg_transaction] >> transaction_to_vertica
[currency_to_vertica , transaction_to_vertica] >> ddl_dwh_global_metrics >> loading_dwh_global_metrics >> end
