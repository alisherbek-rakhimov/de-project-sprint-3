import time
import requests
import json
import pandas as pd

from datetime import datetime, timedelta
from airflow import DAG

from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup

# from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
# from airflow.providers.postgres.operators.postgres import PostgresOperator
# from airflow.hooks.http_hook import HttpHook

http_conn_id = HttpHook.get_connection('http_conn_id')
api_key = http_conn_id.extra_dejson.get('api_key')
base_url = http_conn_id.host

postgres_conn_id = 'postgresql_de'

nickname = 'alisherbekrakhimov'
cohort = '37'

headers = {
    'X-Nickname': nickname,
    'X-Cohort': cohort,
    'X-Project': 'True',
    'X-API-KEY': api_key,
    'Content-Type': 'application/x-www-form-urlencoded'
}


def generate_report(ti):
    print('Making request generate_report')

    response = requests.post(f'{base_url}/generate_report', headers=headers)
    response.raise_for_status()
    task_id = json.loads(response.content)['task_id']
    ti.xcom_push(key='task_id', value=task_id)
    print(f'Response is {response.content}')


def get_report(ti):
    print('Making request get_report')
    task_id = ti.xcom_pull(key='task_id')

    report_id = None

    for i in range(20):
        response = requests.get(f'{base_url}/get_report?task_id={task_id}', headers=headers)
        response.raise_for_status()
        print(f'Response is {response.content}')
        status = json.loads(response.content)['status']
        if status == 'SUCCESS':
            report_id = json.loads(response.content)['data']['report_id']
            break
        else:
            time.sleep(10)

    if not report_id:
        raise TimeoutError()

    ti.xcom_push(key='report_id', value=report_id)
    print(f'Report_id={report_id}')


def get_increment(date, ti):
    print("GET_INCREMENT_DATE: ", date)
    print('Making request get_increment')
    report_id = ti.xcom_pull(key='report_id')
    response = requests.get(
        f'{base_url}/get_increment?report_id={report_id}&date={str(date)}T00:00:00',
        headers=headers
    )
    response.raise_for_status()
    print(f'Response is {response.content}')

    increment_id = json.loads(response.content)['data']['increment_id']
    if not increment_id:
        raise ValueError(f'Increment is empty. Most probably due to error in API call.')

    ti.xcom_push(key='increment_id', value=increment_id)
    print(f'increment_id={increment_id}')


def branch_on_table_state():
    postgres_hook = PostgresHook(postgres_conn_id)

    def is_empty(table):
        count = postgres_hook.get_first(f"SELECT COUNT(*) FROM staging.{table}")[0]
        return count == 0

    empty_tables = [
        is_empty('user_order_log'),
        is_empty('user_activity_log'),
        is_empty('customer_research')
    ]

    if all(empty_tables):
        return 'init_staging_group.init_user_order_log'
    else:
        return 'increment_staging_group.get_increment'


def init_staging_table(filename, date, pg_table, pg_schema, ti):
    print('Making request init_staging_table')
    print("GET_INIT_DATE: ", date)
    report_id = ti.xcom_pull(key='report_id')
    s3_filename = f'https://storage.yandexcloud.net/s3-sprint3/cohort_{cohort}/{nickname}/project/{report_id}/{filename}'
    print(f'S3 filename={s3_filename}')

    print(s3_filename)
    local_filename = date.replace('-', '') + '_' + filename
    print(local_filename)
    response = requests.get(s3_filename)
    response.raise_for_status()
    open(f"{local_filename}", "wb").write(response.content)
    print(response.content)

    df = pd.read_csv(local_filename)
    if 'id' in df.columns:
        df = df.drop('id', axis=1)
    if 'uniq_id' in df.columns:
        df = df.drop_duplicates(subset=['uniq_id'])

    if 'status' not in df.columns:
        df['status'] = 'shipped'

    postgres_hook = PostgresHook(postgres_conn_id)
    engine = postgres_hook.get_sqlalchemy_engine()
    row_count = df.to_sql(pg_table, engine, schema=pg_schema, if_exists='append', index=False)
    print(f'{row_count} rows was inserted')


def upload_data_to_staging(filename, date, pg_table, pg_schema, ti):
    increment_id = ti.xcom_pull(key='increment_id')
    s3_filename = f'https://storage.yandexcloud.net/s3-sprint3/cohort_{cohort}/{nickname}/project/{increment_id}/{filename}'

    print(s3_filename)
    local_filename = date.replace('-', '') + '_' + filename
    print(local_filename)
    response = requests.get(s3_filename)
    response.raise_for_status()
    open(f"{local_filename}", "wb").write(response.content)
    print(response.content)

    df = pd.read_csv(local_filename)
    if 'id' in df.columns:
        df = df.drop('id', axis=1)
    if 'uniq_id' in df.columns:
        df = df.drop_duplicates(subset=['uniq_id'])

    if 'status' not in df.columns:
        df['status'] = 'shipped'

    postgres_hook = PostgresHook(postgres_conn_id)
    engine = postgres_hook.get_sqlalchemy_engine()
    row_count = df.to_sql(pg_table, engine, schema=pg_schema, if_exists='append', index=False)
    print(f'{row_count} rows was inserted')


args = {
    "owner": "student",
    'email': ['student@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

business_dt = '{{ ds }}'

# ––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––
with DAG(
        'sales_mart',
        default_args=args,
        description='Provide default dag for sprint3',
        catchup=True,
        max_active_runs=1,
        start_date=datetime.today() - timedelta(days=8),  # 8 days, because we need to init data + 7 days incrementally load data
        end_date=datetime.today() - timedelta(days=1),
) as dag:
    generate_report = PythonOperator(
        task_id='generate_report',
        python_callable=generate_report
    )

    get_report = PythonOperator(
        task_id='get_report',
        python_callable=get_report
    )

    # --------------------------------------------------------------------------------------------------------------------------
    with TaskGroup(group_id='init_staging_group') as init_staging_group:
        init_user_order_log = PythonOperator(
            task_id='init_user_order_log',
            python_callable=init_staging_table,
            op_kwargs={
                'date': business_dt,
                'filename': 'user_order_log.csv',
                'pg_table': 'user_order_log',
                'pg_schema': 'staging',
            }
        )

        init_user_activity_log = PythonOperator(
            task_id='init_user_activity_log',
            python_callable=init_staging_table,
            op_kwargs={
                'date': business_dt,
                'filename': 'user_activity_log.csv',
                'pg_table': 'user_activity_log',
                'pg_schema': 'staging',
            }
        )

        init_customer_research = PythonOperator(
            task_id='init_customer_research',
            python_callable=init_staging_table,
            op_kwargs={
                'date': business_dt,
                'filename': 'customer_research.csv',
                'pg_table': 'customer_research',
                'pg_schema': 'staging',
            }
        )

        init_user_order_log >> init_user_activity_log >> init_customer_research

    # --------------------------------------------------------------------------------------------------------------------------
    with TaskGroup(group_id='increment_staging_group') as increment_staging_group:
        get_increment = PythonOperator(
            task_id='get_increment',
            python_callable=get_increment,
            op_kwargs={'date': business_dt}
        )

        upload_user_order_inc = PythonOperator(
            task_id='upload_user_order_inc',
            python_callable=upload_data_to_staging,
            op_kwargs={
                'date': business_dt,
                'filename': 'user_order_log_inc.csv',
                'pg_table': 'user_order_log',
                'pg_schema': 'staging'
            }
        )

        upload_user_activity_inc = PythonOperator(
            task_id='upload_user_activity_inc',
            python_callable=upload_data_to_staging,
            op_kwargs={
                'date': business_dt,
                'filename': 'user_activity_log_inc.csv',
                'pg_table': 'user_activity_log',
                'pg_schema': 'staging'
            }
        )

        upload_customer_research_inc = PythonOperator(
            task_id='upload_customer_research_inc',
            python_callable=upload_data_to_staging,
            op_kwargs={
                'date': business_dt,
                'filename': 'customer_research_inc.csv',
                'pg_table': 'customer_research',
                'pg_schema': 'staging'
            }
        )

        get_increment >> upload_user_order_inc >> upload_user_activity_inc >> upload_customer_research_inc
    # --------------------------------------------------------------------------------------------------------------------------

    with TaskGroup(group_id='update_d_tables_group') as update_d_tables_group:
        update_d_calendar_table = PostgresOperator(
            trigger_rule='none_failed_min_one_success',  # 👈 key line
            task_id='update_d_calendar',
            postgres_conn_id=postgres_conn_id,
            sql="sql/mart.d_calendar.sql",
        )

        update_d_item_table = PostgresOperator(
            task_id='update_d_item',
            postgres_conn_id=postgres_conn_id,
            sql="sql/mart.d_item.sql"
        )

        update_d_customer_table = PostgresOperator(
            task_id='update_d_customer',
            postgres_conn_id=postgres_conn_id,
            sql="sql/mart.d_customer.sql"
        )

        update_d_city_table = PostgresOperator(
            task_id='update_d_city',
            postgres_conn_id=postgres_conn_id,
            sql="sql/mart.d_city.sql"
        )

        update_d_calendar_table >> update_d_item_table >> update_d_customer_table >> update_d_city_table

    update_f_sales = PostgresOperator(
        task_id='update_f_sales',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.f_sales.sql",
        parameters={"date": {business_dt}}
    )

    update_f_customer_retention = PostgresOperator(
        task_id='update_f_customer_retention',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.f_customer_retention.sql",
        parameters={"date": {business_dt}}
    )

    branch_task = BranchPythonOperator(
        task_id='branch_table_check',
        python_callable=branch_on_table_state
    )

    generate_report >> get_report >> branch_task

    branch_task >> init_staging_group >> update_d_tables_group
    branch_task >> increment_staging_group >> update_d_tables_group

    update_d_tables_group >> update_f_sales >> update_f_customer_retention
