import os
import logging
import requests
import pandas as pd
import re
import numpy as np
from datetime import datetime, timedelta

from psycopg2.extras import execute_values

from airflow import DAG
from airflow import AirflowException

from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook



from airflow import DAG

dag_default_args = {
    'owner': 'Hnin',
    'start_date': datetime.now() - timedelta(days=1),
    'email': [],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=60),
    'depends_on_past': False,
    'wait_for_downstream': False,
}

dag = DAG(
    dag_id='campaign_monitor_tables_creation',
    default_args=dag_default_args,
    schedule_interval="@once",
    catchup=True,
    max_active_runs=1,
    concurrency=5
)

## Create Table Schema of Dim-host
create_campaign_email_open_table_task = PostgresOperator(
    task_id="create_email_opens_table",
    postgres_conn_id="postgres",
    sql="""
        CREATE TABLE IF NOT EXISTS campaign_monitor.campaign_email_opens (
        campaign_id VARCHAR(50) NOT NULL,
        email_address VARCHAR(100) NOT NULL,
        action_date timestamp  NOT NULL);
    """,
    dag=dag
)

create_campaign_email_click_table_task = PostgresOperator(
    task_id="create_email_clicks_table",
    postgres_conn_id="postgres",
    sql="""
        CREATE TABLE IF NOT EXISTS campaign_monitor.campaign_email_clicks (
        campaign_id VARCHAR(50) NOT NULL,
        email_address VARCHAR(100) NOT NULL,
        action_date timestamp  NOT NULL);
    """,
    dag=dag
)

create_campaign_email_bounce_table_task = PostgresOperator(
    task_id="create_email_bounces_table",
    postgres_conn_id="postgres",
    sql="""
        CREATE TABLE IF NOT EXISTS campaign_monitor.campaign_email_bounces (
        campaign_id VARCHAR(50) NOT NULL,
        email_address VARCHAR(100) NOT NULL,
        action_date timestamp  NOT NULL);
    """,
    dag=dag
)

create_campaign_email_spam_table_task = PostgresOperator(
    task_id="create_email_spams_table",
    postgres_conn_id="postgres",
    sql="""
        CREATE TABLE IF NOT EXISTS campaign_monitor.campaign_email_spams (
        campaign_id VARCHAR(50) NOT NULL,
        email_address VARCHAR(100) NOT NULL,
        action_date timestamp  NOT NULL);
    """,
    dag=dag
)

create_campaign_email_unsubscribe_table_task = PostgresOperator(
    task_id="create_email_unsubscribe_table",
    postgres_conn_id="postgres",
    sql="""
        CREATE TABLE IF NOT EXISTS campaign_monitor.campaign_email_unsubscribes (
        campaign_id VARCHAR(50) NOT NULL,
        email_address VARCHAR(100) NOT NULL,
        action_date timestamp  NOT NULL);
    """,
    dag=dag
)

create_logs_success_load_task = PostgresOperator(
    task_id="create_log_success_load_table",
    postgres_conn_id="postgres",
    sql="""
        CREATE TABLE IF NOT EXISTS campaign_monitor.log_success_load (
        load_date timestamp NOT NULL,
        method VARCHAR(20) NOT NULL,
        data_start_date TIMESTAMP(3)  NOT NULL,
        data_end_date TIMESTAMP(3)  NOT NULL,
        start_runtime TIMESTAMP(3) NOT NULL,
        end_runtime TIMESTAMP(3) NOT NULL,
        table_name VARCHAR(50) NOT NULL
        );
    """,
    dag=dag
)

create_campaign_email_open_table_task >> create_campaign_email_click_table_task 
create_campaign_email_bounce_table_task >> create_campaign_email_spam_table_task 
create_campaign_email_unsubscribe_table_task

create_campaign_email_click_table_task >> create_logs_success_load_task
create_campaign_email_unsubscribe_table_task >> create_logs_success_load_task
create_campaign_email_spam_table_task >> create_logs_success_load_task
