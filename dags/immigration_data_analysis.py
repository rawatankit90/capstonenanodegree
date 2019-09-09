#!/usr/bin/python
# -*- coding: utf-8 -*-
from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.operators import CreateRedshiftOperator
from airflow.operators import CreateTableOperator
from airflow.operators import S3ToRedshiftOperator
from airflow.operators import DataQualityOperator

##from airflow.operators import StageToRedshiftOperator

from airflow.models import Variable
from airflow.models import Connection
from helpers import SqlQueries
import logging
import pandas as pd
import boto3
import json
from airflow import settings
import psycopg2

default_args = {
    'owner': 'ankit rawat',
    'depends_on_past': False,
    'start_date': datetime(2019, 9, 9),
    'email': ['rawatankit90@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': '@daily',
    }


##def print_hello(**kwargs):
##    value = kwargs['task_instance'].xcom_pull(task_ids='Stage_events',key='DWH_ENDPOINT')
##    logging.info(value)
##    return value

def load_sas_data(**kwargs):
    session = settings.Session()
    connection_detail = \
        session.query(Connection).filter(Connection.conn_id
            == 'pipeline_redshift').first()
    logging.info(connection_detail.host)
    DWH_DB_USER = connection_detail.login
    DWH_DB_PASSWORD = connection_detail.password
    DWH_ENDPOINT = connection_detail.host
    DWH_PORT = connection_detail.port
    DWH_DB = connection_detail.schema
    conn_string = 'postgresql://{}:{}@{}:{}/{}'.format(DWH_DB_USER,
            DWH_DB_PASSWORD, DWH_ENDPOINT, DWH_PORT, DWH_DB)
    reader_sas = \
        pd.read_sas('/mnt/c/airflow/dags/i94_apr16_sub.sas7bdat',
                    encoding='ISO-8859-1', chunksize=5000,
                    iterator=True)
    immigration_df = reader_sas.read()
    conn = psycopg2.connect(conn_string)

    country_df = pd.read_sql(SqlQueries.country_data_select, conn)
    immigration_df = \
        immigration_df[immigration_df.i94cit.isin(country_df.code)]

    immigration_df = \
        immigration_df[immigration_df.i94res.isin(country_df.code)]

    address_df = pd.read_sql(SqlQueries.address_data_select, conn)
    immigration_df = \
        immigration_df[immigration_df.i94addr.isin(address_df.code)]

    port_df = pd.read_sql(SqlQueries.port_data_select, conn)
    port_df = port_df[~port_df.port_name.str.contains('No PORT Cod')]
    immigration_df = \
        immigration_df[immigration_df.i94port.isin(port_df.code)]

    logging.info(immigration_df.head())
    logging.info(conn_string)
    immigration_df.to_sql('immigration', con=conn_string,
                          if_exists='append', index=False)


def clean_data(**kwargs):
    session = settings.Session()
    connection_detail = \
        session.query(Connection).filter(Connection.conn_id
            == 'pipeline_redshift').first()
    logging.info(connection_detail.host)
    DWH_DB_USER = connection_detail.login
    DWH_DB_PASSWORD = connection_detail.password
    DWH_ENDPOINT = connection_detail.host
    DWH_PORT = connection_detail.port
    DWH_DB = connection_detail.schema
    conn_string = 'postgresql://{}:{}@{}:{}/{}'.format(DWH_DB_USER,
            DWH_DB_PASSWORD, DWH_ENDPOINT, DWH_PORT, DWH_DB)
    conn = psycopg2.connect(conn_string)
    country_df = pd.read_sql(SqlQueries.country_data_select, conn)
    logging.info(country_df.head())


def load_csv_data(**kwargs):
    session = settings.Session()
    connection_detail = \
        session.query(Connection).filter(Connection.conn_id
            == 'pipeline_redshift').first()
    logging.info(connection_detail.host)
    DWH_DB_USER = connection_detail.login
    DWH_DB_PASSWORD = connection_detail.password
    DWH_ENDPOINT = connection_detail.host
    DWH_PORT = connection_detail.port
    DWH_DB = connection_detail.schema

    conn_string = 'postgresql://{}:{}@{}:{}/{}'.format(DWH_DB_USER,
            DWH_DB_PASSWORD, DWH_ENDPOINT, DWH_PORT, DWH_DB)

    country_df = pd.read_csv('/mnt/c/airflow/dags/cntyl.csv',
                             delimiter='=', header=None, names=['code',
                             'country_name'])
    country_df = country_df.replace(regex=[r'\''], value='')
    country_df.to_sql('country', con=conn_string, if_exists='append',
                      index=False)
    logging.info('country table completed')

    visa_df = pd.read_csv('/mnt/c/airflow/dags/I94VISA.csv',
                          delimiter='=', header=None, names=['visa_code'
                          , 'visa_name'])
    visa_df = visa_df.replace(regex=[r'\''], value='')
    visa_df.to_sql('visa_type', con=conn_string, if_exists='append',
                   index=False)
    logging.info('visa_type table completed')

    address_df = pd.read_csv('/mnt/c/airflow/dags/I94ADDR.csv',
                             delimiter='=', header=None, names=['code',
                             'state_name'])
    address_df = address_df.replace(regex=[r'\''], value='')
    address_df.to_sql('address', con=conn_string, if_exists='append',
                      index=False)
    logging.info('address table completed')

    port_df = pd.read_csv('/mnt/c/airflow/dags/I94PORT.csv',
                          delimiter='=', header=None, names=['code',
                          'port_name'])
    port_df = port_df.replace(regex=[r'\t|\''], value='')
    port_df.to_sql('port', con=conn_string, if_exists='append',
                   index=False)
    logging.info('port table completed')


def analyze_query(**kwargs):
    session = settings.Session()
    connection_detail = \
        session.query(Connection).filter(Connection.conn_id
            == 'pipeline_redshift').first()
    logging.info(connection_detail.host)
    DWH_DB_USER = connection_detail.login
    DWH_DB_PASSWORD = connection_detail.password
    DWH_ENDPOINT = connection_detail.host
    DWH_PORT = connection_detail.port
    DWH_DB = connection_detail.schema

    conn_string = 'postgresql://{}:{}@{}:{}/{}'.format(DWH_DB_USER,
            DWH_DB_PASSWORD, DWH_ENDPOINT, DWH_PORT, DWH_DB)
    conn = psycopg2.connect(conn_string)
    data = pd.read_sql(SqlQueries.analyze_data_select, conn)
    logging.info(data.head())


# dag = DAG('hello_world', description='Simple tutorial DAG',
#           schedule_interval='0 12 * * *',
#           start_date=datetime(2017, 3, 20), catchup=False)

dag = DAG(
    'immigration_data_analysis_f',
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    catchup=False,
    max_active_runs=1,
    schedule_interval='0 * * * *',
    start_date=datetime(2019, 9, 9),
    )

dummy_operator = DummyOperator(task_id='started_pipeline', retries=3,
                               dag=dag)

create_redshift = CreateRedshiftOperator(task_id='create_cluster',
        dag=dag, aws_credentials_id='aws_credentials')

create_table = CreateTableOperator(task_id='create_table', dag=dag,
                                   aws_credentials_id='aws_credentials'
                                   ,
                                   create_tables=SqlQueries.create_table_queries,
                                   drop_tables=SqlQueries.drop_table_queries)

load_csv_files = PythonOperator(task_id='load_csv_files',
                                python_callable=load_csv_data, dag=dag,
                                provide_context=True)
load_sas_file = PythonOperator(task_id='clean_and_load_i94',
                               python_callable=load_sas_data, dag=dag,
                               provide_context=True)

##
##clean_data_check = PythonOperator(task_id='clean_data_query', python_callable = clean_data, dag=dag, provide_context=True)

load_s3_to_redshift = S3ToRedshiftOperator(
    task_id='s3_to_redshift',
    dag=dag,
    schema='public',
    table='us_city_demography',
    redshift_conn_id='pipeline_redshift',
    aws_conn_id='aws_credentials',
    copy_options=tuple(['csv', 'Delimiter', '\';\'', 'IGNOREHEADER 1'
                       ]),
    s3_bucket='udacitycapstonedata',
    s3_key='us-cities-demographics.csv',
    )

run_quality_checks = \
    DataQualityOperator(task_id='Run_data_quality_checks', dag=dag,
                        tables=['us_city_demography', 'immigration'],
                        redshift_conn_id='pipeline_redshift')

## analyze_data = PythonOperator(task_id='analyze_data_from_redshift', python_callable = analyze_query, dag=dag, provide_context=True)
## hello_operator = PythonOperator(task_id='hello_task', python_callable=print_hello, dag=dag, provide_context=True)

dummy_operator >> create_redshift
create_redshift >> create_table
create_table >> load_s3_to_redshift
create_table >> load_csv_files
load_csv_files >> load_sas_file
load_sas_file >> run_quality_checks
load_s3_to_redshift >> run_quality_checks

## dummy_operator >> load_s3_to_redshift
## dummy_operator >> load_sas_file
## dummy_operator >> load_csv_files
# dummy_operator >> load_s3_to_redshift
# load_csv_files >> analyze_data

##dummy_operator >> clean_data_check
