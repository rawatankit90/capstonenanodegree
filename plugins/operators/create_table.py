#!/usr/bin/python
# -*- coding: utf-8 -*-
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.models import Variable
import psycopg2
from airflow import settings
import logging
from airflow.models import Connection
from airflow import settings


class CreateTableOperator(BaseOperator):

    ui_color = '#A98866'

    @apply_defaults
    def __init__(
        self,
        aws_credentials_id='',
        create_tables='',
        drop_tables='',
        *args,
        **kwargs
        ):

        super(CreateTableOperator, self).__init__(*args, **kwargs)
        self.create_tables = create_tables
        self.drop_tables = drop_tables
        self.aws_credentials_id = aws_credentials_id

    def execute(self, context):
        try:
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

            conn = psycopg2.connect(host=DWH_ENDPOINT, dbname=DWH_DB,
                                    user=DWH_DB_USER,
                                    password=DWH_DB_PASSWORD,
                                    port=DWH_PORT)

##            HOST_NAME = "dwhcluster.cluadnihggnj.us-west-2.redshift.amazonaws.com"
##            conn = psycopg2.connect( host = HOST_NAME,
##                                     dbname = "dwh", user = "dwhuser", password = "Passw0rd", port = "5439")

            task_instance = context['task_instance']

            # task_instance.xcom_push(key="connectionObject",value=conn)
            # Variable.set("connection", conn)

            cur = conn.cursor()
            self.log.info('Connection Established')
            self.log.info(conn)

            for query in self.drop_tables:
                cur.execute(query)
                conn.commit()
                self.log.info('table dropped')

            for query in self.create_tables:
                cur.execute(query)
                conn.commit()
                self.log.info('table created')
        except (Exception, psycopg2.Error), error:

            self.log.info('Error while connecting to PostgreSQL')
            self.log.info(error)
