#!/usr/bin/python
# -*- coding: utf-8 -*-
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.contrib.hooks.redshift_hook import RedshiftHook
import boto3
import json
import time
from airflow.utils.decorators import apply_defaults
from airflow.models import Connection
from airflow import settings
import logging
from airflow.models import Connection
from airflow import settings


class CreateRedshiftOperator(BaseOperator):

    ui_color = '#458140'

    @apply_defaults
    def __init__(
        self,
        aws_credentials_id='',
        *args,
        **kwargs
        ):
        super(CreateRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id

    def execute(self, context):
        self.log.info('CreateRedshiftOperator implemented')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = boto3.client('redshift', region_name='us-west-2',
                                aws_access_key_id=credentials.access_key,
                                aws_secret_access_key=credentials.secret_key)
        iam = boto3.client('iam',
                           aws_access_key_id=credentials.access_key,
                           aws_secret_access_key=credentials.secret_key,
                           region_name='us-west-2')
        DWH_IAM_ROLE_NAME = 'dwhRole'
        DWH_CLUSTER_IDENTIFIER = 'dwhCluster'
        DWH_CLUSTER_TYPE = 'multi-node'
        DWH_NODE_TYPE = 'dc2.large'
        DWH_NUM_NODES = 4
        DWH_DB = 'dwh'
        DWH_DB_USER = 'dwhuser'
        DWH_DB_PASSWORD = 'Passw0rd'
        DWH_PORT = '5439'

        try:
            self.log.info('1.1 Creating a new IAM Role')
            dwhRole = iam.create_role(Path='/',
                    RoleName=DWH_IAM_ROLE_NAME,
                    Description="Allows Redshift clusters to call\
                                                   AWS services on\
                                                   your behalf."
                    ,
                    AssumeRolePolicyDocument=json.dumps({'Statement': [{'Action': 'sts:AssumeRole'
                    , 'Effect': 'Allow',
                    'Principal': {'Service': 'redshift.amazonaws.com'
                    }}], 'Version': '2012-10-17'}))
        except Exception, e:

            self.log.info(e)

        self.log.info('1.2 Attaching Policy')

        iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                               PolicyArn='arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess'
                               )['ResponseMetadata']['HTTPStatusCode']
        self.log.info('1.3 Get the IAM role ARN')
        roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn'
                ]
        self.log.info(roleArn)

        try:
            response = redshift.create_cluster(
                ClusterType=DWH_CLUSTER_TYPE,
                NodeType=DWH_NODE_TYPE,
                NumberOfNodes=DWH_NUM_NODES,
                DBName=DWH_DB,
                ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
                MasterUsername=DWH_DB_USER,
                MasterUserPassword=DWH_DB_PASSWORD,
                IamRoles=[roleArn],
                )
        except Exception, e:
            self.log.info(e)

        # Create new task and add a gap of 5 mins.
        # Use XComms to transfer the Postgres details from one task to another task

        time.sleep(350)
        myClusterProps = \
            redshift.describe_clusters(ClusterIdentifier='dwhCluster'
                )['Clusters'][0]
        i = 0
        while i == 0:
            status = myClusterProps.get('ClusterStatus')
            if status == 'available':
                i = 1
                self.log.info('Cluster is available')
            else:
                myClusterProps = \
                    redshift.describe_clusters(ClusterIdentifier='dwhCluster'
                        )['Clusters'][0]
                time.sleep(60)

                # prettyRedshiftProps(myClusterProps)

        DWH_ENDPOINT = myClusterProps.get('Endpoint')['Address']
        DWH_PORT = myClusterProps.get('Endpoint')['Port']
        DB_NAME = myClusterProps.get('DBName')

        self.log.info('Cluster Details')
        self.log.info(DWH_ENDPOINT)
        self.log.info(DWH_PORT)
        self.log.info(DB_NAME)
        task_instance = context['task_instance']
        self.log.info(task_instance)
        task_instance = context['task_instance']

        # DWH_ENDPOINT = 'redshift-cluster-1.cluadnihggnj.
        # us-west-2.redshift.amazonaws.com'
        # DWH_PORT = '5439'
        # DB_NAME = 'dev'

        task_instance.xcom_push(key='DWH_ENDPOINT', value=DWH_ENDPOINT)
        task_instance.xcom_push(key='DWH_PORT', value=DWH_PORT)
        task_instance.xcom_push(key='DB_NAME', value=DB_NAME)
        connection_setup = Connection(
            conn_id='pipeline_redshift',
            schema=DWH_DB,
            conn_type='postgres',
            host=DWH_ENDPOINT,
            login=DWH_DB_USER,
            password=DWH_DB_PASSWORD,
            port=DWH_PORT,
            )
        session = settings.Session()
        session.add(connection_setup)
        session.commit()
