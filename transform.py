#interact with AWS resources
import boto3
import json
import os
import sys
import time
import logging
import requests
import psycopg2
import pandas as pd 
import configparser
import datetime
import numpy as np

#setup config
config = configparser.ConfigParser()
config.read_file(open('cluster.config'))

print(config)
print(config.get('AWS', 'KEY'))

#bring the configuration from the config file and store in variables 
KEY                    = config.get('AWS', 'KEY')
SECRET                 = config.get('AWS', 'SECRET')
DWH_CLUSTER_IDENTIFIER = config.get('DWH', 'DWH_CLUSTER_IDENTIFIER')
DWH_DB                 = config.get('DWH', 'DWH_DB')
DWH_DB_USER            = config.get('DWH', 'DWH_DB_USER')
DWH_DB_PASSWORD        = config.get('DWH', 'DWH_DB_PASSWORD')
DWH_PORT               = config.get('DWH', 'DWH_PORT')
DWH_IAM_ROLE_NAME       = config.get('DWH', 'DWH_IAM_ROLE_NAME')
DWH_CLUSTER_TYPE       = config.get('DWH', 'DWH_CLUSTER_TYPE')
DWH_NUM_NODES          = config.get('DWH', 'DWH_NUM_NODES')
DWH_NODE_TYPE          = config.get('DWH', 'DWH_NODE_TYPE')

SCHEMA_NAME = 'covid-db'
S3_Staging = 's3://akash-covid-project-1/output'
S3_Bucket = 'akash-covid-project-1'
S3_Region = 'us-east-1'
S3_output_dir = 'output'

#create a df out of the above variables
config_df = pd.DataFrame({'Param':
                   ['DWH_CLUSTER_TYPE', 'DWH_NUM_NODES', 'DWH_NODE_TYPE', 'DWH_CLUSTER_IDENTIFIER', 'DWH_DB', 'DWH_DB_USER', 'DWH_DB_PASSWORD', 'DWH_PORT', 'DWH_IAM_ROLE_NAME'],
                   'Value':
                   [DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME]
                  })

print(config_df)

#connect to the EC2 instance
ec2 = boto3.resource('ec2',region_name="us-east-1", aws_access_key_id=KEY, aws_secret_access_key=SECRET)

#connect to the S3 bucket
s3 = boto3.resource('s3', region_name="us-east-1", aws_access_key_id=KEY, aws_secret_access_key=SECRET)

#connect to the Redshift cluster
redshift = boto3.client('redshift', region_name="us-east-1", aws_access_key_id=KEY, aws_secret_access_key=SECRET)

#connect to the IAM role
iam = boto3.client('iam', region_name="us-east-1", aws_access_key_id=KEY, aws_secret_access_key=SECRET)

#connect to Athena 
athena = boto3.client('athena', region_name="us-east-1", aws_access_key_id=KEY, aws_secret_access_key=SECRET)

s3.download_file(S3_Bucket, 'output/fact_final.csv', 'fact_final.csv')

