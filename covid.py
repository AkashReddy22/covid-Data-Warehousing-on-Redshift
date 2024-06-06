#interact with AWS resources
from io import StringIO
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

#function to query athena and load the query results to S3
Dict = {}
def download_and_load_query_results(client: boto3.client, query_response:Dict, table_name)-> pd.DataFrame:
    while True:
        try:
            client.get_query_results(
                QueryExecutionId=query_response['QueryExecutionId']
            )
            break     #this extracts the query execution id from the response
        except Exception as e:
            if "not yet finished" in str(e):
                time.sleep(0.001)
            else:
                raise e
    temp_file_location: str = f"athena_query_results_{table_name}.csv" #this will save it to local directory with the name athena_query_results.csv
    s3_client = boto3.client('s3', region_name="us-east-1", aws_access_key_id=KEY, aws_secret_access_key=SECRET)
    s3_client.download_file(S3_Bucket,
                            f"{S3_output_dir}/{query_response['QueryExecutionId']}.csv",
                            temp_file_location)
    return pd.read_csv(temp_file_location)

#connect to athena and run the query
response_enigma = athena.start_query_execution(
    QueryString = 'SELECT * FROM enigma_jhu',
    QueryExecutionContext = {
        'Database': 'covid-db'
    },
    ResultConfiguration = {
        'OutputLocation': S3_Staging,  #this is the location where the query results will be stored
        'EncryptionConfiguration': {'EncryptionOption': 'SSE_S3'}
    }
)

df_enigma_jhu = download_and_load_query_results(athena, response_enigma, 'enigma_jhu')

#repeating the above process for the other tables
response_country_code_countrycode = athena.start_query_execution(
    QueryString='SELECT * FROM country_code_countrycode',
    QueryExecutionContext={
        'Database': 'covid-db'
    },
    ResultConfiguration={
        'OutputLocation': S3_Staging,
        'EncryptionConfiguration': {'EncryptionOption': 'SSE_S3'}
    }
)
#load the query results to a dataframe
df_country_code_countrycode = download_and_load_query_results(athena, response_country_code_countrycode,'country_code')

response_country_population_countypopulation = athena.start_query_execution(
    QueryString='SELECT * FROM country_population_countypopulation',
    QueryExecutionContext={
        'Database': 'covid-db'
    },
    ResultConfiguration={
        'OutputLocation': S3_Staging,
        'EncryptionConfiguration': {'EncryptionOption': 'SSE_S3'}
    }
)

df_country_population_countypopulation = download_and_load_query_results(athena, response_country_population_countypopulation,'county_population')

response_hospital_beds_rearc_usa_hospital_beds = athena.start_query_execution(
    QueryString='SELECT * FROM hospital_beds_rearc_usa_hospital_beds',
    QueryExecutionContext={
        'Database': 'covid-db'
    },
    ResultConfiguration={
        'OutputLocation': S3_Staging,
        'EncryptionConfiguration': {'EncryptionOption': 'SSE_S3'}
    }
)

df_hospital_beds_rearc_usa_hospital_beds = download_and_load_query_results(athena, response_hospital_beds_rearc_usa_hospital_beds,'hospital_beds')

response_nytimes_us_country_us_county = athena.start_query_execution(
    QueryString='SELECT * FROM nytimes_us_country_us_county',
    QueryExecutionContext={
        'Database': 'covid-db'
    },
    ResultConfiguration={
        'OutputLocation': S3_Staging,
        'EncryptionConfiguration': {'EncryptionOption': 'SSE_S3'}
    }
)

df_nytimes_us_country_us_county = download_and_load_query_results(athena, response_nytimes_us_country_us_county,'nytimes_us_country_us_county')

response_state_abv_state_abv = athena.start_query_execution(
    QueryString='SELECT * FROM state_abv_state_abv',
    QueryExecutionContext={
        'Database': 'covid-db'
    },
    ResultConfiguration={
        'OutputLocation': S3_Staging,
        'EncryptionConfiguration': {'EncryptionOption': 'SSE_S3'}
    }
)

df_state_abv_state_abv = download_and_load_query_results(athena, response_state_abv_state_abv,'state_abv')


new_header = df_state_abv_state_abv.iloc[0] #grab the first row from the data as it has the column names 
static_state_abv = df_state_abv_state_abv[1:] #take the data excluding the header row
static_state_abv.columns = new_header #set the header row as the df header

response_testing_daily_states_daily = athena.start_query_execution(
    QueryString='SELECT * FROM testing_daily_states_daily',
    QueryExecutionContext={
        'Database': 'covid-db'
    },
    ResultConfiguration={
        'OutputLocation': S3_Staging,
        'EncryptionConfiguration': {'EncryptionOption': 'SSE_S3'}
    }
)

df_testing_daily_states_daily = download_and_load_query_results(athena, response_testing_daily_states_daily,'testing_states_daily')

response_testing_us_daily_us_daily = athena.start_query_execution(
    QueryString='SELECT * FROM testing_us_daily_us_daily',
    QueryExecutionContext={
        'Database': 'covid-db'
    },
    ResultConfiguration={
        'OutputLocation': S3_Staging,
        'EncryptionConfiguration': {'EncryptionOption': 'SSE_S3'}
    }
)

df_testing_us_daily_us_daily = download_and_load_query_results(athena, response_testing_us_daily_us_daily,'testing_us_daily')

response_total_testing_us_total_latest = athena.start_query_execution(
    QueryString='SELECT * FROM total_testing_us_total_latest',
    QueryExecutionContext={
        'Database': 'covid-db'
    },
    ResultConfiguration={
        'OutputLocation': S3_Staging,
        'EncryptionConfiguration': {'EncryptionOption': 'SSE_S3'}
    }
)

df_total_testing_us_total_latest = download_and_load_query_results(athena, response_total_testing_us_total_latest,'total_testing_us_total_latest')

response_total_testing_us_total_latest = athena.start_query_execution(
    QueryString='SELECT * FROM us_states',
    QueryExecutionContext={
        'Database': 'covid-db'
    },
    ResultConfiguration={
        'OutputLocation': S3_Staging,
        'EncryptionConfiguration': {'EncryptionOption': 'SSE_S3'}
    }
)

df_us_states = download_and_load_query_results(athena, response_total_testing_us_total_latest,'us_states')

#build the fact and dimension tables 
#fact table
fact_covid_1 = df_enigma_jhu[['fips','province_state','country_region','confirmed','deaths','recovered','active']]
fact_covid_2 = df_testing_daily_states_daily[['fips','date','positive','negative','pending','hospitalized','death','total']]

fact_final = pd.merge(fact_covid_1, fact_covid_2, on='fips', how='inner') #merge the two fact tables on fips and date

#dimension tables
dim_region = df_enigma_jhu[['fips','province_state','country_region','latitude','longitude']]
dim_region_2 = df_nytimes_us_country_us_county[['fips','county','state']]
dim_region_final = pd.merge(dim_region, dim_region_2, on='fips', how='inner')

#hospital dimension table 
dim_hospital = df_hospital_beds_rearc_usa_hospital_beds[['fips','state_name','latitude','longtitude','hq_address','hospital_name','hospital_type','hq_city','hq_state']]

#dimension table 2
dim_date = df_testing_daily_states_daily[['fips','date']]



#changing the integer date to datetime from the dim_date table
dim_date['date'] = pd.to_datetime(dim_date['date'], format='%Y%m%d')

dim_date['year'] = dim_date['date'].dt.year
dim_date['month'] = dim_date['date'].dt.month
dim_date['day'] = dim_date['date'].dt.day
dim_date['dayofweek'] = dim_date['date'].dt.dayofweek

#now we preserve the above transformed fact tables and dimension tables to S3
csv_buffer = StringIO()
fact_final.to_csv(csv_buffer)
s3.Object(S3_Bucket, 'output/fact_final.csv').put(Body=csv_buffer.getvalue())

#similarly for the dimension tables
csv_buffer = StringIO()
dim_region_final.to_csv(csv_buffer)
s3.Object(S3_Bucket, 'output/dim_region_final.csv').put(Body=csv_buffer.getvalue())

#similarly for the dimension tables
csv_buffer = StringIO()
dim_date.to_csv(csv_buffer)
s3.Object(S3_Bucket, 'output/dim_date.csv').put(Body=csv_buffer.getvalue())

#similarly for the hospital dimension tables
csv_buffer = StringIO()
dim_hospital.to_csv(csv_buffer)
s3.Object(S3_Bucket, 'output/dim_hospital.csv').put(Body=csv_buffer.getvalue())



 
            

    
        
