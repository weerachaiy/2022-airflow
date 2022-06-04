import time
from datetime import timedelta
import pendulum
from datetime import datetime
from airflow.models.dag import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
#from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
#from airflow.hooks.base_hook import BaseHook
import pandas as pd
#from sqlalchemy import create_engine

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.python import PythonOperator



#DAG
with DAG(
    'Loanstats_etl_dag',
    # [START default_args]
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={'retries': 2},
    # [END default_args]
    description='ETL DAG of LoanStats Data',
    schedule_interval=timedelta(minutes=10),
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=['ETL'],
) as dag:
  #transformation tasks
  def extraction_to_transformation():
    raw_pd = pd.read_csv('LoanStats_web.csv')
    selected_pd = raw_pd[['int_rate','loan_status','term']]
    regex_list = [r'%','months']
    selected_pd['int_rate'].replace(regex=regex_list,value='',inplace=True)
    selected_pd['term'].replace(regex=regex_list,value='',inplace=True)
    selected_pd.fillna('N/A',inplace=True)
    selected_pd.to_csv('test.json')
  
  transform_task = PythonOperator(
        task_id='extraction_to_transformation',
        python_callable=extraction_to_transformation,
    )
  
  transform_task
