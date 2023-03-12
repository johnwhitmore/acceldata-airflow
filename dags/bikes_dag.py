import datetime
from datetime import timedelta
from datetime import datetime

from airflow.operators.python import PythonOperator
from cta_demo_composer.get_cta_data import *
from cta_demo_composer.get_weather_data import *
from cta_demo_composer.flatten_weather_json import *
from cta_demo_composer.load_cta_datamart import *
import pendulum
import logging
from time import sleep

from torch_sdk.models.pipeline import CreatePipeline, PipelineMetadata
from torch_sdk.models.job import CreateJob, JobMetadata, Node
from torch_sdk.torch_client import TorchClient
from torch_sdk.events.generic_event import GenericEvent
import torch_sdk.constants as const
from torch_airflow_sdk.operators.execute_policy_operator import ExecutePolicyOperator
from torch_sdk.constants import FailureStrategy, DATA_QUALITY
from torch_airflow_sdk.dag import DAG
from torch_airflow_sdk.operators.torch_initialiser_operator import TorchInitializer
from torch_airflow_sdk.decorators.job import job
from torch_airflow_sdk.operators.span_operator import SpanOperator
from torch_airflow_sdk.operators.job_operator import JobOperator
from torch_airflow_sdk.initialiser import torch_credentials

# Pipeline variables
pipeline_uid = 'cta_weather_demo_composer'
pipeline_name = 'CTA Bike & Weather (Composer)'
pipeline_desc = 'Production CTA Monitoring'
snowflake_datasource = 'Snowflake_SE_DS'
job_settings = JobMetadata('Cozzi', 'Product', 'Airflow')

#ds_stg_station_information = Dataset(snowflake_datasource, 'CHICAGO_BIKES.STAGING.STG_STATION_INFORMATION')
#ds_stg_station_status = Dataset(snowflake_datasource, 'CHICAGO_BIKES.STAGING.STG_STATION_STATUS')
#ds_stg_bike_status = Dataset(snowflake_datasource, 'CHICAGO_BIKES.STAGING.STG_BIKE_STATUS')
#ds_stg_weather = Dataset(snowflake_datasource, 'CHICAGO_BIKES.STAGING.STG_WEATHER_DATA')
#ds_stg_weather_flattened = Dataset(snowflake_datasource, 'CHICAGO_BIKES.STAGING.STG_WEATHER_DATA_FLATTENED')
#ds_stg_processed_runs = Dataset(snowflake_datasource, 'CHICAGO_BIKES.STAGING.STG_PROCESSED_RUNS')
#ds_prod_bike_status = Dataset(snowflake_datasource, 'CHICAGO_BIKES.PROD.BIKE_STATUS')
#ds_prod_station_status = Dataset(snowflake_datasource, 'CHICAGO_BIKES.PROD.STATION_STATUS')

ds_stg_station_information = Node(asset_uid=snowflake_datasource+'.CHICAGO_BIKES.STAGING.STG_STATION_INFORMATION')
ds_stg_station_status = Node(asset_uid=snowflake_datasource+'.CHICAGO_BIKES.STAGING.STG_STATION_STATUS')
ds_stg_bike_status = Node(asset_uid=snowflake_datasource+'.CHICAGO_BIKES.STAGING.STG_BIKE_STATUS')
ds_stg_weather = Node(asset_uid=snowflake_datasource+'.CHICAGO_BIKES.STAGING.STG_WEATHER_DATA')
ds_stg_weather_flattened = Node(asset_uid=snowflake_datasource+'.CHICAGO_BIKES.STAGING.STG_WEATHER_DATA_FLATTENED')
ds_stg_processed_runs = Node(asset_uid=snowflake_datasource+'.CHICAGO_BIKES.STAGING.STG_PROCESSED_RUNS')
ds_prod_bike_status = Node(asset_uid=snowflake_datasource+'.CHICAGO_BIKES.PROD.BIKE_STATUS')
ds_prod_station_status = Node(asset_uid=snowflake_datasource+'.CHICAGO_BIKES.PROD.STATION_STATUS')

@job(job_uid='create_run_id',metadata=job_settings)
def create_run_id(**context):
    now = datetime.now()
    run_id = int(now.strftime("%Y%m%d%H%M%S"))

    task_instance = context['ti']
    task_instance.xcom_push(key="run_id", value=run_id)
    logging.info('Run ID created: '+str(run_id))
    return run_id

@job(job_uid='acceldata_dq_datamart',
   inputs=[ds_prod_station_status, ds_prod_bike_status],
   metadata=job_settings)
def execute_dq_rule_cta(**context):

    station_rule_id = 137
    bike_rule_id = 138

    logging.info("Creating Torch Client...")
    torch_client = TorchClient(**torch_credentials)

    execute_dq_span = context['span_context_parent']

    dq_span = execute_dq_span.create_child_span(uid="acceldata_dq_datamart_stations")
    dq_rule = torch_client.execute_rule(const.DATA_QUALITY, station_rule_id, sync=True, incremental=True, failure_strategy=FailureStrategy.FailOnError)
    dq_span.end()

    dq_span = execute_dq_span.create_child_span(uid="acceldata_dq_datamart_bikes")
    dq_rule = torch_client.execute_rule(const.DATA_QUALITY, bike_rule_id, sync=True, incremental=True, failure_strategy=FailureStrategy.FailOnError)
    dq_span.end()

@job(job_uid='acceldata_dq_stg_cta',
    inputs=[ds_stg_station_information,ds_stg_station_status,ds_stg_bike_status],
    metadata=job_settings)
def execute_dq_rule_cta_stg(**context):

    bike_rule_id=141
    status_rule_id=139
    info_rule_id=140

    logging.info("Creating Torch Client...")
    torch_client = TorchClient(**torch_credentials)

    execute_dq_span = context['span_context_parent']

    dq_span = execute_dq_span.create_child_span(uid="acceldata_dq_stg_bikes")
    dq_rule = torch_client.execute_rule(const.DATA_QUALITY, bike_rule_id, sync=True, incremental=True, failure_strategy=FailureStrategy.FailOnError)
    dq_span.end()

    dq_span = execute_dq_span.create_child_span(uid="acceldata_dq_stg_station_status")
    dq_rule = torch_client.execute_rule(const.DATA_QUALITY, status_rule_id, sync=True, incremental=True, failure_strategy=FailureStrategy.FailOnError)
    dq_span.end()

    dq_span = execute_dq_span.create_child_span(uid="acceldata_dq_stg_station_info")
    dq_rule = torch_client.execute_rule(const.DATA_QUALITY, info_rule_id, sync=True, incremental=True, failure_strategy=FailureStrategy.FailOnError)
    dq_span.end()

@job(job_uid='acceldata_recon_cta',
    inputs=[ds_stg_station_information,ds_stg_station_status],
    metadata=job_settings)
def execute_recon_rule_cta(**context):

    recon_rule_id=132

    logging.info("Creating Torch Client...")
    torch_client = TorchClient(**torch_credentials)

    recon_rule = torch_client.execute_rule(const.RECONCILIATION, recon_rule_id, sync=True, incremental=True, failure_strategy=FailureStrategy.FailOnError)


@job(job_uid='acceldata_recon_weather',
    inputs=[ds_stg_weather,ds_stg_weather_flattened],
    metadata=job_settings)
def execute_recon_rule_weather(**context):
    recon_rule_id = 133

    logging.info("Creating Torch Client...")
    torch_client = TorchClient(**torch_credentials)

    recon_rule = torch_client.execute_rule(const.RECONCILIATION, recon_rule_id, sync=True, incremental=True, failure_strategy=FailureStrategy.FailOnError)

@job(job_uid='acceldata_recon_datamart_stations',
    inputs=[ds_stg_station_status,ds_prod_station_status],
    metadata=job_settings)
def execute_recon_rule_datamart_stations(**context):
    recon_rule_id = 135

    logging.info("Creating Torch Client...")
    torch_client = TorchClient(**torch_credentials)

    recon_rule = torch_client.execute_rule(const.RECONCILIATION, recon_rule_id, sync=True, incremental=True, failure_strategy=FailureStrategy.FailOnError)

@job(job_uid='acceldata_recon_datamart_bikes',
    inputs=[ds_stg_bike_status,ds_prod_bike_status],
    metadata=job_settings)
def execute_recon_rule_datamart_bikes(**context):
    recon_rule_id = 134

    logging.info("Creating Torch Client...")
    torch_client = TorchClient(**torch_credentials)

    recon_rule = torch_client.execute_rule(const.RECONCILIATION, recon_rule_id, sync=True, incremental=True, failure_strategy=FailureStrategy.FailOnError)

# Declare Default arguments for the DAG
args = {"owner": "Airflow",'retries': 0, "start_date": pendulum.datetime(2022, 5, 5, tz="America/Denver")}
dag = DAG(dag_id="cta_demo_composer", catchup=False, default_args=args, schedule_interval=timedelta(hours=3) , max_active_runs=1)
#dag = DAG(dag_id="cta_demo_composer", catchup=False, default_args=args, schedule_interval=None , max_active_runs=1)

#Initialize Torch
pipeline_initializer = TorchInitializer(
    task_id='pipeline_initializer',
    pipeline_uid=pipeline_uid,
    pipeline_name=pipeline_name,
    dag=dag,
    create_pipeline = True,
    span_name = 'cta_demo_composer',
    meta=PipelineMetadata(owner='chris@acceldata.io', team='Product', codeLocation='GCP')
)

#Create run ID
create_run_id = PythonOperator(task_id='create_run_id', python_callable=create_run_id, dag=dag)

#Extract and Load Weather Data
load_weather_data = JobOperator(
        task_id="load_weather_data",
        job_uid='load_weather_data',
        outputs=[ds_stg_weather],
        metadata=job_settings,
        dag=dag,
        operator=PythonOperator(task_id='load_weather_data_python_op', python_callable=load_weather_data)
    )

#Extract and Load CTA Data
load_cta_data = JobOperator(
        task_id="load_cta_data",
        job_uid='load_cta_data',
        outputs=[ds_stg_station_information,ds_stg_station_status,ds_stg_bike_status],
        metadata=job_settings,
        dag=dag,
        operator=PythonOperator(task_id='load_cta_data_python_op', python_callable=load_cta_data)
    )

#Data Quality Checks CTA
#execute_dq_rule_cta_stg = PythonOperator(task_id='dq_check_cta_stg', python_callable=execute_dq_rule_cta_stg, dag=dag)

#Reconcile API data
#recon_check_cta = PythonOperator(task_id='recon_check_cta', python_callable=execute_recon_rule_cta, dag=dag)

#Flatten Weather Data
flatten_weather_data = JobOperator(
        task_id="flatten_weather_data",
        job_uid='flatten_weather_data',
        inputs=[ds_stg_weather],
        outputs=[ds_stg_weather_flattened,ds_stg_processed_runs],
        metadata=job_settings,
        dag=dag,
        operator=PythonOperator(task_id='flatten_weather_data_python_op', python_callable=flatten_weather_data)
    )

#Data Quality Checks Weather
#dq_check_weather = JobOperator(
#        task_id="dq_check_weather",
#        job_uid='dq_check_weather',
#        inputs=[ds_stg_weather_flattened],
#        metadata=job_settings,
#        dag=dag,
#            operator=ExecutePolicyOperator(task_id='acceldata_dq_check_weather_op',rule_type=const.DATA_QUALITY,rule_id=142,sync=True, failure_strategy=FailureStrategy.FailOnError)
#    )

#Reconcile Weather Data
#recon_check_weather = PythonOperator(task_id='recon_check_weather', python_callable=execute_recon_rule_weather, dag=dag)

#Load production tables
load_cta_datamart = JobOperator(
        task_id="load_cta_datamart",
        job_uid='load_cta_datamart',
        inputs=[ds_stg_station_information, ds_stg_station_status, ds_stg_bike_status, ds_stg_weather_flattened],
        outputs=[ds_prod_bike_status,ds_prod_station_status,ds_stg_processed_runs],
        metadata=job_settings,
        dag=dag,
        operator=PythonOperator(task_id='load_cta_datamart_python_op', python_callable=load_cta_datamart)
    )

#Reconcile Prod
recon_check_datamart_stations = PythonOperator(task_id='recon_check_datamart_stations', python_callable=execute_recon_rule_datamart_stations, dag=dag)
recon_check_datamart_bikes = PythonOperator(task_id='recon_check_datamart_bikes', python_callable=execute_recon_rule_datamart_bikes, dag=dag)
dq_check_cta = PythonOperator(task_id='dq_check_cta', python_callable=execute_dq_rule_cta, dag=dag)

#Pipeline dependencies
#Parallel
#pipeline_initializer >> create_run_id  >> [load_weather_data, load_cta_data]
#load_cta_data >> recon_check_cta >> execute_dq_rule_cta_stg
#load_weather_data >> flatten_weather_data >> recon_check_weather >> dq_check_weather
#execute_dq_rule_cta_stg >> load_cta_datamart
#dq_check_weather >> load_cta_datamart
#load_cta_datamart  >> [recon_check_datamart_stations, recon_check_datamart_bikes] >> dq_check_cta

#Pipeline dependencies
#Non-Parallel
pipeline_initializer >> create_run_id  >> load_weather_data >> load_cta_data >> flatten_weather_data >> load_cta_datamart >> recon_check_datamart_stations>> recon_check_datamart_bikes >> dq_check_cta