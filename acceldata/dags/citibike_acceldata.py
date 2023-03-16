from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from io import BytesIO
from zipfile import ZipFile
import requests
import boto3
import pandas as pd
from acceldata_airflow_sdk.dag import DAG
from acceldata_airflow_sdk.operators.torch_initialiser_operator import TorchInitializer
from acceldata_sdk.models.pipeline import PipelineMetadata
from acceldata_airflow_sdk.decorators.job import job
from acceldata_sdk.models.job import JobMetadata, Node
import logging
from airflow.models import Variable


minio_access_key_id = Variable.get("minio_access_key_id")
minio_secret_access_key = Variable.get("minio_secret_access_key")
minio_endpoint_url = Variable.get("minio_endpoint_url")
minio_bucket = Variable.get("minio_bucket")
aws_access_key_id = Variable.get("aws_access_key_id")
aws_secret_access_key = Variable.get("aws_secret_access_key")
aws_bucket = Variable.get("aws_bucket")

src_urlbase = 'https://s3.amazonaws.com/tripdata/'
raw_path = 'citibike/raw/'
processed_path = 'citibike/processed/'
daily_path = 'citibike/daily/'

job_settings = JobMetadata(owner='Demo', team='demo_team', codeLocation='...')
#ds_http_src = Node(asset_uid=f"{src_urlbase}")
#ds_citibike_raw = Node(asset_uid=f"{bucket}/{raw_path}")

# data range to process
min_month = "2019-01"
max_month = "2019-03"
# Set up the lab S3 connection
lab_s3 = boto3.resource('s3',
                        aws_access_key_id=minio_access_key_id,
                        aws_secret_access_key=minio_secret_access_key,
                        endpoint_url=minio_endpoint_url
                        )
# Set up the AWS S3 connection
aws_s3 = boto3.resource('s3',
                        aws_access_key_id=aws_access_key_id,
                        aws_secret_access_key=aws_secret_access_key
                        )


pipeline_uid = "torch.citibike.pipeline"
pipeline_name = "Citibike Rides ETL"
default_args = {'start_date': datetime(2022, 5, 31)}
dag = DAG(
    dag_id='citibike_rides_etl',
    schedule_interval=None,
    default_args=default_args,
    start_date=datetime(2022, 6, 6)
)


def move_file(filename):
    print(filename)

    # read the zipfile contents using requests
    response = requests.get(f"{src_urlbase}{filename}")
    # Use StringIO to make it file-like so we can read the .csv
    # open infile as a file-like object to hold the entire zipfile read from the website
    infile = BytesIO()
    infile.write(response.content)

    # open outfile to hold the re-zipped file that will be written to s3
    outfile = BytesIO()
    # create a zipfile object that will write to outfile
    outzip = ZipFile(outfile, 'w')
    # print(f"fresh outzip file listing: {outzip.namelist()}")

    # create a zipfile object myzip reading from the infile
    with ZipFile(infile) as myzip:
        print(myzip.namelist())
        # grab the first file from the archive, which is the actual .csv.
        # The second is a MACOS resource fork and breaks pandas.
        csvfilename = myzip.namelist()[0]
        print(f"csvfilename: {csvfilename}")
        # read the csv file from the archive into csvdata
        with myzip.open(csvfilename) as myfile:
            csvdata = (myfile.read())

    # write the csv data to the output zipfile with the csv filename and close it to finish writing
    outzip.writestr(csvfilename, csvdata)
    print(f"outzip directory: ")
    outzip.printdir()
    outzip.close()

    # seek to the start of the outfile object so that it can be read and sent to s3.
    outfile.seek(0)

    # Set the S3 bucket and key where you want to write the file
    key = f"{raw_path}{filename}"

    # Write the file contents to S3 using the S3 client
    lab_s3.Object(minio_bucket, key).put(Body=outfile.read())


#    s3.put_object(Bucket=bucket, Key=key, Body=outfile.read())

@job(job_uid='download_rides_data',
    metadata=job_settings)
def download_data(**context):
    # Generate the months we want to process
    months = pd.period_range(min_month, max_month, freq='M')
    for month in months:
        # move the file using name generated from the month
        move_file(month.strftime('%Y%m-citibike-tripdata.csv.zip'))


@job(
    job_uid='read_rides_data',
    inputs=[Node(job_uid='download_rides_data')],
    metadata=job_settings
)
def read_data(**context):
    lab_s3_bucket = lab_s3.Bucket(minio_bucket)
    prefix_objs = lab_s3_bucket.objects.filter(Prefix=raw_path)
    #full_df = pd.DataFrame()
    for obj in prefix_objs:
        if obj.key.endswith('.zip'):
            print(f"reading {obj.key}")
            body = obj.get()['Body'].read()
            df = pd.read_csv(BytesIO(body), compression='zip')
            df['starttime'] = pd.to_datetime(df['starttime'])
            df['stoptime'] = pd.to_datetime(df['stoptime'])
            print(df)
            # insert unique first column
            df.insert(loc=0, column='row_id',
                      value=(df['bikeid'].astype(str) + '_' +
                             df['starttime'].apply(lambda x: str(int(datetime.timestamp(x))))))

            parquet_key = obj.key.replace(".csv.zip", ".parquet")
            parquet_key = parquet_key.replace(raw_path,processed_path)
            print(f"key: {obj.key}, parquet_key = {parquet_key}")
            out_buffer = BytesIO()
            df.to_parquet(out_buffer, index=False)
            out_buffer.seek(0)
            aws_s3.Object(aws_bucket, parquet_key).put(Body=out_buffer.read())
            # full_df = pd.concat([full_df, df])

    print("finished processing")
    # print(full_df)


@job(
    job_uid='aggregate_rides_data',
    inputs=[Node(job_uid='read_rides_data')],
    metadata=job_settings
)
def aggregate_rides_data(**context):
    current_year = datetime.now().year
    aws_s3_bucket = aws_s3.Bucket(aws_bucket)
    prefix_objs = aws_s3_bucket.objects.filter(Prefix=processed_path)
    for obj in prefix_objs:
        if obj.key.endswith('.parquet'):
            print(f"reading {obj.key}")
            daily_key = obj.key.replace(processed_path,daily_path)
            body = obj.get()['Body'].read()
            df = pd.read_parquet(BytesIO(body))
            print(df)
            # "tripduration","starttime","stoptime","start station id","start station name",
            # "start station latitude","start station longitude","end station id","end station name",
            # "end station latitude","end station longitude","bikeid","usertype","birth year","gender"
            # convert starttime column to datetime format
            #df['starttime'] = pd.to_datetime(df['starttime'])
            df['date'] = df['starttime'].dt.date
            df['age'] = current_year - df['birth year']

            daily_summary = df.groupby('date').agg(
                trip_count=('starttime', 'size'),
                duration_total=('tripduration', 'sum'),
                duration_avg=('tripduration', 'mean'),
                age_min=('age', 'min'),
                age_med=('age', 'median'),
                age_max=('age', 'max'),
                subscriber_pct=('usertype', lambda x: (x == 'Subscriber').sum() / len(x) * 100)
            )
            daily_summary['rowid'] = df.reset_index().index
            daily_summary.insert(0, 'rowid', daily_summary.pop('rowid'))

            print(daily_summary)
            out_buffer = BytesIO()
            daily_summary.to_parquet(out_buffer, index=False)
            out_buffer.seek(0)
            aws_s3.Object(aws_bucket, daily_key).put(Body=out_buffer.read())


@job(
    job_uid='create_run_id',
    metadata=job_settings
)
def create_run_id(**context):
    now = datetime.now()
    run_id = int(now.strftime("%Y%m%d%H%M%S"))

    task_instance = context['ti']
    task_instance.xcom_push(key="run_id", value=run_id)
    logging.info('Run ID created: ' + str(run_id))
    return run_id


torch_initializer_task = TorchInitializer(
    task_id='torch_pipeline_initializer',
    pipeline_uid=pipeline_uid,
    pipeline_name=pipeline_name,
    #connection_id="torch_connection_id",
    meta=PipelineMetadata(owner='Demo', team='demo_team', codeLocation='...'),
    dag=dag
)

#Create run ID
create_run_id = PythonOperator(
    task_id='create_run_id',
    python_callable=create_run_id,
    dag=dag
)


task_download_data = PythonOperator(
    task_id='download_src_data',
    python_callable=download_data,
    dag=dag
)

task_read_data = PythonOperator(
    task_id='read_rides_data',
    python_callable=read_data,
    dag=dag
)

task_aggregate_rides_data = PythonOperator(
    task_id='aggregate_rides_daily',
    python_callable=aggregate_rides_data,
    provide_context=True,
    dag=dag
)


torch_initializer_task >> create_run_id >> task_download_data >> task_read_data >> task_aggregate_rides_data
