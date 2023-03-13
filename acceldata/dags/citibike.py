from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from io import BytesIO
from zipfile import ZipFile
import requests
import boto3
import pandas as pd

access_key_id = 'o2odjCI59uVYRhbm'
secret_access_key = 'L83dBpJZD4RxrQSezpON5VFnWfzUxaVH'
endpoint_url = 'http://192.168.1.201:9000'

# data range to process
min_month = "2019-01"
max_month = "2019-01"
# Set up the S3 client
s3 = boto3.resource('s3',
                  aws_access_key_id=access_key_id,
                  aws_secret_access_key=secret_access_key,
                  endpoint_url=endpoint_url)

src_urlbase = 'https://s3.amazonaws.com/tripdata/'
bucket = 'airflow-data'
raw_path = 'citibike/raw/'

dag = DAG(
    dag_id="citibike_etl",
    catchup=False,
    default_args=args,
    schedule_interval=timedelta(hours=3) ,
    max_active_runs=1)


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
        # grab the first file from the archive, which is the actual .csv.  The second is a MACOS resource fork and breaks pandas.
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
    s3.Object(bucket, key).put(Body=outfile.read())
#    s3.put_object(Bucket=bucket, Key=key, Body=outfile.read())

def download_data():
    # Generate the months we want to process
    months = pd.period_range(min_month, max_month, freq='M')
    for month in months:
        # move the file using name generated from the month
        move_file(month.strftime('%Y%m-citibike-tripdata.csv.zip'))
def read_data():
    s3_bucket = s3.Bucket(bucket)
    prefix_objs = s3_bucket.objects.filter(Prefix=raw_path)
    # read the s3 bucket data into pandas
    df = pd.read_csv(
        f"s3://{bucket}/{raw_path}201901-citibike-tripdata.csv.zip",
        storage_options={
            "key": access_key_id,
            "secret": secret_access_key,
            "client_kwargs": {"endpoint_url": endpoint_url}
        }
    )
    print("finished reading")
    print(df)
    print("printed df")
    df.describe()
    print("described df")

task_download_data = PythonOperator(
    task_id='download_src_data',
    python_callable=download_data(),
    dag=dag
)

task_read_data = PythonOperator(
    task_id='read_rides_data',
    python_callable=read_data(),
    dag=dag
)

task_download_data >> task_read_data