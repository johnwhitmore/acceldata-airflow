# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
from io import BytesIO
from zipfile import ZipFile
import requests
import boto3
import pandas as pd

access_key_id = 'o2odjCI59uVYRhbm'
secret_access_key = 'L83dBpJZD4RxrQSezpON5VFnWfzUxaVH'
endpoint_url = 'http://192.168.1.201:9000'

min_month = "2019-01"
max_month = "2019-01"
# Set up the S3 client
s3 = boto3.resource('s3',
                  aws_access_key_id=access_key_id,
                  aws_secret_access_key=secret_access_key,
                  endpoint_url=endpoint_url)

urlbase = 'https://s3.amazonaws.com/tripdata/'
bucket = 'airflow-data'
raw_path = 'citibike/raw/'


def move_file(filename):
    print(filename)

    # read the zipfile contents using requests
    response = requests.get(f"{urlbase}{filename}")
    # Use StringIO to make it file-like so we can read the .csv
#    data = response.content
    infile = BytesIO()
    infile.write(response.content)
    outfile = BytesIO()
    outzip = ZipFile(outfile, 'w')
    print(f"fresh outzip file listing: {outzip.namelist()}")

    with ZipFile(infile) as myzip:
        print(myzip.namelist())
        csvfilename = myzip.namelist()[0]
        print(f"csvfilename: {csvfilename}")
        with myzip.open(csvfilename) as myfile:
            csvdata = (myfile.read())

    outzip.writestr(csvfilename, csvdata)
    print(f"outzip directory: ")
    outzip.printdir()
    outzip.close()
    outfile.seek(0)

    # Set the S3 bucket and key where you want to write the file
    key = f"{raw_path}{filename}"

    # Write the file contents to S3 using the S3 client
    s3.Object(bucket, key).put(Body=outfile.read())
#    s3.put_object(Bucket=bucket, Key=key, Body=outfile.read())

def download_data():
    # Generate filenames for the range of months we want to process
    months = pd.period_range(min_month, max_month, freq='M')
    for month in months:
        # print("skipped")
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

read_data()