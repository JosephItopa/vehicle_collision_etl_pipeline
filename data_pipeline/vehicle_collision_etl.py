import os
import time
import shutil
import requests
import psycopg2
import numpy as np
import pandas as pd
from datetime import datetime
from prefect import task, flow
#from dotenv import dotenv_values
from google.cloud import storage
from sqlalchemy import create_engine, text

# secrets = dotenv_values(".env")
# sudo docker-compose up airflow-init

directory_raw = "./raw"
directory_transformed = "./transformed"

@task(log_prints=True)
def temp_dir():    
    if not os.path.exists(directory_raw):
        os.makedirs(directory_raw)
        
    if not os.path.exists(directory_transformed):
        os.makedirs(directory_transformed)

@task(log_prints=True)
def extract_from_api():
    # vehicle collision data [2million]
    no_of_request=50000
    url = "https://data.cityofnewyork.us/resource/h9gi-nx95.json?$limit=".format(no_of_request)
    response = requests.get(url)
    data_list = pd.read_json(response.text, typ='series').to_list()
    df = pd.DataFrame(data_list)
        #
    df = df[['collision_id', 'crash_date', 'crash_time', 'on_street_name', 'number_of_persons_injured',\
         'number_of_persons_killed', 'number_of_pedestrians_injured', 'number_of_pedestrians_killed',\
         'number_of_cyclist_injured', 'number_of_cyclist_killed','number_of_motorist_injured',\
         'number_of_motorist_killed', 'contributing_factor_vehicle_1',  'vehicle_type_code1', 'latitude','longitude'\
        ]]
    df.to_parquet("./raw/raw_vehicle_collision_dataset.parquet")
    return "extraction is done."

@task(log_prints=True)
def transform_dataset():
    df = pd.read_parquet("./raw/raw_vehicle_collision_dataset.parquet")
    df["crash_date"] = pd.to_datetime(df["crash_date"])
    df["number_of_persons_injured"] = df["number_of_persons_injured"].astype('int64')
    df["number_of_persons_killed"] = df["number_of_persons_killed"].astype('int64')
    df["number_of_pedestrians_injured"] = df["number_of_pedestrians_injured"].astype('int64')
    df["number_of_pedestrians_killed"] = df["number_of_pedestrians_killed"].astype('int64')
    df["number_of_cyclist_injured"] = df["number_of_cyclist_injured"].astype('int64')
    df["number_of_cyclist_killed"] = df["number_of_cyclist_killed"].astype('int64')
    df["number_of_motorist_injured"] = df["number_of_motorist_injured"].astype('int64')
    df["number_of_motorist_killed"] = df["number_of_motorist_killed"].astype('int64')
    df["latitude"] = df["latitude"].astype('float64')
    df["longitude"] = df["longitude"].astype('float64')
    #
    df["latitude"] = df["latitude"].fillna(0)
    df["longitude"] = df["longitude"].fillna(0)
    df["on_street_name"] = df["on_street_name"].fillna("NA")
    df["vehicle_type_code1"] = df["vehicle_type_code1"].fillna("NA")
    df["contributing_factor_vehicle_1"] = df["contributing_factor_vehicle_1"].fillna("NA")
    df.drop_duplicates(inplace=True)
    df.to_parquet("./transformed/transformed_vehicle_collision_dataset.parquet")
    return "transformation is done."

def connect_2_db(uidd, pwdd, dserver, dport, ddb):
    return create_engine(f'postgresql://{uidd}:{pwdd}@{dserver}:{dport}/{ddb}')
# connect to database
connection = connect_2_db("sakar_", "sakar10", "localhost", 5432, "postgres")

#(secrets["API_USER"], secrets["API_PWD"], secrets["API_HOST"], 5432, secrets["API_DB"])
@task(log_prints=True)
def load_2_database():
    def check_tbl_exist():
        try: 
            connection.execute(text("""CREATE TABLE IF NOT EXISTS vehicle_collision(
                                                                                    collision_id VARCHAR, 
                                                                                    crash_date DATE,
                                                                                    crash_time VARCHAR,
                                                                                    on_street_name VARCHAR,
                                                                                    number_of_persons_injured INT,
                                                                                    number_of_persons_killed INT,
                                                                                    number_of_pedestrians_injured INT,
                                                                                    number_of_pedestrians_killed INT,
                                                                                    number_of_cyclist_injured INT,
                                                                                    number_of_cyclist_killed INT,
                                                                                    number_of_motorist_injured INT,
                                                                                    number_of_motorist_killed INT,
                                                                                    contributing_factor_vehicle_1 VARCHAR, 
                                                                                    vehicle_type_code1 VARCHAR,
                                                                                    latitude FLOAT,
                                                                                    longitude FLOAT
                                                                                    );"""
                                   )
                              )
        except Exception as e:
            print(e) 
        return True
    
    if check_tbl_exist() is True:
        df = pd.read_parquet("./transformed/transformed_vehicle_collision_dataset.parquet")
        # load data to table
        df.to_sql("vehicle_collision", con=connection, if_exists='append', index=False)
            
    return "data is loaded into a data warehouse."

@task(log_prints=True)
def upload_2_bucket(bucket_name, local_dir, remote_dir):
    try:
        with open('deep-contact-credentials.json', 'r') as f:
            client = storage.Client.from_service_account_json('deep-contact-credentials.json')
    except Exception as e:
        print(e)
    
    bucket = client.bucket(bucket_name)
    for root, dirs, files in os.walk(local_dir):
        for file in files:
            local_path = os.path.join(root, file)
            remote_path = remote_dir+file
            blob = bucket.blob(remote_path)
            print(f'uploading file: {local_path}')
            try:
                blob.upload_from_filename(local_path)
            except Exception as e:
                print(f'Error uploading file: {e}')
            print('files successfully uploaded')

@task(log_prints=True)
def load_2_cloud_storage():
    try:
        upload_2_bucket("invoice-text-extraction", "./transformed", "processed_csv")
    except Exception as e:
        print("unable to upload file")

@task(log_prints=True)
def drop_tmp_dir():
    shutil.rmtree(directory_raw)
    time.sleep(0.5)
    shutil.rmtree(directory_transformed)

# 'owner' : 'data engineering department'
# dag_id = 'vehicle-collision-dag-1'

@flow(retries=2, retry_delay_seconds=3)
def prefect_flow(name="vehicle-collision-flow"):
    # Note: If waiting for one task it must still be in a list.
    dir_create = temp_dir.submit()
    extract_data = extract_from_api.submit()
    transform_data = transform_dataset.submit(wait_for=[dir_create, extract_data])
    load_data_2_dwh = load_2_database(wait_for=[transform_data])
    load_data_2_cloud = load_2_cloud_storage(wait_for=[transform_data])
    drop_tmp_folder = drop_tmp_dir.submit(wait_for=[load_data_2_dwh, load_data_2_cloud])
    # hospital_clustering_{int(datetime.now().timestamp())}.csv')

if __name__ == '__main__':
    prefect_flow() #.visualize()

# prefect orion start
# pip install anyio==3.7.1
# rm ~/.prefect/orion.db*

# docker build . -t test:latest
# docker run --name mycontainername -i -t test:latest sh