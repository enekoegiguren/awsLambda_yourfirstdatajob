from datetime import datetime
import os
from dotenv import load_dotenv
import pickle
from io import BytesIO

import pandas as pd
import boto3
from sqlalchemy import create_engine

import sys
import os

# Add the parent directory to the system path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.credentials import get_secret
from utils.get_data import get_data
from utils.transform_data import classify_job_title, classify_job_title_chef, dates, skills, map_experience, extract_experience, extract_salary

# Load environment variables
load_dotenv('.env')
conn_url = os.getenv('DB_CONN_URL')
ip = os.getenv('IP')
bucket_name = "francejobdata"

# Initialize secrets
client_id, client_secret = get_secret()


# Database Connection
def get_connection(conn_url):
    engine = create_engine(conn_url)
    return engine


# Retrieve existing IDs from the database
def get_existing_ids():
    conn = get_connection(conn_url)
    query = "SELECT id FROM ft_jobdata"
    existing_ids = pd.read_sql(query, conn)
    return set(existing_ids['id'])


# Append new rows to the database
def append_to_db(df, conn_url):
    conn = get_connection(conn_url)
    try:
        with conn.begin():
            df.to_sql('ft_jobdata', conn, if_exists='append', index=False)
    except Exception as e:
        print(f"Error during database insertion: {e}")


# S3 Operations
def get_s3_client():
    return boto3.client('s3')


def list_parquet_files(bucket_name, prefix=""):
    s3_client = get_s3_client()
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    return [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.parquet')]


def load_parquet_from_s3(bucket_name, file_key):
    s3_client = get_s3_client()
    parquet_object = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    return pd.read_parquet(BytesIO(parquet_object['Body'].read()))


def delete_parquet_files(bucket_name, files):
    s3_client = get_s3_client()
    delete_objects = [{'Key': file} for file in files]
    s3_client.delete_objects(Bucket=bucket_name, Delete={'Objects': delete_objects})


def upload_to_s3(dataframe, bucket_name, file_name):
    parquet_buffer = BytesIO()
    dataframe.to_parquet(parquet_buffer, index=False)
    s3_client = get_s3_client()
    parquet_buffer.seek(0)
    s3_client.put_object(Bucket=bucket_name, Key=file_name, Body=parquet_buffer.getvalue())
    print(f"Data successfully uploaded to {bucket_name}/{file_name}")


# Data Processing Functions
def filter_new_rows(df, existing_ids):
    return df[~df['id'].isin(existing_ids)]


def merge_and_update_parquet(bucket_name, new_df, prefix=""):
    # List parquet files in the bucket
    parquet_files = list_parquet_files(bucket_name, prefix)
    print(f"Parquet files found: {parquet_files}")
    
    # Check if any parquet files exist
    if not parquet_files:
        print("No existing parquet files found. Uploading new DataFrame directly.")
        extracted_date = pd.Timestamp.now().strftime("%Y%m%d")
        output_file = f"jobdata_{extracted_date}.parquet"
        upload_to_s3(new_df, bucket_name, output_file)
        print(f"Uploaded new DataFrame to {output_file}.")
        return  # Exit the function after uploading

    all_dataframes = []
    
    # Load existing parquet files into DataFrames
    for file_key in parquet_files:
        df = load_parquet_from_s3(bucket_name, file_key)
        print(f"Loaded {file_key} with columns: {df.columns} and shape: {df.shape}")
        
        # Check for 'id' column presence
        if 'id' not in df.columns:
            print(f"Warning: {file_key} does not contain 'id' column.")
            continue  # Skip this DataFrame if it lacks the 'id' column
        
        all_dataframes.append(df)

    if all_dataframes:
        merged_df = pd.concat(all_dataframes, ignore_index=True).drop_duplicates(subset='id')
        print(f"Merged DataFrame shape: {merged_df.shape} with columns: {merged_df.columns}")
    else:
        merged_df = pd.DataFrame()
        print("No DataFrames loaded. Returning empty DataFrame.")
        
    if merged_df.empty or 'id' not in merged_df.columns:
        print("Merged DataFrame is empty or does not contain an 'id' column.")
        return pd.DataFrame()  # Handle this case as necessary

    # Identify unique new rows to add to the merged DataFrame
    unique_new_rows = new_df[~new_df['id'].isin(merged_df['id'])]
    final_df = pd.concat([merged_df, unique_new_rows]).drop_duplicates(subset='id')

    # Prepare to upload the final DataFrame, replacing the old file
    extracted_date = pd.Timestamp.now().strftime("%Y%m%d")
    output_file = f"jobdata_{extracted_date}.parquet"

    # Upload the merged DataFrame to S3, replacing the old parquet file
    upload_to_s3(final_df, bucket_name, output_file)
    print(f"Updated the DataFrame and uploaded to {output_file}. Old parquet files retained in the bucket.")





def process_and_insert_data(min_data, max_data, max_results, mots, client_id, client_secret):
    df = get_data(min_data, max_data, max_results, mots, client_id, client_secret)
    
    # Perform transformations
    df['job_category'] = df['title'].apply(classify_job_title)
    df['chef'] = df['title'].apply(classify_job_title_chef)
    df = df[df['job_category'] != 'Other']
    
    df = dates(df)
    df = skills(df)
    df['extracted_date'] = datetime.now().strftime('%Y-%m-%d')

    # Renaming columns
    df = df.rename(columns={'power bi': 'power_bi', 'data warehouse': 'data_warehouse',
                            'data lake': 'data_lake', 'power query': 'power_query',
                            'machine learning': 'machine_learning', 'deep learning': 'deep_learning',
                            'data governance': 'data_governance', 'azure devops': 'azure_devops'})

    # Process experience and salary
    df['experience_bool'] = df['experience_bool'].apply(map_experience)
    df['experience'] = df['experience'].apply(extract_experience)
    salary_data = df['salary'].apply(extract_salary)
    df['min_salary'] = salary_data.apply(lambda x: x['min_salary'])
    df['max_salary'] = salary_data.apply(lambda x: x['max_salary'])
    df['avg_salary'] = salary_data.apply(lambda x: x['avg_salary'])
    df.drop(columns=['salary', 'description'], inplace=True)

    # Update parquet and existing IDs
    existing_data = merge_and_update_parquet(bucket_name, df, prefix="")
    
    # Ensure existing_data is valid before proceeding
    if isinstance(existing_data, pd.DataFrame) and not existing_data.empty and 'id' in existing_data.columns:
        existing_ids = set(existing_data['id'])
    else:
        existing_ids = set()

    new_rows = filter_new_rows(df, existing_ids)
    
    print(f"New rows to insert: {len(new_rows)}")
    
    if not new_rows.empty:
        # Attempt to merge and update the parquet files in the S3 bucket
        merge_and_update_parquet(bucket_name, new_rows, prefix="")
        
        # Try to append new rows to the database
        try:
            append_to_db(new_rows, conn_url)
            print(f"{len(new_rows)} rows appended to the database successfully.")
        except Exception as e:
            print(f"Error while appending to the database: {e}")
            print(f"{len(new_rows)} rows appended to the bucket but the database is not currently working.")
    else:
        print("No new data to insert.")



# Execution Functions
def full_charge():
    process_and_insert_data('2022-01-01', datetime.now().strftime('%Y-%m-%d'), 3000, 'data', client_id, client_secret)
    return "Data inserted"


def last_month_charge():
    current_date = datetime.now()
    first_day_str = current_date.replace(day=1).strftime('%Y-%m-%d')
    process_and_insert_data(first_day_str, current_date.strftime('%Y-%m-%d'), 3000, 'data', client_id, client_secret)


def requested_date_charge(first_date, last_date):
    process_and_insert_data(first_date, last_date, 3000, 'data', client_id, client_secret)


