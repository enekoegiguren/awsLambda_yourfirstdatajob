from datetime import datetime, date, timedelta
import os
from dotenv import load_dotenv
import re
import pickle
from io import BytesIO

import pandas as pd
from sqlalchemy import create_engine



from utils.credentials import *
from utils.get_data import *
from utils.transform_data import *

load_dotenv('.env')
conn_url = os.getenv('DB_CONN_URL')
ip = os.getenv('IP')


client_id, client_secret = get_secret()


def get_connection(conn_url):
    engine = create_engine(conn_url)
    
    return engine


def get_existing_ids():
    conn = get_connection(conn_url)
    query = "SELECT id FROM ft_jobdata"
    existing_ids = pd.read_sql(query, conn)
    return set(existing_ids['id'])

def query():
    conn = get_connection(conn_url)
    query = "SELECT * FROM ft_jobdata"
    query_result = pd.read_sql(query, conn)
    return query_result

def filter_new_rows(df, existing_ids):
    return df[~df['id'].isin(existing_ids)]

def append_to_db(df, conn_url):
    conn = get_connection(conn_url)
    with conn.begin():  # Commence une transaction
        try:
            df.to_sql('ft_jobdata', conn, if_exists='append', index=False)
        except Exception as e:
            print(f"Erreur lors de l'insertion: {e}")
            

# def upload_to_s3(dataframe, bucket_name, file_name):
#     """
#     Uploads a DataFrame to an S3 bucket as a Parquet file.

#     Parameters:
#     - dataframe (pd.DataFrame): DataFrame to upload.
#     - bucket_name (str): S3 bucket name.
#     - file_name (str): Name of the file to save in the bucket.
#     """
#     # Convert DataFrame to Parquet in memory
#     parquet_buffer = BytesIO()
#     dataframe.to_parquet(parquet_buffer, index=False)

#     # Upload to S3
#     s3_client = boto3.client('s3')
#     try:
#         parquet_buffer.seek(0)  # Move to the beginning of the buffer
#         s3_client.put_object(Bucket=bucket_name, Key=file_name, Body=parquet_buffer.getvalue())
#         print(f"Data successfully uploaded to {bucket_name}/{file_name}")
#     except Exception as e:
#         print(f"Failed to upload data to S3: {e}")

def list_parquet_files(bucket_name, prefix=""):
    s3_client = boto3.client('s3')
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    
    if 'Contents' in response:
        return [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.parquet')]
    return []

def load_parquet_from_s3(bucket_name, file_key):
    s3_client = boto3.client('s3')
    parquet_object = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    return pd.read_parquet(BytesIO(parquet_object['Body'].read()))

def delete_parquet_files(bucket_name, files):
    """
    Deletes specified files from the S3 bucket.

    Parameters:
    - bucket_name (str): S3 bucket name.
    - files (list): List of file keys to delete.
    """
    s3_client = boto3.client('s3')
    delete_objects = [{'Key': file} for file in files]
    s3_client.delete_objects(Bucket=bucket_name, Delete={'Objects': delete_objects})

def upload_to_s3(dataframe, bucket_name, file_name):
    parquet_buffer = BytesIO()
    dataframe.to_parquet(parquet_buffer, index=False)

    s3_client = boto3.client('s3')
    parquet_buffer.seek(0)
    s3_client.put_object(Bucket=bucket_name, Key=file_name, Body=parquet_buffer.getvalue())
    print(f"Data successfully uploaded to {bucket_name}/{file_name}")

def merge_and_update_parquet(bucket_name, new_df, prefix=""):
    """
    Merges all Parquet files in S3 with new rows and uploads a single Parquet file.

    Parameters:
    - bucket_name (str): S3 bucket name.
    - new_df (pd.DataFrame): New DataFrame with potential new rows.
    - prefix (str): Prefix to filter files in the bucket (default is empty).
    """
    # Step 1: List and load all Parquet files from the bucket
    parquet_files = list_parquet_files(bucket_name, prefix)
    all_dataframes = []

    for file_key in parquet_files:
        df = load_parquet_from_s3(bucket_name, file_key)
        all_dataframes.append(df)

    # Step 2: Concatenate all existing data and drop duplicates
    if all_dataframes:
        merged_df = pd.concat(all_dataframes).drop_duplicates(subset='id')
    else:
        merged_df = pd.DataFrame()  # If no files are present

    # Step 3: Filter new rows and concatenate with merged data
    unique_new_rows = new_df[~new_df['id'].isin(merged_df['id'])]
    final_df = pd.concat([merged_df, unique_new_rows]).drop_duplicates(subset='id')

    # Step 4: Upload final DataFrame as a single Parquet file
    extracted_date = pd.Timestamp.now().strftime("%Y%m%d")
    output_file = f"jobdata_{extracted_date}.parquet"
    upload_to_s3(final_df, bucket_name, output_file)

    # Step 5: Delete old Parquet files
    delete_parquet_files(bucket_name, parquet_files)
    print("All previous Parquet files deleted from the bucket.")
    
def process_and_insert_data(min_data, max_data, max_results, mots, client_id, client_secret):
    # Charger les données
    df = get_data(min_data, max_data, max_results, mots, client_id, client_secret)
    
    # Classifier et préparer les données
    df['job_category'] = df['title'].apply(classify_job_title)
    df['chef'] = df['title'].apply(classify_job_title_chef)
    df = df[df['job_category'] != 'Other']
    
    df = dates(df)
    df = skills(df)
    
    # Ajouter la date d'extraction
    extracted_date = datetime.now().strftime('%Y-%m-%d')
    df['extracted_date'] = extracted_date
    
    df = df.rename(columns = {'power bi':'power_bi',
                              'data warehouse':'data_warehouse',
                              'data lake':'data_lake',
                              'power query':'power_query',
                              'machine learning':'machine_learning',
                              'deep learning':'deep_learning',
                              'data governance':'data_governance',
                              'azure devops':'azure_devops'})
    
    df['experience_bool'] = df['experience_bool'].apply(map_experience)
    df['experience'] = df['experience'].apply(extract_experience)
    salary_data = df['salary'].apply(extract_salary)

    # Création des nouvelles colonnes pour le salaire minimum, maximum et moyen
    df['min_salary'] = salary_data.apply(lambda x: x['min_salary'])
    df['max_salary'] = salary_data.apply(lambda x: x['max_salary'])
    df['avg_salary'] = salary_data.apply(lambda x: x['avg_salary'])
    df.drop(columns=['salary', 'description'], inplace=True)
    

    existing_ids = get_existing_ids()
    new_rows = filter_new_rows(df, existing_ids)
    print(f"New rows to insert: {len(new_rows)}")

    if not new_rows.empty:
        merge_and_update_parquet("francejobdata", new_rows, prefix="")  
        append_to_db(new_rows, conn_url)
    else:
        print("Aucune nouvelle donnée à insérer")
        
def full_charge():
    min_data = '2022-01-01'
    extracted_date = datetime.now().strftime('%Y-%m-%d')
    #max_data = '2024-10-19'
    max_results = 3000
    mots = 'data'

    process_and_insert_data(min_data, extracted_date, max_results, mots, client_id, client_secret)

    return "Data inserted"
    
def last_month_charge():
    #first day of the month
    current_date = datetime.now()
    first_day_of_current_month = current_date.replace(day=1)
    first_day_str = first_day_of_current_month.strftime('%Y-%m-%d')
    
    #current date
    extracted_date = datetime.now().strftime('%Y-%m-%d')
    max_results = 3000
    mots = 'data'

    process_and_insert_data(first_day_str, extracted_date, max_results, mots, client_id, client_secret)
    
def requested_date_charge(first_date, last_date):
    #first day of the month
    first_date = datetime.strptime(first_date, '%Y-%m-%d')
    last_date = datetime.strptime(last_date, '%Y-%m-%d')
    
    first_day_str = first_date.strftime('%Y-%m-%d')
    last_day_str = last_date.strftime('%Y-%m-%d')
    #current date
    max_results = 3000
    mots = 'data'

    process_and_insert_data(first_day_str, last_day_str, max_results, mots, client_id, client_secret)
    

    