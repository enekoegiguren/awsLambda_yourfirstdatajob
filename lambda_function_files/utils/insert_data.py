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
            

def upload_to_s3(dataframe, bucket_name, file_name):
    """
    Uploads a DataFrame to an S3 bucket as a Parquet file.

    Parameters:
    - dataframe (pd.DataFrame): DataFrame to upload.
    - bucket_name (str): S3 bucket name.
    - file_name (str): Name of the file to save in the bucket.
    """
    # Convert DataFrame to Parquet in memory
    parquet_buffer = BytesIO()
    dataframe.to_parquet(parquet_buffer, index=False)

    # Upload to S3
    s3_client = boto3.client('s3')
    try:
        parquet_buffer.seek(0)  # Move to the beginning of the buffer
        s3_client.put_object(Bucket=bucket_name, Key=file_name, Body=parquet_buffer.getvalue())
        print(f"Data successfully uploaded to {bucket_name}/{file_name}")
    except Exception as e:
        print(f"Failed to upload data to S3: {e}")
    
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

    if not new_rows.empty:
        append_to_db(new_rows, conn_url)
        upload_to_s3(new_rows, "francejobdata", f"jobdata_{extracted_date}.parquet")  
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
    

    