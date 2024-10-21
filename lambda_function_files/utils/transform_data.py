import pandas as pd
import re

from utils.credentials import *
from utils.get_data import *


def classify_job_title(title):
    title = title.lower()  # Convert to lowercase for consistent matching
    if re.search(r'business analyst', title):
        return 'Business Analyst'
    #elif re.search(r'migration|intégration', title):
    #    return 'Data Migration'
    elif re.search(r'owner|product|product owner', title):
        return 'Data Product Owner'
    elif re.search(r'chef de projet', title):
        return 'Data Project Manager'
    elif re.search(r'consultant bi|bi', title):
        return 'Consultant BI'
    elif re.search(r'consultant data|consultant', title):
        return 'Consultant Data'
    #elif re.search(r'chief data officer', title):
    #    return 'Chief Data Officer'
    elif re.search(r'back|développeur|infra|software', title):
        return 'Software - Développeur Data'
    elif re.search(r'tech', title):
        return 'Technicien Data'
    elif re.search(r'assistant', title):
        return 'Data Assistant'
    elif re.search(r'data engineer|ingénieur|engineer|ingenieur', title):
        return 'Data Engineer'
    elif re.search(r'data analyst|analyste|analytics', title):
        return 'Data Analyst'
    elif re.search(r'data architect|architect', title):
        return 'Data Architect'
    elif re.search(r'data scientist|scientifique|science', title):
        return 'Data Scientist'
    elif re.search(r'data manager|gestionnaire|manager|administrator|manager|gestion', title):
        return 'Data Manager'
    elif re.search(r'data', title):
        return 'Other Data Position'
    else:
        return 'Other'
    
# Function to classify job titles
def classify_job_title_chef(title):
    title = title.lower()  # Convert to lowercase for consistent matching
    if re.search(r'chef', title):
        return 'Chef'
    else:
        return 'Other'
    


# Function to check for keyword presence in the description
def check_keyword_presence(description, keyword):
    # Search for the keyword in the description (case-insensitive)
    return 'Y' if re.search(rf'\b{keyword.lower()}\b', description.lower()) else 'N'

def dates(df):
    df['date_creation'] = pd.to_datetime(df['date_creation'])
    # Extract date in 'yyyy-mm-dd' format

    # Extract year, month, and day into separate columns
    df['year'] = df['date_creation'].dt.year
    df['month'] = df['date_creation'].dt.month
    df['day'] = df['date_creation'].dt.day
    df['date_creation'] = df['date_creation'].dt.strftime('%Y-%m-%d')
    return df

def skills(df):
    keywords = [
    'sql', 'python', 'pyspark', 'azure', 'aws', 'gcp', 'etl', 'airflow', 'kafka', 'spark', 
    'power bi', 'tableau', 'snowflake', 'docker', 'kubernetes', 'git', 'data warehouse', 
    'hadoop', 'mlops', 'data lake', 'bigquery', 'databricks', 'dbt', 'mlflow',
    'java', 'scala', 'sas', 'matlab', 'power query', 'looker', 'apache', 'hive', 
    'terraform', 'jenkins', 'gitlab', 'machine learning', 'deep learning', 'nlp', 
    'api', 'pipeline', 'data governance','erp', 'ssis', 'ssas', 'ssrs', 'ssms', 'postgre', 'mysql', 'mongodb', 'cloud',
    
    
    # Azure components
    'synapse', 'blobstorage', 
    'azure devops',
    'fabric',

    # AWS components
    'glue', 'redshift', 's3', 'lambda', 'emr', 'athena',
    'kinesis', 'rds', 'sagemaker',
    ]
    
    # Apply the function for each keyword and create a new column
    for keyword in keywords:
        df[keyword] = df['description'].apply(lambda x: check_keyword_presence(x, keyword))
    return df


def map_experience(value):
    return 'Y' if value == 'E' else 'N'


def extract_experience(value):

    if value == 'Débutant accepté':
        return 0
    

    match_years = re.search(r'(\d+)\s*An\(s\)', value)  
    match_months = re.search(r'(\d+)\s*Mo(i)?s', value) 
    

    if match_years:
        return int(match_years.group(1))  
    elif match_months:
        return int(match_months.group(1)) / 12  

    return None  



def extract_salary(value):
    if pd.isna(value) or value is None:
        return {'min_salary': None, 'max_salary': None, 'avg_salary': None}  

    monthly_match = re.search(r'Mensuel de (\d+(?:\.\d+)?) Euros(?: à (\d+(?:\.\d+)?))?', value)

    annual_match = re.search(r'Annuel de (\d+(?:\.\d+)?) Euros(?: à (\d+(?:\.\d+)?))?', value)

    if monthly_match:
        min_salary = float(monthly_match.group(1)) * 12  
        max_salary = float(monthly_match.group(2)) * 12 if monthly_match.group(2) else min_salary
    elif annual_match:
        min_salary = float(annual_match.group(1)) 
        max_salary = float(annual_match.group(2)) if annual_match.group(2) else min_salary
    else:
        return {'min_salary': None, 'max_salary': None, 'avg_salary': None}  

    avg_salary = (min_salary + max_salary) / 2  
    return {'min_salary': min_salary, 'max_salary': max_salary, 'avg_salary': avg_salary}
