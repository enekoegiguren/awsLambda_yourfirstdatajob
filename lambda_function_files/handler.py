from datetime import datetime, date
import os
import re
import pickle

import pandas as pd
from sqlalchemy import create_engine


from utils.credentials import *
from utils.get_data import *
from utils.transform_data import *


client_id, client_secret = get_secret()

min_data = '2024-10-01'
max_data = '2024-10-19'
max_results = 50
mots = 'data'

# Load the Parquet file into a DataFrame
df = get_data(min_data,
              max_data,
              max_results,
              mots,
              client_id,
              client_secret)


df['job_category'] = df['title'].apply(classify_job_title)
df['chef'] = df['title'].apply(classify_job_title_chef)
df = df[df['job_category'] != 'Other']
df = dates(df)

df = skills(df)
df