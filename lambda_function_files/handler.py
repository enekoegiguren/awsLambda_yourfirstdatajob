from datetime import datetime, date
import os
import re
import pickle

import pandas as pd
from sqlalchemy import create_engine


from utils.credentials import *
from utils.get_data import *
from utils.transform_data import *
from utils.insert_data import *


import logging


# Set up logging
logging.basicConfig(level=logging.INFO)

def handler(event, context):
    try:
        client_id, client_secret = get_secret()
        
        last_month_charge()
        return {
            'statusCode': 200,
            'body': json.dumps('Function executed successfully!')
        }

    except Exception as e:

        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }