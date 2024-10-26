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
        # Retrieve client_id and client_secret
        client_id, client_secret = get_secret()
        
        if not client_id or not client_secret:
            raise ValueError("Client ID or Client Secret not found in the retrieved secret.")

        logging.info("Handler started with client_id: %s", client_id)

        # Call the full_charge function
        full_charge()

        logging.info("Handler completed successfully")
        return {
            'statusCode': 200,
            'body': 'Execution successful'
        }
    except Exception as e:
        logging.error(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': f"Error occurred: {str(e)}"
        }