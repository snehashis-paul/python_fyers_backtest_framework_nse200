from fyers_generate_token import generate_access_token
import logging
import os
import json
from hashlib import sha256
from urllib import parse
import pyotp
import requests
from fyers_apiv3 import fyersModel
import pandas as pd
from sqlalchemy import create_engine
import pytz
import psycopg2
from psycopg2 import sql
from datetime import datetime, timedelta

requester = "TEST BOT"
# Database Details
db_params = {
    'host': 'localhost',
    'database': 'botdb',
    'user': 'darwin',
    'password': 'Pa55w0rd',
    'port': '54320'
}
SUCCESS = 1
ERROR = -1
URL_MAIN = "https://api-t1.fyers.in/api/v3"

def setup_logger(log_file_path):
    # Create a logger
    logger = logging.getLogger('my_logger')
    logger.setLevel(logging.DEBUG)
    # Create a file handler and set the level to DEBUG
    file_handler = logging.FileHandler(log_file_path)
    file_handler.setLevel(logging.DEBUG)
    # Create a formatter and set the formatter for the handler
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%d/%m/%Y %H:%M:%S')
    file_handler.setFormatter(formatter)
    # Add the file handler to the logger
    logger.addHandler(file_handler)
    return logger


# Get the path to the directory where the script is located
script_directory = os.path.dirname(os.path.abspath(__file__))
# Create log file name with the specified format
log_file_name = datetime.now().strftime('%d_%m_%Y_%H_%M_%S') + "_TESTBOT.log"
# Specify the log file path
log_file_path = f"{script_directory}/logs/{log_file_name}"
# Setup the logger
logger = setup_logger(log_file_path)
'''ALL LOGIC TO BE DEFINED HERE'''
# Get the current time in UTC
current_time_utc = datetime.utcnow()
# Define the time zones
india_timezone = pytz.timezone('Asia/Kolkata')  # India Standard Time (IST)
mauritius_timezone = pytz.timezone('Indian/Mauritius')  # Mauritius Standard Time (MUT)
# Convert UTC time to IST and MUT
current_time_india = current_time_utc.replace(tzinfo=pytz.utc).astimezone(india_timezone)
current_time_mauritius = current_time_utc.replace(tzinfo=pytz.utc).astimezone(mauritius_timezone)
current_time_india = current_time_india.strftime("%Y-%m-%d %H:%M:%S")
logger.info(f"{requester} ON MAURITIUS TIME: {current_time_mauritius} | INDIA TIME: {current_time_india}")
print(f"{requester} ON MAURITIUS TIME: {current_time_mauritius} | INDIA TIME: {current_time_india}")


def verify_token(token):
    test_url = URL_MAIN+ "/profile"
    # Construct the database URI
    db_uri = f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['database']}"
    # Create an SQLAlchemy engine
    engine = create_engine(db_uri)
    # SQL query to select all rows from the global_vars table
    sql_query = "SELECT * FROM global_vars;"
    # Load data into a Pandas DataFrame using SQLAlchemy engine
    df = pd.read_sql_query(sql_query, engine)
    # Extract key_var values as variables
    # user_id = df[df['key_var'] == 'USER ID']['val_var'].values[0]
    # user_pin = df[df['key_var'] == 'PIN']['val_var'].values[0]
    # totp_key = df[df['key_var'] == 'TOTP KEY']['val_var'].values[0]
    # app_name = df[df['key_var'] == 'APP']['val_var'].values[0]
    # app_id = df[df['key_var'] == 'APP ID']['val_var'].values[0]
    # APP_TYPE = df[df['key_var'] == 'APP TYPE']['val_var'].values[0]
    # app_secret = df[df['key_var'] == 'SECRET ID']['val_var'].values[0]
    # redirect_url = df[df['key_var'] == 'REDIRECT']['val_var'].values[0]
    broker = df[df['key_var'] == 'BROKER']['val_var'].values[0]
    client_id = df[df['key_var'] == 'CLIENT ID']['val_var'].values[0]
    engine.dispose()
    fyers = fyersModel.FyersModel(client_id=client_id, is_async=False, token=token, log_path="")
    # Make a request to get the user profile information
    response = fyers.get_profile()
    if response["s"] == "ok":
        # print(f"ACCESS TOKEN GENERATED FOR PROFILE - {response['data']['name']}")
        logger.info(f"ACCESS TOKEN VALIDATED FOR PROFILE - {response['data']['name']}")
        return [SUCCESS, response['data']['name']]
    else:
        logger.error(f"ACCESS TOKEN VALIDATION FAILED - {response}")

        token=None
        return [ERROR, token]


def fetch_token(connection_params):
    # Connect to the PostgreSQL database
    try:
        connection = psycopg2.connect(**connection_params)
        cursor = connection.cursor()
        # Execute the SELECT query
        record_id = 1
        query = f"SELECT * FROM fyers_access_token WHERE id = {record_id};"
        cursor.execute(query)
        # Fetch all records
        record = cursor.fetchone()
        if record:
            # Convert record to a dictionary for easy access to key-values
            record_dict = {
                'id': record[0],
                'access_token': str(record[1]),
                'generated_date': str(record[2]),
                'expiry_date': str(record[3]),
            }
            token = record_dict['access_token']
            print("ACCESS TOKEN FETCHED FROM DB")
            if token is not None:
                # verify whether access token is expired or not
                expiry = datetime.strptime(record_dict['expiry_date'], "%Y-%m-%d %H:%M:%S.%f")
                expiry = expiry.strftime("%Y-%m-%d %H:%M:%S")
                print(f"ACCESS TOKEN EXPIRY TIME: {expiry}")
                if expiry > current_time_india:
                    logger.info(f"ACCESS TOKEN IS NOT EXPIRED\n{token}")
                    print("ACCESS TOKEN IS NOT EXPIRED")
                    flag = verify_token(token)
                    if flag[0] != SUCCESS:
                        token = None
                        print(f"ACCESS TOKEN VALIDATION FAILED")
                    else:
                        print(f"ACCESS TOKEN VALIDATED FOR PROFILE - {flag[1]}")
                else:
                    logger.warn("ACCESS TOKEN IS EXPIRED")
                    token = None
            else:
                logger.error("NO ACCESS TOKEN IN DB RECORD")
                token = None
        else:
            logger.error("NO ACCESS TOKEN IN DB")
            token = None
    except Exception as e:
        logger.error(f"UNABLE TO FETCH ACCESS TOKEN FROM DB - {e}")
        token = None
    finally:
        # Close the cursor and connection
        cursor.close()
        connection.close()
    return token


token = fetch_token(db_params)
while token is None:
    logger.info("ATTEMPT TO GENERATE ACCESS TOKEN")
    token = generate_access_token(requester)
    if token is not None:
        logger.info(f"NEW ACCESS TOKEN: {token}")
    else:
        logger.error(f"ACCESS TOKEN GENERATION FAILED.")
logger.info("BOT LOGIC EXECUTION STARTED")

logger.info("EXECUTION END: SUCCESS")
# Close the logger explicitly when you're done
logging.shutdown()