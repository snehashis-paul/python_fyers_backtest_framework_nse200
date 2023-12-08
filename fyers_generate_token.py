'''
This code will be imported into other bots to generate Fyers access token if the bot's access token expires.

This code will do the following:

1. Connect to PGSQL botdb
2. Load the Fyers broker details
3. Request to Generate the access token
4. Retrieve request_key from send_login_otp API
5. Generate the TOTP
6. Verify the totp and get request key from verify_otp API [RETRY 2 times if totp invalid]
7. Verify the user pin and return the access token
8. Insert the new access token in the database table - fyers_access_token
'''

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

SUCCESS = 1
ERROR = -1
# Database Details
db_params = {
    'host': 'localhost',
    'database': 'botdb',
    'user': 'darwin',
    'password': 'Pa55w0rd',
    'port': '54320'
}
# Construct the database URI
db_uri = f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['database']}"
# Create an SQLAlchemy engine
engine = create_engine(db_uri)
# SQL query to select all rows from the global_vars table
sql_query = "SELECT * FROM global_vars;"
# Load data into a Pandas DataFrame using SQLAlchemy engine
df = pd.read_sql_query(sql_query, engine)
# Extract key_var values as variables
user_id = df[df['key_var'] == 'USER ID']['val_var'].values[0]
user_pin = df[df['key_var'] == 'PIN']['val_var'].values[0]
totp_key = df[df['key_var'] == 'TOTP KEY']['val_var'].values[0]
app_name = df[df['key_var'] == 'APP']['val_var'].values[0]
app_id = df[df['key_var'] == 'APP ID']['val_var'].values[0]
APP_TYPE = df[df['key_var'] == 'APP TYPE']['val_var'].values[0]
app_secret = df[df['key_var'] == 'SECRET ID']['val_var'].values[0]
redirect_url = df[df['key_var'] == 'REDIRECT']['val_var'].values[0]
broker = df[df['key_var'] == 'BROKER']['val_var'].values[0]
client_id = df[df['key_var'] == 'CLIENT ID']['val_var'].values[0]
engine.dispose()

APP_ID_TYPE = "2"  # Keep default as 2, It denotes web login
APP_ID_HASH = sha256((client_id + ":" + app_secret).encode('utf-8')).hexdigest()
# API endpoints
BASE_URL = "https://api-t2.fyers.in/vagator/v2"
BASE_URL_2 = "https://api.fyers.in/api/v2"
URL_SEND_LOGIN_OTP = BASE_URL + "/send_login_otp"
URL_VERIFY_TOTP = BASE_URL + "/verify_otp"
URL_VERIFY_PIN = BASE_URL + "/verify_pin"
URL_TOKEN = BASE_URL_2 + "/token"
URL_VALIDATE_AUTH_CODE = BASE_URL_2 + "/validate-authcode"

# Function to generate the log file
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

def send_login_otp(fy_id, app_id):
    try:
        payload = {
            "fy_id": fy_id,
            "app_id": app_id
        }
        result_string = requests.post(url=URL_SEND_LOGIN_OTP, json=payload)
        if result_string.status_code != 200:
            return [ERROR, result_string.text]
        result = json.loads(result_string.text)
        request_key = result["request_key"]
        return [SUCCESS, request_key]
    except Exception as e:
        return [ERROR, e]


def generate_totp(secret):
    try:
        generated_totp = pyotp.TOTP(secret).now()
        return [SUCCESS, generated_totp]
    except Exception as e:
        return [ERROR, e]


def verify_totp(request_key, totp):
    try:
        payload = {
            "request_key": request_key,
            "otp": totp
        }
        result_string = requests.post(url=URL_VERIFY_TOTP, json=payload)
        if result_string.status_code != 200:
            return [ERROR, result_string.text]

        result = json.loads(result_string.text)
        request_key = result["request_key"]
        return [SUCCESS, request_key]
    except Exception as e:
        return [ERROR, e]


def verify_PIN(request_key, pin):
    try:
        payload = {
            "request_key": request_key,
            "identity_type": "pin",
            "identifier": pin
        }
        result_string = requests.post(url=URL_VERIFY_PIN, json=payload)
        if result_string.status_code != 200:
            return [ERROR, result_string.text]
        result = json.loads(result_string.text)
        access_token = result["data"]["access_token"]
        return [SUCCESS, access_token]
    except Exception as e:
        return [ERROR, e]


def token(fy_id, app_id, redirect_uri, app_type, access_token):
    try:
        payload = {
            "fyers_id": fy_id,
            "app_id": app_id,
            "redirect_uri": redirect_uri,
            "appType": app_type,
            "code_challenge": "",
            "state": "sample_state",
            "scope": "",
            "nonce": "",
            "response_type": "code",
            "create_cookie": True
        }
        headers = {'Authorization': f'Bearer {access_token}'}
        result_string = requests.post(
            url=URL_TOKEN, json=payload, headers=headers
        )
        if result_string.status_code != 308:
            return [ERROR, result_string.text]
        result = json.loads(result_string.text)
        url = result["Url"]
        auth_code = parse.parse_qs(parse.urlparse(url).query)['auth_code'][0]
        return [SUCCESS, auth_code]
    except Exception as e:
        return [ERROR, e]


def validate_authcode(app_id_hash, auth_code):
    try:
        payload = {
            "grant_type": "authorization_code",
            "appIdHash": app_id_hash,
            "code": auth_code,
        }
        result_string = requests.post(url=URL_VALIDATE_AUTH_CODE, json=payload)
        if result_string.status_code != 200:
            return [ERROR, result_string.text]
        result = json.loads(result_string.text)
        access_token = result["access_token"]
        return [SUCCESS, access_token]
    except Exception as e:
        return [ERROR, e]

# Function to retrive the Fyers access token
def get_auth_code(logger):
    # print(f"AUTHENTICATE USING - USER ID: {user_id} | BROKER : {broker}")
    logger.info(f"AUTHENTICATE USING - USER ID: {user_id} | BROKER : {broker}")
    # Step 1 - Retrieve request_key from send_login_otp API
    send_otp_result = send_login_otp(fy_id=user_id, app_id=APP_ID_TYPE)
    if send_otp_result[0] != SUCCESS:
        # print(f"send_login_otp failure - {send_otp_result[1]}")
        logger.error(f"send_login_otp failure - {send_otp_result[1]}")
        return None
    else:
        # print("send_login_otp success")
        logger.info("send_login_otp success")

    # Step 2 - Generate totp
    generate_totp_result = generate_totp(secret=totp_key)
    if generate_totp_result[0] != SUCCESS:
        # print(f"generate_totp failure - {generate_totp_result[1]}")
        logger.error(f"generate_totp failure - {generate_totp_result[1]}")
        return None
    else:
        # print("generate_totp success")
        logger.info("generate_totp success")

    # Step 3 - Verify totp and get request key from verify_otp API
    request_key = send_otp_result[1]
    totp = generate_totp_result[1]
    verify_totp_result = verify_totp(request_key=request_key, totp=totp)
    if verify_totp_result[0] != SUCCESS:
        # print(f"verify_totp_result failure - {verify_totp_result[1]}")
        logger.error(f"verify_totp_result failure - {verify_totp_result[1]}")
        return None
    else:
        # print("verify_totp_result success")
        logger.info("verify_totp_result success")

    # Step 4 - Verify pin and send back access token
    request_key_2 = verify_totp_result[1]
    verify_pin_result = verify_PIN(request_key=request_key_2, pin=user_pin)
    if verify_pin_result[0] != SUCCESS:
        # print(f"verify_pin_result failure - {verify_pin_result[1]}")
        logger.error(f"verify_pin_result failure - {verify_pin_result[1]}")
        return None
    else:
        # print("verify_pin_result success")
        logger.info("verify_pin_result success")

    # Step 5 - Get auth code for API V2 App from trade access token
    token_result = token(
        fy_id=user_id, app_id=app_id, redirect_uri=redirect_url, app_type=APP_TYPE,
        access_token=verify_pin_result[1]
    )
    if token_result[0] != SUCCESS:
        # print(f"token_result failure - {token_result[1]}")
        logger.error(f"token_result failure - {token_result[1]}")
        return None
    else:
        # print("token_result success")
        logger.info("token_result success")

    # Step 6 - Get API V2 access token from validating auth code
    auth_code = token_result[1]
    validate_authcode_result = validate_authcode(
        app_id_hash=APP_ID_HASH, auth_code=auth_code
    )
    if token_result[0] != SUCCESS:
        # print(f"validate_authcode failure - {validate_authcode_result[1]}")
        logger.error(f"validate_authcode failure - {validate_authcode_result[1]}")
        return None
    else:
        # print("validate_authcode success")
        logger.info("validate_authcode success")

    access_token = validate_authcode_result[1]
    logger.info(f"access_token - {access_token}")
    return access_token


def execute_insert_statement(connection_params, token, logger):
    # Connect to the PostgreSQL database
    flag = False
    try:
        connection = psycopg2.connect(**connection_params)
        cursor = connection.cursor()
        # Create the INSERT statement
        insert_statement = sql.SQL("""
               INSERT INTO fyers_access_token (id,access_token, generated_date, expiry_date) VALUES (1, 
               {}, current_timestamp AT TIME ZONE 'Asia/Kolkata', current_timestamp AT TIME ZONE 'Asia/Kolkata' + INTERVAL '20 hours') 
               ON CONFLICT (id) DO UPDATE SET access_token = EXCLUDED.access_token, generated_date = EXCLUDED.generated_date,
               expiry_date = EXCLUDED.expiry_date;""").format(sql.Literal(token))
        # Execute the INSERT statement
        cursor.execute(insert_statement)
        # Commit the changes
        connection.commit()
        logger.info("ACCESS TOKEN INSERTED IN TABLE - fyers_access_token")
        flag = True
    except Exception as e:
        logger.error(f"Error: {e}")
    finally:
        # Close the cursor and connection
        cursor.close()
        connection.close()
    return flag


def generate_access_token(requester):
    # Get the path to the directory where the script is located
    script_directory = os.path.dirname(os.path.abspath(__file__))
    # Create log file name with the specified format
    log_file_name = datetime.now().strftime('%d_%m_%Y_%H_%M_%S') + "_GENERATE-ACCESS-TOKEN-FYERS.log"
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
    # Display the results
    # print(f"FYERS ACCESS TOKEN REQUESTED BY {requester} ON MAURITIUS TIME: {current_time_mauritius} | INDIA TIME: {current_time_india}")
    logger.info(f"FYERS ACCESS TOKEN REQUESTED BY {requester} ON MAURITIUS TIME: {current_time_mauritius} | INDIA TIME: {current_time_india}")
    URL_MAIN = "https://api-t1.fyers.in/api/v3"
    token = get_auth_code(logger)
    if token is not None:
        PROFILE_URL = URL_MAIN + "/profile"
        # Initialize the FyersModel instance with your client_id, access_token, and enable async mode
        fyers = fyersModel.FyersModel(client_id=client_id, is_async=False, token=token, log_path="")
        # Make a request to get the user profile information
        response = fyers.get_profile()
        if response["s"] == "ok":
            # print(f"ACCESS TOKEN GENERATED FOR PROFILE - {response['data']['name']}")
            logger.info(f"ACCESS TOKEN GENERATED FOR PROFILE - {response['data']['name']}")
            flag = execute_insert_statement(db_params, token, logger)
            if (flag):
                # print("DONE")
                logger.info("EXECUTION END: SUCCESS")
            else:
                # print("FAIL")
                logger.info("EXECUTION END: ABORT")
        else:
            logger.error(f"ACCESS TOKEN GENERATED UNUSABLE - {response}")
            logger.info("EXECUTION END: ABORT")
    else:
        logger.error(f"ACCESS TOKEN NOT GENERATED")
        logger.info("EXECUTION END: ABORT")
    # Close the logger explicitly when you're done
    logging.shutdown()
    return token


if __name__ == "__main__":
    # Call the main function with an argument string
    requester = "SELF"
    generate_access_token(requester)
