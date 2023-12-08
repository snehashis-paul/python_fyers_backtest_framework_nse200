import shutil

from fyers_generate_token import generate_access_token
import logging
import os
from fyers_apiv3 import fyersModel
import pandas as pd
from sqlalchemy import create_engine
import pytz
import psycopg2
import csv
from datetime import datetime, date, timedelta

requester = "NSE200 FETCH HISTORICAL DATA BOT"
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
period = ""
limit = 0

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
log_file_name = datetime.now().strftime('%d_%m_%Y_%H_%M_%S') + "_RCFHD.log"
# Specify the log file path
log_file_path = f"{script_directory}/logs/{log_file_name}"
# Setup the logger
logger = setup_logger(log_file_path)
# Get the current time in UTC
current_time_utc = datetime.utcnow()
# Define the time zones
india_timezone = pytz.timezone('Asia/Kolkata')  # India Standard Time (IST)
mauritius_timezone = pytz.timezone('Indian/Mauritius')  # Mauritius Standard Time (MUT)
# Convert UTC time to IST and MUT
current_time_india = current_time_utc.replace(tzinfo=pytz.utc).astimezone(india_timezone)
current_time_mauritius = current_time_utc.replace(tzinfo=pytz.utc).astimezone(mauritius_timezone)
current_time_india = current_time_india.strftime("%Y-%m-%d %H:%M:%S")
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
logger.info(f"{requester} ON MAURITIUS TIME: {current_time_mauritius} | INDIA TIME: {current_time_india}\nBROKER: {broker} | CLIENT ID: {client_id}")
print(f"{requester} ON MAURITIUS TIME: {current_time_mauritius} | INDIA TIME: {current_time_india}\nBROKER: {broker} | CLIENT ID: {client_id}")


def verify_token(token):
    test_url = URL_MAIN+ "/profile"
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
'''
****************************************************************************************
'''

# Take user input
print("PLEASE SELECT DESIRED DATA RESOLUTION:\n1. 1 minute\n2. 3 minute\n3. 5 minute\n4. 10 minute\n5. 15 minute\n"
      "6. 30 minute\n7. 1 hour\n8. 1 day")
while True:
    candle_duration = input("CANDLE DURATION: ")
    try:
        if int(candle_duration) > 0 and int(candle_duration) < 9:
            break
        else:
            print("Invalid candle duration selection. Please provide a valid selection.")
    except ValueError:
        print("Invalid candle duration selection. Please provide a valid selection.")

def validate_dates(start_date_str, end_date_str):
    def is_valid_date(date_object):
        # Check if the parsed date is not in the future
        if date_object > date.today():
            return False
        # Check if the parsed date is valid
        if date_object.year < 1 or date_object.month < 1 or date_object.day < 1:
            return False
        # Check if the number of days in the month is correct
        if date_object.day > 31 or (date_object.month in [4, 6, 9, 11] and date_object.day > 30) or (date_object.month == 2 and date_object.day > 29):
            return False
        # Check for leap year in February
        if date_object.month == 2 and date_object.day == 29:
            if (date_object.year % 4 != 0) or (date_object.year % 100 == 0 and date_object.year % 400 != 0):
                return False
        return True
    try:
        # Parse the input date strings
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()
        # Check if both parsed dates are valid
        if not is_valid_date(start_date) or not is_valid_date(end_date):
            return False
        # Check if start_date is not after end_date
        if start_date > end_date:
            return False
        # Check if start_date and end_date are not today
        if start_date == date.today() or end_date == date.today():
            return False
        # If all checks pass, the dates are valid
        return True
    except ValueError:
        return False

while True:
    print("PLEASE PROVIDE THE DATE RANGE FOR THE HISTORICAL DATA IN yyyy-mm-dd FORMAT\nStart date should not be after end date, and both should not be today.")
    start_date = input("ENTER THE START DATE [yyyy-mm-dd] : ")
    end_date = input("ENTER THE END DATE [yyyy-mm-dd] : ")
    if validate_dates(start_date,end_date):
        break
    else:
        print("Invalid dates. Please provide a valid dates.")

match candle_duration:
    case "1":
        print(f"OPTION {candle_duration} CHOSEN - 1 minute period candle historical data will be retrieved from {start_date} till {end_date}")
        logger.info(f"OPTION {candle_duration} CHOSEN - 1 minute period candle historical data will be retrieved from {start_date} till {end_date}")
        period = "1"
        limit = 100
    case "2":
        print(f"OPTION {candle_duration} CHOSEN - 3 minute period candle historical data will be retrieved from {start_date} till {end_date}")
        logger.info(f"OPTION {candle_duration} CHOSEN - 3 minute period candle historical data will be retrieved from {start_date} till {end_date}")
        period = "3"
        limit = 100
    case "3":
        print(f"OPTION {candle_duration} CHOSEN - 5 minute period candle historical data will be retrieved from {start_date} till {end_date}")
        logger.info(f"OPTION {candle_duration} CHOSEN - 5 minute period candle historical data will be retrieved from {start_date} till {end_date}")
        period = "5"
        limit = 100
    case "4":
        print(f"OPTION {candle_duration} CHOSEN - 10 minute period candle historical data will be retrieved from {start_date} till {end_date}")
        logger.info(f"OPTION {candle_duration} CHOSEN - 10 minute period candle historical data will be retrieved from {start_date} till {end_date}")
        period = "10"
    case "5":
        print(f"OPTION {candle_duration} CHOSEN - 15 minute period candle historical data will be retrieved from {start_date} till {end_date}")
        logger.info(f"OPTION {candle_duration} CHOSEN - 15 minute period candle historical data will be retrieved from {start_date} till {end_date}")
        period = "15"
        limit = 100
    case "6":
        print(f"OPTION {candle_duration} CHOSEN - 30 minute period candle historical data will be retrieved from {start_date} till {end_date}")
        logger.info(f"OPTION {candle_duration} CHOSEN - 30 minute period candle historical data will be retrieved from {start_date} till {end_date}")
        period = "30"
        limit = 100
    case "7":
        print(f"OPTION {candle_duration} CHOSEN - 1 hour period candle historical data will be retrieved from {start_date} till {end_date}")
        logger.info(f"OPTION {candle_duration} CHOSEN - 1 hour period candle historical data will be retrieved from {start_date} till {end_date}")
        period = "60"
        limit = 100
    case "8":
        print(f"OPTION {candle_duration} CHOSEN - 1 day period candle historical data will be retrieved from {start_date} till {end_date}")
        logger.info(f"OPTION {candle_duration} CHOSEN - 1 day period candle historical data will be retrieved from {start_date} till {end_date}")
        period = "1D"
        limit = 366


def create_csv_file(file_name):
    # Get the current working directory
    current_directory = os.getcwd()
    # Define the stock_data directory path
    stock_data_directory = os.path.join(current_directory, 'stock_data')
    # Create the stock_data directory if it doesn't exist
    if not os.path.exists(stock_data_directory):
        os.makedirs(stock_data_directory)
    # Construct the full path of the CSV file
    csv_file_path = os.path.join(stock_data_directory, f'{file_name}.csv')
    # Overwrite the CSV file if it already exists
    csv_headers = ['DATE', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME']
    with open(csv_file_path, 'w', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)
        # Write headers
        csv_writer.writerow(csv_headers)

    print(f"The new file {file_name}.csv has been created.")
    logger.info(f"The new file {file_name}.csv has been created.")



def generate_date_ranges(start_date, end_date, max_days):
    date_ranges = []
    current_start_date = datetime.strptime(start_date, "%Y-%m-%d")
    current_end_date = min(current_start_date + timedelta(days=max_days - 1),
                           datetime.strptime(end_date, "%Y-%m-%d"))
    while current_start_date <= datetime.strptime(end_date, "%Y-%m-%d"):
        date_ranges.append((current_start_date.strftime("%Y-%m-%d"), current_end_date.strftime("%Y-%m-%d")))

        current_start_date = current_end_date + timedelta(days=1)
        current_end_date = min(current_start_date + timedelta(days=max_days - 1),
                               datetime.strptime(end_date, "%Y-%m-%d"))
    print(f"DATE RANGE AS PER LIMIT {max_days} DAYS OF FYERS API ARE:")
    logger.info(f"DATE RANGE AS PER LIMIT {max_days} DAYS OF FYERS API ARE:")
    for idx, date_range in enumerate(date_ranges, start=1):
        print(f"Range {idx}: {date_range[0]} to {date_range[1]}")
        logger.info(f"Range {idx}: {date_range[0]} to {date_range[1]}")
    return date_ranges

def fetch_scrip_data(token, client_id, exchange, symbol, series, period, start_date, end_date, csv_filename):
    fyers = fyersModel.FyersModel(client_id=client_id, is_async=False, token=token, log_path="")
    sym = f"{exchange}:{symbol}-{series}"
    data = {
        "symbol": sym,
        "resolution": period,
        "date_format": "1",
        "range_from": start_date,
        "range_to": end_date,
        "cont_flag": "1"
    }
    response = fyers.history(data=data)
    # print(response)
    if response["s"] == 'ok':
        # Get the current working directory
        current_directory = os.getcwd()
        # Define the stock_data directory path
        stock_data_directory = os.path.join(current_directory, 'stock_data')
        # Create the stock_data directory if it doesn't exist
        if not os.path.exists(stock_data_directory):
            os.makedirs(stock_data_directory)
        # Construct the full path of the CSV file
        csv_file_path = os.path.join(stock_data_directory, f'{csv_filename}.csv')
        candles = response["candles"]
        with open(csv_file_path, 'a', newline='') as csv_file:
            csv_writer = csv.writer(csv_file)
            # Write candle data
            for candle in candles:
                csv_writer.writerow(candle)
        #print(f"The file {csv_filename}.csv has been updated and records written - {len(candles)} records.")
        logger.info(f"FILE {csv_filename}.csv UPDATED WITH {len(candles)} RECORDS")
        return len(candles)
    else:
        #print(response)
        logger.error(f"NO DATA FOR THE TIME RANGE")
        return 0

def process_nse200(logger, token, client_id, start_date, end_date, limit, period, conn_params):
    logger.info(f"PROCESSING ALL NSE 200 STOCKS FOR HISTORICAL {period} PERIOD CANDLE DATA FROM {start_date} TILL {end_date}")
    print(f"PROCESSING ALL NSE 200 STOCKS FOR HISTORICAL {period} PERIOD CANDLE DATA FROM {start_date} TILL {end_date}")
    date_ranges = generate_date_ranges(start_date, end_date, limit)
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    query = "SELECT company_name, industry, symbol, series FROM nse200_stock_data"
    cursor.execute(query)
    records = cursor.fetchall()
    exchange = "NSE"
    series = "EQ"
    for record in records:
        count = 0
        company_name, industry, symbol, series = record
        print(f"Company Name: {company_name}, Industry: {industry}, Symbol: {symbol}, Series: {series}")
        logger.info(f"Company Name: {company_name}, Industry: {industry}, Symbol: {symbol}, Series: {series}")
        create_csv_file(f"{exchange}_{symbol}_{series}_{period}_{start_date}_{end_date}")
        for idx, date_range in enumerate(date_ranges, start=1):
            #print(f"Range {idx}: {date_range[0]} to {date_range[1]}")
            logger.info(f"FETCHING STOCK DATA FOR Range {idx}: {date_range[0]} to {date_range[1]}")
            done = fetch_scrip_data(token, client_id, exchange, symbol, series, period, date_range[0], date_range[1],
                                    f"{exchange}_{symbol}_{series}_{period}_{start_date}_{end_date}")
            count = count + done
        print(f"{count} RECORDS WRITTEN TO FILE - {exchange}_{symbol}_{series}_{period}_{start_date}_{end_date}.csv")
        logger.info(
            f"{count} RECORDS WRITTEN TO FILE - {exchange}_{symbol}_{series}_{period}_{start_date}_{end_date}.csv")
    cursor.close()
    conn.close()

process_nse200(logger,token,client_id,start_date,end_date,limit,period,db_params)
print("EXECUTION END: SUCCESS")
logger.info("EXECUTION END: SUCCESS")
'''
****************************************************************************************
'''
# Close the logger explicitly when you're done
logging.shutdown()