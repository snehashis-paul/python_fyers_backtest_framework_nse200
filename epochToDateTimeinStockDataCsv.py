import os
import pandas as pd
from datetime import datetime

# Directory path where CSV files are located
directory_path = os.path.join(os.path.dirname(__file__), "stock_data")


# Function to convert epoch timestamp to date and time format
def convert_epoch_to_date(epoch_time):
    return datetime.utcfromtimestamp(epoch_time).strftime('%Y-%m-%d %H:%M:%S')


# Find all CSV files in the directory
csv_files = [file for file in os.listdir(directory_path) if file.endswith('.csv')]

# Process each CSV file
for csv_file in csv_files:
    file_path = os.path.join(directory_path, csv_file)

    # Read the CSV file
    df = pd.read_csv(file_path)

    # Assuming the first column is named 'DATE' and contains epoch timestamps
    df['CONV_DATE'] = df['DATE'].apply(convert_epoch_to_date)

    # Save the updated DataFrame back to the CSV file
    df.to_csv(file_path, index=False)

    print(f"Converted dates and updated CSV file: {csv_file}")
