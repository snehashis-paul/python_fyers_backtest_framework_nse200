import os
import glob
from datetime import datetime, timedelta

def delete_old_log_files_by_age(directory_path, max_age, time_unit):
    # Change the current working directory to the script's directory
    os.chdir(directory_path)

    # List all .log files in the logs directory
    log_files = glob.glob('logs/*.log')

    # Get the current date and time
    current_time = datetime.now()

    # Calculate the timedelta based on the selected time unit
    if time_unit == 'days':
        age_delta = timedelta(days=max_age)
    elif time_unit == 'hours':
        age_delta = timedelta(hours=max_age)
    elif time_unit == 'minutes':
        age_delta = timedelta(minutes=max_age)
    else:
        raise ValueError("Invalid time unit. Please choose 'days', 'hours', or 'minutes'.")

    # Delete log files older than the specified age
    for log_file in log_files:
        # Get the modification time of the log file
        file_mtime = datetime.fromtimestamp(os.path.getmtime(log_file))

        # Calculate the age of the log file
        file_age = current_time - file_mtime

        # Check if the log file is older than the specified age
        if file_age > age_delta:
            os.remove(log_file)
            print(f"Deleted: {log_file}")

if __name__ == "__main__":
    # Get the path to the directory of the script
    script_directory = os.path.dirname(os.path.abspath(__file__))

    # Prompt the user to choose the time unit
    time_unit_choice = input("Choose the time unit for age (1=Days, 2=Hours, 3=Minutes): ")
    if time_unit_choice == '1':
        time_unit = 'days'
    elif time_unit_choice == '2':
        time_unit = 'hours'
    elif time_unit_choice == '3':
        time_unit = 'minutes'
    else:
        print("Invalid choice. Exiting.")
        exit()

    # Prompt the user for the maximum age of log files to retain
    max_age = int(input(f"Enter the maximum age of log files to retain (in {time_unit}): "))

    # Delete old log files by age
    delete_old_log_files_by_age(script_directory, max_age, time_unit)
