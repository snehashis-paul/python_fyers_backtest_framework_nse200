import os
import glob
from datetime import datetime

def delete_old_log_files(directory_path, num_to_retain):
    # Change the current working directory to the script's directory
    os.chdir(directory_path)

    # List all .log files in the logs directory
    log_files = glob.glob('logs/*.log')

    # Sort the log files based on modification time (oldest to newest)
    log_files.sort(key=os.path.getmtime)

    # Calculate the number of files to delete
    num_to_delete = max(0, len(log_files) - num_to_retain)

    # Delete the older log files
    for i in range(num_to_delete):
        os.remove(log_files[i])
        print(f"Deleted: {log_files[i]}")

if __name__ == "__main__":
    # Get the path to the directory of the script
    script_directory = os.path.dirname(os.path.abspath(__file__))

    # Prompt the user for the number of latest .log files to retain
    num_to_retain = int(input("Enter the number of latest .log files to retain: "))

    # Delete old log files
    delete_old_log_files(script_directory, num_to_retain)
