#!/bin/bash

set -e
set -u
set -o pipefail

timestamp=$(date +%Y-%m-%d_%H-%M-%S)
 
# Script paths
SCRIPT_DIR="$(dirname "$(readlink -f "$0")")"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$SCRIPT_DIR"
 
# Log files
log_filename=$(basename "$0" | cut -f 1 -d .)
logfile="${PROJECT_DIR}/logs/${log_filename}_${timestamp}.log"
mkdir -p "${PROJECT_DIR}/logs/" && touch "${logfile}"
find "${PROJECT_DIR}/logs/" -type f -mtime +92 -delete

exec > >(tee -a "${logfile}")
exec 2>&1
 
# Kerberos authentication
user_server=$USER
user=$(echo "$user_server" | cut -d@ -f1)
echo "User: $user"
kinit -kt "/home/${user}/${user}.keytab" "${user}"

# Environment detection
if [[ "$PROJECT_DIR" =~ "/prod/" ]]; then
    export environment='prod'
    HDFS_EXCEL_DIR="/path/to/prod/data/"
else
    export environment='dev'
    HDFS_EXCEL_DIR="/path/to/dev/data/"
fi
echo "Environment: $environment"
 
echo "Input HDFS Directory: $HDFS_EXCEL_DIR"
echo "Python Script Directory: $SCRIPT_DIR"

# Find the newest Excel File in HDFS
echo "Searching for .xlsx files in HDFS directory: $HDFS_EXCEL_DIR"
echo "Criteria: Filename must start with 'file_prefix_' and contain no spaces."
mapfile -t hdfs_excel_paths < <(hdfs dfs -ls -t "$HDFS_EXCEL_DIR" | \
                                  awk '{print $NF}' | \
                                  grep -E '\/file_prefix_[^ ]+\.xlsx$')

num_files=${#hdfs_excel_paths[@]}
echo "Found $num_files .xlsx file(s) matching the criteria."

if [ "$num_files" -eq 0 ]; then
    echo "No .xlsx files matching the criteria were found in HDFS directory: $HDFS_EXCEL_DIR"
    exit 0
fi

# Select the newest file
excel_file_to_process="${hdfs_excel_paths[0]}"
echo "Selected the newest Excel file found: $excel_file_to_process"
echo "----------------------------------------"

# Execute the Spark command
spark-submit \
    "${SCRIPT_DIR}/process_excel_data.py" \
    "${excel_file_to_process}"

# Check the return code of spark-submit
if [ $? -ne 0 ]; then
    echo "FATAL: spark-submit failed. See error messages above." >&2
    exit 1
fi

echo "spark-submit finished successfully." 

# Cleanup
echo "Cleaning up source Excel files from $HDFS_EXCEL_DIR..."
hdfs dfs -rm "$HDFS_EXCEL_DIR"/*.xlsx
echo "completion_status = 1"
echo "Script finished."
