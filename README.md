## Automated Data Processing Pipeline

### Project Overview

This project, developed in 2024, automates the ingestion and processing of Excel files from HDFS into a Hive data warehouse. It is designed to be robust, handling dynamic sheet and column names within the source files.

The code in this repository has been anonymized and its publication has been authorized by the company. All sensitive details such as server paths, file prefixes, database names, and specific business logic have been replaced with generic placeholders.

#### Technologies Used

    - Orchestration: Bash
    - Data Processing: Python, PySpark
    - Data Storage: HDFS, Hive

#### Core Functionality

The pipeline consists of two main components:

    1) Bash_file_anonymized.sh (Bash Orchestrator):

        - Sets up the environment and handles Kerberos authentication.
        - Manages logging for each execution run.
        - Automatically identifies and selects the most recent Excel file from a designated HDFS directory based on a specific              naming convention.
        - Submits the PySpark application (process_excel_data.py) with the identified file path as an argument.
        - Cleans up the source HDFS directory by removing the processed Excel file upon successful execution.

    2) process_excel_data.py (PySpark Application):

        - Initializes a SparkSession.
        - Dynamically finds the correct worksheet and column names within the Excel file, making the process resilient to minor             changes in the file structure.
        - Reads the Excel data into a Spark DataFrame.
        - Performs necessary transformations and data cleansing.
        - Overwrites an intermediate Hive table with the processed data.
        - Calculates summary statistics from the data and appends a new record to a historical logging table in Hive.
