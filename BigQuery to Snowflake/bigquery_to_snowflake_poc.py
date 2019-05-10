'''
Author: Kirk Wilson

This is a POC demonstrating process of extracting data from Google BigQuery into Snowflake.  
Some shortcuts are taken, but this was used primarily to demonstrate that transferring Google Analytics
data into Snowflake can be executed very quickly and cheaply.  

Prior to this, the client was utilizing row-by-row processing to load the data, which was extremely slow and expensive.
They wanted to leverage Snowflake's more flexibile architecture compared to BigQuery, but they weren't 
properly educated in best practices for data loading/unloading.
'''

from google.cloud import bigquery
from google.cloud import storage
from google.oauth2 import service_account
import snowflake.connector
from datetime import datetime

def print_log(table_name, message):
    print(table_name + ':  ' + datetime.now().strftime('%m-%d-%Y %H:%M:%S') + '   ' + message)

LOCAL_DIRECTORY = 'C:/temp/'
GOOGLE_PROJECT_ID = 'xxxxxx'
GOOGLE_DATASET_ID = 'xxxxxx'
GOOGLE_STORAGE_BUCKET_NAME = 'xxxxxxx'

# Google Clooud service account that will connect to BigQuery and Storage
credentials = service_account.Credentials.from_service_account_file(LOCAL_DIRECTORY + 'xxxxxx.json')

# Connect to BigQuery using service account
bigquery_client = bigquery.Client(credentials=credentials, project=GOOGLE_PROJECT_ID)

# Connect to Google Cloud Storage and get a bucket reference
storage_client = storage.Client(credentials=credentials, project=GOOGLE_PROJECT_ID)
bucket = storage_client.get_bucket(GOOGLE_STORAGE_BUCKET_NAME)

# This is the quick-and-dirty way to connect to Snowflake.
# A better/safer option is documented in the "Key Pair Authentication" section at https://docs.snowflake.net/manuals/user-guide/python-connector-example.html
snowflake_connection = snowflake.connector.connect(
    user='xxxx',
    password='xxxx',
    account='xxxx' 
)

snowflake_cursor = snowflake_connection.cursor()

try:
    # Initialize Snowflake session
    snowflake_cursor.execute('USE WAREHOUSE POC_WH')
    snowflake_cursor.execute('USE DATABASE POC')
    snowflake_cursor.execute('USE SCHEMA PUBLIC')

    # Get all of the BigQuery tables in the current dataset
    dataset = bigquery_client.dataset(GOOGLE_DATASET_ID, project=GOOGLE_PROJECT_ID) 
    tables = list(bigquery_client.list_tables(dataset))

    # Process flow:
    # Loop through all BigQuery tables
    #   Export BigQuery table to CSV in Google Cloud Storage
    #   Download CSV to local PC
    #   Upload CSV to Snowflake
    # NOTE: For simplicity, this assumes all of the BigQuery tables have been already created in Snowflake with the same names and columns
    for table in tables:      
        # Just to improve readability a bit
        table_name = table.table_id 
        csv_file_name = table_name + '.csv'
        
        print_log(table_name, 'START')

        # Get count of table in BigQuery just for validation to ensure Snowflake count matches
        bigquery_count_result = bigquery_client.query('SELECT COUNT(*) FROM `' + GOOGLE_PROJECT_ID + '.' + GOOGLE_DATASET_ID + '.' + table_name + '`').result()
        for record in bigquery_count_result:
            print_log(table_name, 'BigQuery Table Count: ' + str(record[0]))

        # Export BigQuery table data to the root directory in a Google Cloud Storage bucket as CSV, with the file name the same as the table name
        destination_uri = 'gs://{}/{}'.format(GOOGLE_STORAGE_BUCKET_NAME, csv_file_name) 
        extract_job = bigquery_client.extract_table(table, destination_uri, location='US')  
        extract_job.result()  # Wait for job to complete before proceeding.

        print_log(table_name, 'Exported BigQuery table to Google Storage bucket')

        # Download CSV from Google bucket to local PC
        blob = bucket.blob(csv_file_name) # Google bucket location
        blob.download_to_filename(LOCAL_DIRECTORY + csv_file_name) # Local location where the file will be saved

        print_log(table_name, 'Downloaded CSV from Google bucket to local PC') 

        # Ensure Snowflake target table is empty (just for this POC)
        snowflake_cursor.execute('TRUNCATE TABLE ' + table_name)
        print_log(table_name, 'Snowflake table count before upload: ' + str(snowflake_cursor.execute('SELECT COUNT(*) FROM ' + table_name).fetchone()[0]))

        # Upload CSV to internal Snowflake stage and copy data into the target table
        snowflake_cursor.execute("PUT 'file://" + LOCAL_DIRECTORY + csv_file_name + "' @%" + table_name)
        snowflake_cursor.execute("COPY INTO " + table_name + " FILE_FORMAT='POC.PUBLIC.CSV' ON_ERROR=ABORT_STATEMENT PURGE=TRUE")

        print_log(table_name, 'Snowflake table count after upload: ' + str(snowflake_cursor.execute('SELECT COUNT(*) FROM ' + table_name).fetchone()[0]))

        print_log(table_name, 'FINISH')
        print('')
finally:
    snowflake_cursor.close()
    
