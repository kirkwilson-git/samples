'''
Author: Kirk Wilson

This script can replicate a SQL Server table to Snowflake.

This was originally created as a backup in case Fivetran didn't end up working.

It can migrate just table structures and no data, or migrate both structures and data.

Because this is an "as-needed" script, I'm not providing too much documentation.
Note that to actually load table data, either call this script from the command line with the table to loaded as an argument,
or modify the "source_tables" list in the "migrate_table_data" function.
Example of how to call this script to load specific tables (assuming you create a batch file or something similar if multiple tables
need to be loaded):
    python sql_server_to_snowflake_copy.py <table_one_to_load>
    python sql_server_to_snowflake_copy.py <table_two_to_load>
    ...

For this to work, make sure the following conditions are met:
- Target table already exists in Snowflake  (handled by the "migrate_table_structures" function)
- SNOWFLAKE_SCHEMA and SNOWFLAKE_DATABASE variables are set correctly
- sql_server_connection is set correctly

'''

import pyodbc
import snowflake.connector
import csv
import math
import time
import sys

# All of the below libraries are only used for Snowflake authentication.
# Code is taken from the "Key Pair Authentication" section at https://docs.snowflake.net/manuals/user-guide/python-connector-example.html
import os 
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric import dsa
from cryptography.hazmat.primitives import serialization

# Used for Snowflake login authentication.
SNOWFLAKE_PRIVATE_KEY_PATH = 'C:/Python/rsa_key.p8'

SNOWFLAKE_DATABASE = 'PROD'
SNOWFLAKE_SCHEMA = 'PR_DS_DBO'
SNOWFLAKE_WAREHOUSE = 'DEV_WH'


###################################
# Establish SQL Server connection #
###################################


##sql_server_connection = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
##                      "Server=TB-LAWDB-P02;"
##                      "Database=lawproddb;"
##                      "Trusted_Connection=yes;")

sql_server_connection = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                      "Server=DL-BIWSQL-P201;"
                      "Database=PR_DS;"
                      "UID=svc_FiveTranProd;PWD=xxxxxx")        


sql_cursor = sql_server_connection.cursor()



##################################
# Establish Snowflake connection #
##################################
# Establish the Snowflake connection first so it can be referenced anywhere easily.
# Key pair authentication is used rather than directly storing the SCRIPT_USER user's password in this script for extra security.
# Currently the private key passphrase is stored directly below, but it only works if the user also has the private key itself which is in an external file.
# The private key passphrase can be easily externalized from this script as well for an additional layer of security if needed.
# Code is taken from the "Key Pair Authentication" section at https://docs.snowflake.net/manuals/user-guide/python-connector-example.html
with open(SNOWFLAKE_PRIVATE_KEY_PATH, 'rb') as key:
    p_key = serialization.load_pem_private_key(
        key.read(),
        password='xxxxxxxx'.encode(),
        backend=default_backend()
    )

pkb = p_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption())

snowflake_connection = snowflake.connector.connect(
    user='SCRIPT_USER',
    account='xxxxxx',
    private_key=pkb)

snowflake_cursor = snowflake_connection.cursor()

#############
# Functions #
#############
NUMBER_TYPES = ['smallint', 'tinyint', 'decimal', 'float', 'int', 'money', 'numeric']
DATETIME_TYPES = ['datetime', 'datetime2', 'smalldatetime']
# Convert SQL Server to Snowflake data types
def get_converted_datatypes(data_type, data_scale):  
    if data_type in NUMBER_TYPES:
        if data_scale is None:
            data_scale = '0'
        elif data_scale == 'None':
            data_scale = '0'
            
        return 'NUMBER(38, ' + str(data_scale) + ')'
    elif data_type in DATETIME_TYPES:
        return 'DATETIME'
    else:
        return 'VARCHAR'

def create_table_in_snowflake(table_ddl, table_name):
    table_ddl = table_ddl[:-2] + ')' # Strip off the last ', ' characters

    # Execute DDL in Snowflake
    print('Creating table: ' + table_name)
    #print(table_ddl)

    try:
        snowflake_cursor.execute(table_ddl)
    except Exception as e:
            print(e)
            #sys.exit()

def migrate_table_structures():
    # NOTE: Comment out the T.TABLE_NAME IN ... line to make this script generic.
    query = """
            SELECT T.TABLE_NAME, C.COLUMN_NAME, C.DATA_TYPE, C.NUMERIC_SCALE
            FROM INFORMATION_SCHEMA.TABLES T, INFORMATION_SCHEMA.COLUMNS C
            WHERE 
            C.TABLE_CATALOG = T.TABLE_CATALOG
            AND C.TABLE_SCHEMA = T.TABLE_SCHEMA
            AND C.TABLE_NAME = T.TABLE_NAME
            AND T.TABLE_TYPE = 'BASE TABLE'
            AND C.DATA_TYPE NOT IN ('varbinary', 'image', 'binary')
            --AND T.TABLE_NAME IN ('DM_LineOfBusinessFinance')  
            ORDER BY T.TABLE_NAME, C.ORDINAL_POSITION
        """

    current_table = ''
    table_ddl = ''

    for record in sql_cursor.execute(query):
        if current_table != record[0]:
            # Moving on to a new table.  Finish creation of the previous table (as long as this isn't the first loop).
            if table_ddl != '':
                create_table_in_snowflake(table_ddl, current_table)

            # Start creation of new table
            current_table = record[0]
            if str(record[0]).find(' ') != -1:
                table_ddl = 'CREATE OR REPLACE TABLE "' + str(record[0]) + '"('
            else:
                table_ddl = 'CREATE OR REPLACE TABLE ' + str(record[0]) + '('

        # Add a line for each column
        if str(record[1]).find(' ') != -1:
             column_name = '"' + str(record[1]) + '"'
        else:
            column_name = str(record[1])
            
        table_ddl += column_name + ' ' + get_converted_datatypes(str(record[2]), str(record[3])) + ', '

    # Finish the last table since it won't be processed in the above loop
    create_table_in_snowflake(table_ddl, current_table)

def get_column_list(table_name):
    # First get the primary key(s) of the table
    query = """SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + QUOTENAME(CONSTRAINT_NAME)), 'IsPrimaryKey') = 1
                AND TABLE_NAME = '""" + table_name + """'"""

    primary_key = ''
    for record in sql_cursor.execute(query):
        primary_key += record[0] + ','

    primary_key = primary_key[:-1] # Strip off the last comma

    # Now get the column list
    query = """SELECT COLUMN_NAME 
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = '""" + table_name + """'
                AND DATA_TYPE NOT IN ('varbinary', 'image', 'binary')
                ORDER BY ORDINAL_POSITION"""
    
    columns = ''
    for record in sql_cursor.execute(query):
        columns += record[0] + ','

    columns = columns[:-1] # Strip off the last comma

    if primary_key == '':
        print('WARNING: Table ' + table_name + ' does not have primary key defined')
        return 'SELECT ' + columns + ' FROM '

    # Can safely use 'SELECT *..." since the table columns were created in the same COLUMN_ID order, and this is a one-time operation.
    # The query is formatted in this way so that a specific subset of records can be returned as needed.
    query = 'SELECT ' + columns + ' FROM (SELECT *, ROW_NUMBER() OVER (ORDER BY ' + primary_key  + ') RANKING FROM ' + table_name + ') x WHERE RANKING BETWEEN '
    return(query)

# Note that data is queried from SQL Server and uploaded to Snowflake in chunks for performance and restartability reasons.
# For each chunk/loop, the very high-level process flow is:
    # Select table data from source
    # Write to CSV 
    # Stage to Snowflake
def migrate_table_data():

    # If this script is called with an input argument, use that as the table list.  
    source_tables = []

    # If no input parameters are passed in, only process these source tables
    if len(sys.argv) == 1:
        source_tables = ['DM_LineOfBusinessFinance']
    else:
        print(sys.argv[1])
        source_tables = [sys.argv[1]]

    # The number of records that will be returned from SQL Server and uploaded to Snowflake during each iteration
    batch_size = 50000

    for table in source_tables:
        print('Gathering table data for ' + table)

        base_query = get_column_list(table)
        
        for data_record in sql_cursor.execute('SELECT COUNT(*) FROM ' + table):
            table_count = data_record[0]

        current_chunk = 1

        if 'ROW_NUMBER() OVER' not in base_query: # If this condition is TRUE, the current table does not have a PK and so the entire table will be loaded in one chunk.
            total_chunks = 1
        else:
            total_chunks = math.ceil(table_count / batch_size)

        start = 1
        finish = batch_size

        start_time = time.time()
        while current_chunk <= total_chunks:
            # Spool some logging information
            print('Current chunk: ' + str(current_chunk) + ' (' + str((current_chunk / total_chunks) * 100) + '%) - ' + str((time.time() - start_time) / 60) + ' minutes')

            # Create a new CSV file, named the same as the source table
            csv_file  = open(table + '.csv', 'w', newline='')
            csv_writer = csv.writer(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
        
            if 'ROW_NUMBER() OVER' not in base_query:
                query = base_query + table  # This is effectively "select * from <table>"
            else:
                query = base_query + str(start) + ' and ' + str(finish)

            # Loop through each data record, writing the record to the CSV file
            for data_record in sql_cursor.execute(query):
                csv_writer.writerow(data_record)

            csv_file.close()

            # Bulk upload the data into Snowflake
            snowflake_cursor.execute("PUT 'file://" + os.getcwd().replace('\\', '/') + "/" + table + ".csv' @%" + table)
            snowflake_cursor.execute("COPY INTO " + table + " FILE_FORMAT='ARCHIVE.PUBLIC.CSV_NO_HEADERS' PURGE=TRUE")            
            
            start += batch_size
            finish += batch_size
            current_chunk += 1
          

##############################
# Start of main script logic #
##############################
try:
    snowflake_cursor.execute('USE DATABASE ' + SNOWFLAKE_DATABASE)
    snowflake_cursor.execute('USE SCHEMA ' + SNOWFLAKE_SCHEMA)
    snowflake_cursor.execute('USE WAREHOUSE ' + SNOWFLAKE_WAREHOUSE)
    
    migrate_table_structures()
    migrate_table_data()

finally:
    snowflake_cursor.close()
    sql_cursor.close()
