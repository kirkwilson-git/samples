'''
Author: Kirk Wilson

This was created for internal use to validate and log table counts between a SQL Server database (the source in this scenario) and 
a Snowflake database (the target).  

Supporting Snowflake objects that drive this process:
-- This table must be manually populated.
create or replace TABLE RECORD_COUNT_SOURCES (
	SOURCE_KEY NUMBER(38,0),
	SOURCE_NAME VARCHAR(16777216),
	SOURCE_TYPE VARCHAR(16777216),
	SOURCE_SERVER VARCHAR(16777216),
	SOURCE_DATABASE VARCHAR(16777216),
	SOURCE_REPLICATED_BY VARCHAR(16777216),
	SNOWFLAKE_DATABASE VARCHAR(16777216),
	SNOWFLAKE_SCHEMA VARCHAR(16777216),
	AUDIT_ENABLED_FLAG VARCHAR(16777216),
	MD_INSERT_DATE TIMESTAMP_NTZ(9),
	SOURCE_FILE_NAME VARCHAR(16777216),
	SOURCE_SCHEMA VARCHAR(16777216)
);

-- This table will be populated automatically by this Python script.
create or replace TABLE RECORD_COUNT_RESULTS (
	SOURCE_KEY NUMBER(38,0),
	TABLE_NAME VARCHAR(16777216),
	SOURCE_COUNT NUMBER(38,0),
	SOURCE_DATE TIMESTAMP_NTZ(9),
	TARGET_COUNT NUMBER(38,0),
	TARGET_DATE TIMESTAMP_NTZ(9),
	MD_ACTIVE_FLAG VARCHAR(16777216)
);
'''

import snowflake.connector
import pyodbc # SQL Server ODBC library
import csv
# All of the below libraries are only used for Snowflake authentication.
# Code is taken from the "Key Pair Authentication" section at https://docs.snowflake.net/manuals/user-guide/python-connector-example.html
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric import dsa
from cryptography.hazmat.primitives import serialization

SNOWFLAKE_AUDIT_DATABASE = 'AUDIT'
SNOWFLAKE_AUDIT_SCHEMA = 'PUBLIC'
SNOWFLAKE_WAREHOUSE = 'DEV_WH'

# Used for Snowflake login authentication.
SNOWFLAKE_PRIVATE_KEY_PATH = 'C:/Python/rsa_key.p8'

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
        password='xxxxx'.encode(),
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


# This function will gather all table counts from both SQL Server and Snowflake for the parameterized connections.
# The process flow is:
#   Establish SQL Server connection
#   Gather source table list from SQL Server
#   Create a CSV file that will store all query results for eventual upload to the Snowflake table AUDIT.PUBLIC.RECORD_COUNT_RESULTS
#   Loop through each SQL Server table and get its record count
#       If the SQL Server table has a non-zero record count, get the record count from the corresponding table in Snowflake
#   Upload the results (contained within the CSV file) to Snowflake
def gather_sql_server_counts(source_key, sql_server, sql_database, sql_schema, snowflake_database, snowflake_schema):
    try:
        # Establish SQL Server connection
        # Use this for Windows authentication.
##        sql_cursor = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
##                      "Server=" + sql_server + ";"
##                      "Database=" + sql_database + ";"
##                      "Trusted_Connection=yes;")

        # Use this for standard username/password connection
        sql_cursor = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                      "Server=" + sql_server + ";"
                      "Database=" + sql_database + ";"
                      "UID=svc_FiveTranProd;PWD=xxxxxxxx")        

        # Get list of tables to query in SQL Server database
        table_list = sql_cursor.execute("""SELECT T.TABLE_NAME
                    FROM INFORMATION_SCHEMA.TABLES T
                    WHERE T.TABLE_TYPE = 'BASE TABLE'
                    AND T.TABLE_SCHEMA = '""" + sql_schema + """' 
                    ORDER BY T.TABLE_NAME""").fetchall()

        # Re-use existing Snowflake cursor, just changing the current database/schema as needed.
        snowflake_cursor.execute('USE DATABASE ' + snowflake_database)
        snowflake_cursor.execute('USE SCHEMA ' + snowflake_schema)

        # Simple checker to make sure that the below loop actually processes at least one table with a non-zero count.
        # This boolean is then checked after the loop before uploading the CSV file to Snowflake to avoid uploading an empty or old file.
        processed_records = False;

        # Create the CSV file.
        # Column order within the file: 'SOURCE_KEY','TABLE_NAME','SOURCE_COUNT','SOURCE_DATE','TARGET_COUNT','TARGET_DATE', 'MD_ACTIVE_FLAG'
        with open('record_count_results_temp.csv', mode='w') as csv_file:
            csv_writer = csv.writer(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            
            # Loop through each table and get its record count
            for record in table_list:
                sql_table_count = sql_cursor.execute('SELECT COUNT(*), CURRENT_TIMESTAMP FROM ' + record[0]).fetchall()

                # Only track tables that have non-zero record counts in the source.  
                if sql_table_count[0][0] != 0:
                    processed_records = True
                    print('Processing ' + record[0])

                    # Put this in exception handler block in case the table doesn't exist in Snowflake for some reason.
                    try:
                        snowflake_table_count = snowflake_cursor.execute('SELECT COUNT(*), CURRENT_TIMESTAMP::TIMESTAMP_NTZ FROM ' + record[0]).fetchall()
                    except:
                        snowflake_table_count = [(-1, '')]

                    # Write the SQL Server and Snowflake counts to the CSV file for later upload to Snowflake.
                    csv_writer.writerow([source_key, record[0], str(sql_table_count[0][0]), str(sql_table_count[0][1]), str(snowflake_table_count[0][0]), str(snowflake_table_count[0][1]), 'Y'])

        # Only upload to Snowflake if there was at least one table in the source database with a non-zero record count.
        if processed_records:
            print('Uploading results to Snowflake')
            
            snowflake_cursor.execute('USE DATABASE ' + SNOWFLAKE_AUDIT_DATABASE)
            snowflake_cursor.execute('USE SCHEMA ' + SNOWFLAKE_AUDIT_SCHEMA)

            # Mark any existing records in the table as inactive since the data set that is uploaded in the next statement will be the new active data set.
            snowflake_cursor.execute("UPDATE RECORD_COUNT_RESULTS SET MD_ACTIVE_FLAG = 'N' WHERE SOURCE_KEY = " + str(source_key))
            
            # The PUT command will automatically compress and upload the file to an internal Snowflake stage within AWS/Azure.
            # The COPY INTO command will extract the contents of the staged file into the destination table.
            snowflake_cursor.execute("PUT 'file://record_count_results_temp.csv' @%RECORD_COUNT_RESULTS")
            snowflake_cursor.execute("COPY INTO RECORD_COUNT_RESULTS PURGE=TRUE") # PURGE deletes the file from the Snowflake stage after it's loaded.
    finally:
        sql_cursor.close()


##############################
# Start of main script logic #
##############################
try:
    snowflake_cursor.execute('USE DATABASE ' + SNOWFLAKE_AUDIT_DATABASE)
    snowflake_cursor.execute('USE SCHEMA ' + SNOWFLAKE_AUDIT_SCHEMA)
    snowflake_cursor.execute('USE WAREHOUSE ' + SNOWFLAKE_WAREHOUSE)

    for record in snowflake_cursor.execute("""SELECT SOURCE_KEY, SOURCE_SERVER, SOURCE_DATABASE, SOURCE_SCHEMA, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA
                                            FROM RECORD_COUNT_SOURCES
                                            WHERE AUDIT_ENABLED_FLAG = 'Y'
                                            AND SOURCE_TYPE = 'SQL SERVER'""").fetchall():
        gather_sql_server_counts(record[0], record[1], record[2], record[3], record[4], record[5])
        

finally:
    snowflake_cursor.close()

