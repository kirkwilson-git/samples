'''
Author: Kirk Wilson

Given a source directory (SOURCE_FILE_PATH), this script will write all files within that directory to a simple text file
and upload the contents to Snowflake.

Note that if the number of files is large, do not try to run this against a network drive because performance will be poor.

This script has largely been used to make the files provided by SaaS vendors easily query-able in Snowflake to confirm the
appropriate relationships exist between tabular data and associated files.
'''

from os import listdir
import snowflake.connector
import os 
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric import dsa
from cryptography.hazmat.primitives import serialization

SOURCE_FILE_PATH = 'W:/Basware_docs' # Directory to get contents of
SYSTEM_NAME = 'BASWARE' # Table in Snowflake will be named <SYSTEM_NAME>_FILE_LIST

# Schema and database where the table will be created.
SNOWFLAKE_SCHEMA = 'BASWARE'
SNOWFLAKE_DATABASE = 'ARCHIVE'

SNOWFLAKE_WAREHOUSE = 'DEV_ELT_WH'

# Create the text file, overwriting it if it already exists.
f = open(SYSTEM_NAME + "_FILES.txt", "w")

# Loop through each file in the directory and write the file name to the text file.
for file in listdir(SOURCE_FILE_PATH):
    try:
        f.write(file + '\n')
    except:
        print(file)

f.close()

print(SYSTEM_NAME + '_FILES.txt created')

# Start upload to Snowflake

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
        password='xxxxxxx'.encode(), 
        backend=default_backend()
    )

pkb = p_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption())

snowflake_connection = snowflake.connector.connect(
    user='SCRIPT_USER',
    account='xxxxx',
    private_key=pkb)

snowflake_cursor = snowflake_connection.cursor()

try:
    snowflake_cursor.execute('USE DATABASE ' + SNOWFLAKE_DATABASE)
    snowflake_cursor.execute('USE SCHEMA ' + SNOWFLAKE_SCHEMA)
    snowflake_cursor.execute('USE WAREHOUSE ' + SNOWFLAKE_WAREHOUSE)

    snowflake_cursor.execute('CREATE OR REPLACE TABLE ' + SYSTEM_NAME + '_FILE_LIST (FILE_NAME VARCHAR)')
    
    snowflake_cursor.execute('PUT file://' + SYSTEM_NAME + '_FILES.txt @%' + SYSTEM_NAME + '_FILE_LIST')
    snowflake_cursor.execute("COPY INTO " + SYSTEM_NAME + "_FILE_LIST FILE_FORMAT='PUBLIC.FILE_LIST_NO_HEADER' PURGE=TRUE")

    print(SYSTEM_NAME + '_FILE_LIST table created and populated')

finally:
    snowflake_cursor.close()



