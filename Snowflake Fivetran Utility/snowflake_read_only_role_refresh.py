'''
Author: Kirk Wilson

Loop through all databases/schemas in Snowflake and grant read-only access to the role READ_ONLY_REPORTING_ROLE. 

NOTE: This was created before I knew about the FUTURE option for grants in Snowflake, which renders this basically useless...   :-)
https://docs.snowflake.net/manuals/sql-reference/sql/grant-privilege.html
'''

import snowflake.connector
import sys # Used only for getting input parameters.
# All of the below libraries are only used for Snowflake authentication.
# Code is taken from the "Key Pair Authentication" section at https://docs.snowflake.net/manuals/user-guide/python-connector-example.html
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric import dsa
from cryptography.hazmat.primitives import serialization

###########################
# Define global variables #
###########################
# Note that any variables in all caps are intended to be global static variables (there's no way to enforce a static variable in Python though)
SNOWFLAKE_WAREHOUSE = 'DEV_WH'

# Used for Snowflake login authentication.
SNOWFLAKE_PRIVATE_KEY_PATH = 'C:/Python/rsa_key.p8'

READ_ONLY_ROLE = 'READ_ONLY_REPORTING_ROLE'


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
        password='xxxxxx'.encode(),
        backend=default_backend()
    )

pkb = p_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption())

snowflake_connection = snowflake.connector.connect(
    user='SCRIPT_USER',
    account='xxxxxxxx',
    private_key=pkb)

cursor = snowflake_connection.cursor()


#############
# Functions #
#############
# This function is used solely to execute Snowflake statements.
# It is a separate function in case logging or console output is desired.
def cursor_execute(statement):
    print(statement)
    cursor.execute(statement)


########################
# Start of main script #
########################
# This is one case where the code is truly self-documenting, since it's just executing Snowflake commands sequentially.
# Overview of actions:
#   Create security role and grant it appropriate read-only privileges on only the Fivetran schema being replicated.
#   Create a new database and schema that will contain only the objects from the Fivetran schema being replicated.
#   Call a function that will create views in the new schema that point to the Fivetran schema being replicated.  This is purely for convenience.
try:
    cursor_execute('USE ROLE SECURITYADMIN')
    cursor_execute('USE WAREHOUSE ' + SNOWFLAKE_WAREHOUSE)
    
    result_set = cursor.execute('SHOW SCHEMAS').fetchall()

    for record in result_set:
        database = record[4]
        schema = record[1]

        if database not in ('FIVETRAN', 'SNOWFLAKE', 'SNOWFLAKE_SAMPLE_DATA') and schema != 'INFORMATION_SCHEMA':
            cursor_execute('GRANT USAGE ON DATABASE ' + database + ' TO ROLE ' + READ_ONLY_ROLE) # Executing this in each loop is redundant if a database has mutliple schemas, but it keeps this code a bit simpler.
            cursor_execute('GRANT USAGE ON SCHEMA ' + database + '.' + schema + ' TO ROLE ' + READ_ONLY_ROLE)
            cursor_execute('GRANT SELECT ON ALL TABLES IN SCHEMA ' + database + '.' + schema + ' TO ROLE ' + READ_ONLY_ROLE)
            cursor_execute('GRANT SELECT ON ALL VIEWS IN SCHEMA ' + database + '.' + schema + ' TO ROLE ' + READ_ONLY_ROLE)

finally:
    cursor.close()
