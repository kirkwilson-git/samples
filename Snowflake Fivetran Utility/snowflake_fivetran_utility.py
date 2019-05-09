'''
Author: Kirk Wilson

This script helps streamline securing a Snowflake environment that contains data replicated by Fivetran.  There is no harm in running this script
multiple times in a row since the Snowflake objects will only be created if they do not already exist or in the case of views will be simply recreated.

Fivetran will replicate all data sources into a single Snowflake database, with the different sources each having their own schema.
For obvious security reasons, a typical user should not have full access to the Snowflake Fivetran database since, depending on the sources
being replicated, it could contain a wide variety of sensitive information.

This script automates the process of creating a new Snowflake schema within a separate database (such as DEV or PROD) and replicating the Fivetran-replicated
tables into the new schema as read-only views.

The Fivetran-replicated data sets are further replicated via this script from their own schema within the FIVETRAN database into their own schema in DEV/PROD as 
“SELECT <all_columns> FROM FIVETRAN.<schema>.<table> WHERE _FIVETRAN_DELETED <> TRUE” views.  This is done for a few reasons:
	It ensures the data is read-only since the databases only have the “SELECT ANY TABLE/VIEW…” privilege.
	It ensures that deleted records are always filtered out (since Fivetran only performs logical deletes indicated by the _FIVETRAN_DELETED column) 
	It makes the data set much more explicit and convenient when within Snowflake and completed isolated from others.  
		It’s more intuitive to write queries to a “DEV.URP_DBO” schema for example than the “FIVETRAN.D101_SQL_UPR_DBO” schema.  Maintenance is also more intuitive.
	
	Side note – the views are not “SELECT * FROM <table>” but rather have all columns listed.  
	This is because if a view is defined as “SELECT *” and then a new column is added in the future to the source table, Snowflake will invalidate the view and will 
	throw an error when it is queried.  As a result, it is safer to only declare the specific columns and then if something is added in the future, it will not automatically break anything.

The script is largely self-documenting since it is largely just executing GRANT statements or creating views.

'''

import snowflake.connector
import sys # Used only for getting input parameters.
# All of the below libraries are only used for Snowflake authentication.
# Code is taken from the "Key Pair Authentication" section at https://docs.snowflake.net/manuals/user-guide/python-connector-example.html
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric import dsa
from cryptography.hazmat.primitives import serialization

#############################
# Validate Input Parameters #
#############################
# Before doing anything else, make sure the correct number of arguments was passed in to the script.
# Arguments are automatically stored in the list "sys.argv".
# sys.argv[0] by default is the name of the script, so since 6 arguments are expected need to make sure there are actually 7.
if len(sys.argv) != 5:
    print('Incorrect number of arguments specified.  Required arguments: ')
    print('  Fivetran schema to replicate')
    print('  Target database name')
    print('  Target schema name')
    print('  Snowflake role that requires access to this schema')
    print('\nExample: python ' + sys.argv[0] + ' D101_SQL_UPR_DBO DEV UPR_DBO PAYROLL_ROLE')
    sys.exit()

###########################
# Define global variables #
###########################
# Note that any variables in all caps are intended to be global static variables (there's no way to enforce a static variable in Python though)
# Input parameters
FIVETRAN_SCHEMA = sys.argv[1]
TARGET_DATABASE = sys.argv[2]
TARGET_SCHEMA = sys.argv[3]
TARGET_ROLE = sys.argv[4]
# /Input parameters

SNOWFLAKE_WAREHOUSE = 'DEV_WH'

FIVETRAN_DATABASE = 'FIVETRAN'

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
        password='xxxxxxxxxx'.encode(),
        backend=default_backend()
    )

pkb = p_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption())

snowflake_connection = snowflake.connector.connect(
    user='SCRIPT_USER',
    account='xxxxxxxxx',
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

# Return comma-separated string with all columns in the parameterized table.
def get_table_column_list(table_name):
    cursor.execute('USE DATABASE ' + FIVETRAN_DATABASE)

    result_set = cursor.execute("""SELECT COLUMN_NAME
                                    FROM INFORMATION_SCHEMA.COLUMNS
                                    WHERE TABLE_NAME = '""" + table_name + """'
                                    AND TABLE_CATALOG = '""" + FIVETRAN_DATABASE + """'
                                    AND TABLE_SCHEMA = '""" + FIVETRAN_SCHEMA + """'
                                    AND COLUMN_NAME NOT IN ('_FIVETRAN_DELETED')
                                    ORDER BY ORDINAL_POSITION""").fetchall()

    column_list = ''

    for record in result_set:
        column_list += record[0] + ','

    return column_list[:-1]  # Strip off the last comma
    

# Since Snowflake does not currently support synonyms like some other databases, this function
# will loop through all tables in the Fivetran schema being replicated and create views in the newly
# created database.schema that are defined as "CREATE OR REPLACE VIEW <source_table> AS SELECT <all_columns> FROM <source_table>".
# Note that the view query explicitly lists the columns instead of simply "SELECT * FROM <source_table>" because if a new column
# is added in the future to the source table, Snowflake will for some reason throw an error when trying to query the view
# instead of simply returning all columns as expected.  As a result, if a new column is added and needed, this script
# can be executed again to recreate the view with all current columns.
# Of course if a column is ever dropped from a source table, the view will be invalidated regardless and must be recompiled.
# Creating these views is done both for convenience, so that users don't have to fully qualify all queries with the
# Snowflake Fivetran database and schema names.  Also, it has a filter to automatically exclude any deleted columns, since Fivetran
# only performs logical instead of physical deletes - indicated by the boolean column _FIVETRAN_DELETED.
# NOTE: If a source table has columns modified (added or removed), then the Snowflake view needs to be re-created or else
# it will throw an error when trying to query.  This entire script can simply be executed again to do that.
def replicate_views():
    cursor.execute('USE DATABASE ' + FIVETRAN_DATABASE)
    result_set = cursor.execute("""SELECT TABLE_NAME
                                    FROM INFORMATION_SCHEMA.TABLES
                                    WHERE TABLE_CATALOG = '""" + FIVETRAN_DATABASE + """'
                                    AND TABLE_SCHEMA = '""" + FIVETRAN_SCHEMA + """'
                                    AND TABLE_NAME <> 'FIVETRAN_AUDIT'
                                    ORDER BY 1""").fetchall()

    cursor_execute('USE DATABASE ' + TARGET_DATABASE)
    cursor_execute('USE SCHEMA ' + TARGET_SCHEMA)
    
    for record in result_set:
        column_list = get_table_column_list(record[0])

        # Have to do this in each loop because the "get_table_column_list" function called above changes the Snowflake database.
        # Use "cursor.execute" instead of "cursor_execute" function so this doesn't get needlessly logged during each loop.
        cursor.execute('USE DATABASE ' + TARGET_DATABASE) 
        cursor.execute('USE SCHEMA ' + TARGET_SCHEMA)        

        # Create the view, selecting all columns and filtering out any deleted records.
        cursor_execute('CREATE OR REPLACE VIEW ' + record[0] + ' AS SELECT ' + column_list + ' FROM ' + FIVETRAN_DATABASE + '.' + FIVETRAN_SCHEMA + '.' + record[0] + ' WHERE _FIVETRAN_DELETED <> TRUE')

    

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
    cursor_execute('USE DATABASE ' + TARGET_DATABASE)
    
    cursor_execute('CREATE SCHEMA IF NOT EXISTS ' + TARGET_DATABASE + '.' + TARGET_SCHEMA)

    replicate_views()

    cursor_execute('GRANT ALL PRIVILEGES ON SCHEMA ' + TARGET_DATABASE + '.' + TARGET_SCHEMA + ' TO ROLE SYSADMIN')

    cursor_execute('GRANT USAGE ON DATABASE ' + TARGET_DATABASE + ' TO ROLE ' + TARGET_ROLE)
    cursor_execute('GRANT USAGE ON SCHEMA ' + TARGET_DATABASE + '.' + TARGET_SCHEMA + ' TO ROLE ' + TARGET_ROLE)
    cursor_execute('GRANT SELECT ON ALL TABLES IN SCHEMA ' + TARGET_DATABASE + '.' + TARGET_SCHEMA + ' TO ROLE ' + TARGET_ROLE)
    cursor_execute('GRANT SELECT ON ALL VIEWS IN SCHEMA ' + TARGET_DATABASE + '.' + TARGET_SCHEMA + ' TO ROLE ' + TARGET_ROLE)

finally:
    cursor.close()
