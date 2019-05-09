'''
Author: Kirk Wilson

This script will take a given CSV data file, automatically upload it to stage table in Snowflake, profile the column data types, and populate a "final" table with appropriate data types.
There are some inefficiencies and shortcuts taken in this script, but it was created only for internal use for specific use cases and it satisifies all of those needs.

Script process flow:
Given a source CSV file and a few basic input parameters, this script will perform the following actions to create and populate a corresponding Snowflake table:
1. Read column names from the header row and create a corresponding stage table in Snowflake, with all columns defined as type VARCHAR.
2. Populate the newly created stage table with the CSV data (as all text).
3. Profile the data in the stage table to determine actual data types and to determine if any columns are all NULL.
4. Create the final table in Snowflake with appropriate data types, including only columns that have data.
5. Copy data from the stage to final table, performing appropriate data conversions as needed.

Any scripts that are executed are also written to a flat file with the same name as the target table.

*********************************************************************************************************************************************************

Script requirements:
- Python 3.x.
    Download at https://www.python.org/downloads and install like any standard program.
    At the initial installation screen, check the box to say "Add Python to PATH".
- Snowflake Connector for Python.
    Once Python is installed, open a command prompt and execute this command to automatically install the connector
    (run the command directly from the command line - not within Python):
    pip install --upgrade snowflake-connector-python

    NOTE: If the command does not work, try executing it from this directory
    (the 'Python37-32' folder might be named differently depending on the specific version of Python you're using):
    C:/Users/<Your Name>/AppData/Local/Programs/Python/Python37-32/Scripts>
- Snowflake private key file for the user SCRIPT_USER, with the SNOWFLAKE_PRIVATE_KEY_PATH variable set to the proper location of the file.
    Required for authentication to Snowflake.

This script must be executed via the command line and have six arguments passed in:
1. Source CSV file location.
2. Snowflake file format.
    This format must be defined in the same Snowflake database and schema the script will be executed in.
    This tells Snwoflake basic information about the format of the file so it can parse it appropriately.
    Issue the following Snowflake statement to see defined formats in the current database/schema: SHOW FILE FORMATS;
3. Destination table name to be created (or overwritten) in Snowflake.
4. Snowflake database where the table will reside.
5. Snowflake schema where the table will reside.
6. Snowflake warehouse to use for executing any statements.

Example execution syntax to demonstrate loading multiple files sequentially via Windows batch file:
@ECHO OFF
SET DATABASE="DEV"
SET SCHEMA="ARCHIVE"
SET WAREHOUSE="DEV_ELT_WH"
C:/Users/<Your Name>/AppData/Local/Programs/Python/Python37-32/python.exe snowflake_file_loader.py "F:/xxx/concur_expenses.csv" CSV CONCUR_EXPENSES %DATABASE% %SCHEMA% %WAREHOUSE%
C:/Users/<Your Name>/AppData/Local/Programs/Python/Python37-32/python.exe snowflake_file_loader.py "F:/xxx/ia_invoice.csv" CSV BASWARE_AP_INVOICE_LINES %DATABASE% %SCHEMA% %WAREHOUSE%
C:/Users/<Your Name>/AppData/Local/Programs/Python/Python37-32/python.exe snowflake_file_loader.py "//xxx/Groups/AP/Basware_Images/Invoice_data/Data170718/ia_invoice.csv" CSV BASWARE_AP_INVOICE_LINES %DATABASE% %SCHEMA% %WAREHOUSE%

*********************************************************************************************************************************************************

Source file requirements:
 - CSV or TSV format
 - File format is understood and there is correspdonding format defined in Snowflake
 - Header row with Snowflake-friendly column names.
     - Spaces are automatically converted to underscores and periods are removed. Any other special character will result in an error without modifying this script.
 - Only supports text, number, and date/time data types
 - Date/time formats are understood and represented in the TIMESTAMP_INPUT_FORMATS list.
 
 
 SUPPORTING SNOWFLAKE OBJECTS
 ----------------------------
 -- Simple function that identifies the number of digits to the right of the decimal point in a number
CREATE OR REPLACE FUNCTION "DECIMAL_SCALE"(NUMBER_AS_STRING VARCHAR)
RETURNS NUMBER(38,0)
LANGUAGE SQL
AS '
  length(substring(number_as_string, case when position(''.'', number_as_string)  = 0 then length(number_as_string) else position(''.'', number_as_string) end + 1, length(number_as_string)))
';

-- Sample format (much better examples are at https://docs.snowflake.net/manuals/sql-reference/sql/create-file-format.html)
CREATE OR REPLACE FILE FORMAT CSV
	SKIP_HEADER = 1
	FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
	NULL_IF = ('')
	ENCODING = 'iso-8859-1'
;

-- Populated by this script if there are any errors
create or replace TABLE FILE_LOAD_ERRORS (
	ERROR VARCHAR(16777216),
	FILE VARCHAR(16777216),
	LINE NUMBER(38,0),
	CHARACTER NUMBER(38,0),
	CATEGORY VARCHAR(16777216),
	TABLE_NAME VARCHAR(16777216),
	COLUMN_NAME VARCHAR(16777216),
	REJECTED_RECORD VARCHAR(16777216),
	MD_INSERT_DATE TIMESTAMP_NTZ(9)
);
'''

import snowflake.connector
import csv
import sys # Used only for getting input parameters.
from datetime import datetime # Used only for logging.
import codecs
import os # Used for misc. file handling tasks.
import ntpath # Used to get just the file name from a file path
# All of the below libraries are only used for Snowflake authentication.
# Code is taken from the "Key Pair Authentication" section at https://docs.snowflake.net/manuals/user-guide/python-connector-example.html
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric import dsa
from cryptography.hazmat.primitives import serialization

# Before doing anything else, make sure the correct number of arguments was passed in to the script.
# Arguments are automatically stored in the list "sys.argv".
# sys.argv[0] by default is the name of the script, so since 6 arguments are expected need to make sure there are actually 7.
if len(sys.argv) != 7:
    print('Incorrect number of arguments specified.  Required arguments: ')
    print('  Source File Location')
    print('  Snowflake File Format')
    print('  Final Table Name')
    print('  Snowflake Database')
    print('  Snowflake Schema')
    print('  Snowflake Warehouse')
    print('\nExample: python ' + sys.argv[0] + ' C:/folder/file.csv CSV TABLE_NAME ARCHIVE CERTIFY DEV_WH')
    sys.exit()

    
###########################
# Define global variables #
###########################
# Note that any variables in all caps are intended to be global static variables (there's no way to enforce a static variable in Python though)
# Input parameters
SOURCE_FILE_LOCATION = sys.argv[1]
FILE_FORMAT = sys.argv[2]
FINAL_TABLE_NAME = sys.argv[3]
SNOWFLAKE_DATABASE = sys.argv[4]
SNOWFLAKE_SCHEMA = sys.argv[5]
SNOWFLAKE_WAREHOUSE = sys.argv[6]
# /Input parameters

# Get the current directory where this script resides.  Do this so a "log" folder can be created if necessary and logs written to that folder.
LOG_FOLDER_PATH = os.path.dirname(os.path.realpath(__file__)).replace('\\', '/') + '/logs/'

LOG_FILE_NAME = FINAL_TABLE_NAME + '.sql'

STAGE_TABLE_NAME = 'STAGE_' + FINAL_TABLE_NAME

# Used for Snowflake login authentication.
SNOWFLAKE_PRIVATE_KEY_PATH = 'C:/Python/rsa_key.p8'

# Used in table comments to store the source file name.
SOURCE_FILE_NAME = SOURCE_FILE_LOCATION[SOURCE_FILE_LOCATION.rfind('/')+1:]

# Used in the check_for_date function.  Stores Snowflake date mask formats (https://docs.snowflake.net/manuals/sql-reference/data-types-datetime.html).
TIMESTAMP_INPUT_FORMATS = ['DD-MON-YY', 'Mon DD, YYYY HH:MI:SS AM', 'DD-MON-YY HH12.MI.SS.FF9 AM', 'YYYY-', 'MM/DD/YYYY HH12:MI AM', 'AUTO']


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
    account='xxxxx',
    private_key=pkb)

cursor = snowflake_connection.cursor()


###################
# Create log file #
###################
# Create a 'logs' folder in the same directory where this script resides if the folder doesn't already exist.
if not (os.path.exists(LOG_FOLDER_PATH)):
    os.mkdir(LOG_FOLDER_PATH)

# Like above, create the log file now for easy reference anywhere else in the script.
# The 'w+' argument means it will create a file if it doesn't exist and overwrite a file if it does (w = write, + = create file if it doesn't exist).
log_file = open(LOG_FOLDER_PATH + LOG_FILE_NAME, 'w+')


##################################
# Class and Function definitions #
##################################
# Basic class used to store column attributes in a data structure for simplicity.
# The "__init__" function is a built-in Python constructor function.  The first argument by default is the object itself ('self').
# Example for how to define a new "column" object:
# new_column = column('COLUMN_NAME', 'VARCHAR', 0, 'NOT_A_DATE')
# Access the elements with standard dot notation, such as: print(new_column.name)
class column(object):
    def __init__(self, name, data_type, scale, date_mask):
        self.name = name
        self.data_type = data_type
        self.scale = scale # Only used for numbers
        self.date_mask = date_mask # Only used for dates


# This is broken out into a function to allow for different date formats to be tested.
# Different source files can have completely different date/time formats, and Snowflake can't always automatically detect the types.
# Altering the session lets the format be explicitly specified.
# The TRY_TO_TIMESTAMP is a built-in Snowflake function that will just return NULL instead of throwing an error if a value can't be converted to a datetime.
# The logic is that if the TRY_TO_TIMESTAMP function returns anything other than NULL, and there is a non-NULL value in the column, then it is a date.
# Query the entire column for non-NULL values and apply the TO_TIMESTAMP function.  If the function doesn't return any NULLs (count = 0), it's a date.
# Given a column, this function will test if its data can be successfully converted to DATETIME format and if so the appropriate format mask
# is returned by the function, or NOT_A_DATE is returned if nothing was found.
def check_for_date(column_name):
    # Loop through all pre-defined date formats defined at the top of this script.
    for timestamp_format in TIMESTAMP_INPUT_FORMATS:
        cursor.execute("ALTER SESSION SET TIMESTAMP_INPUT_FORMAT = '" + timestamp_format + "'")
        if cursor.execute("""SELECT COUNT(*)
                        FROM """ + STAGE_TABLE_NAME + """
                        WHERE TRY_TO_TIMESTAMP(""" + column_name + """) IS NULL
                        AND """ + column_name + """ IS NOT NULL""").fetchone()[0] == 0:
            return timestamp_format
    
    # This is either not a date, or it's a date in a format that's not expected.
    return 'NOT_A_DATE'

# This simple function consolidates all logging and Snowflake execution in one place, to reduce the number of "log_file.write" and "cursor.execute" statements in the main script.
# The second argument is a boolean.
def log_and_execute(snowflake_statement, print_timestamp):
    if print_timestamp:
        log_file.write('-- ' + datetime.now().strftime('%m-%d-%Y %H:%M:%S') + '\n')

    log_file.write(snowflake_statement)
    cursor.execute(snowflake_statement)


##############################
# Start of main script logic #
##############################
# The "try" block is only used to ensure the Snowflake cursor and log file are always closed in the "finally" block at the bottom of the script, even if there is an exception.
# Since this script will only be used internally by technically literate people, "proper" exception handling was not implemented as it would not add any significant value.
try:
    ##############################
    # Read headers of source CSV #
    ##############################
    # First read the headers of the source file and generate a stage table in Snowflake that has all columns
    # set to VARCHAR data type.  This will allow for accurate profiling later that will be used to create the final table
    # with accurate data types.
    if FILE_FORMAT == 'CSV':
        csv_delimiter = ','
    elif FILE_FORMAT == 'CSV_SEMICOLON_DELIMITER':
        csv_delimiter = ';'
    elif FILE_FORMAT == 'TSV':
        csv_delimiter = '\t'
        
    with open(SOURCE_FILE_LOCATION) as csv_file:
        csv_reader = csv.reader(csv_file,  delimiter=csv_delimiter)
        
        header = next(csv_reader)  # Get just the first line

##        # Count the number of records in the file for auditing purposes.  
##        for line_count, line in enumerate(csv_file):
##            pass

    csv_file.close()

    ######################################
    # Log record count in AUDIT database #
    ######################################
    '''
    This was intended to be used with the "line_count" variable from the above FOR loop, but I determined for the data sets at <client> it was easier to just manually validate file counts than to automate.
    As a result this code is not complete but I'm leaving it here in case the concept is useful for someone else.

    # Set the AUDIT database/schema initially. 
    cursor.execute('USE DATABASE AUDIT');
    cursor.execute('USE SCHEMA PUBLIC');
    cursor.execute('USE WAREHOUSE ' + SNOWFLAKE_WAREHOUSE);

    # Get the audit key value associated with this source.
    source_key = cursor.execute("""SELECT SOURCE_KEY
                                    FROM RECORD_COUNT_SOURCES
                                    WHERE SOURCE_TYPE = 'FLAT FILE'
                                    AND AUDIT_ENABLED_FLAG = 'Y'
                                    AND SOURCE_FILE_NAME = '""" + ntpath.basename(path) + """'""").fetchone()

    ... do more stuff ...
    '''

    
    ##################
    # Snowflake Prep #
    ##################
    # Set the proper Snowflake warehouse/database/schema for this session.
    log_and_execute('USE WAREHOUSE ' + SNOWFLAKE_WAREHOUSE + ';\n\n', True)
    log_and_execute('USE DATABASE ' + SNOWFLAKE_DATABASE + ';\n', False)
    log_and_execute('USE SCHEMA ' + SNOWFLAKE_SCHEMA + ';\n', False)

    # Set this so that any two-digit year values are processed appropriately.
    # This will ensure a DD-MM-YY value of '01-02-03' is loaded as the date '01-02-2003' instead of '01-02-0003', for example.
    log_and_execute('ALTER SESSION SET TWO_DIGIT_CENTURY_START = 1980;\n\n', False)
    

    ###################################
    # Create STAGE table in Snowflake #
    ###################################
    # Create the DDL for the STAGE table, which will have all columns defined as VARCHAR.
    stage_table_ddl = "CREATE OR REPLACE TABLE " + STAGE_TABLE_NAME + " \nCOMMENT = 'Source file: " + SOURCE_FILE_NAME + "' \n("
    for column_name in header:
        column_name = column_name.replace(' ', '_')
        column_name = column_name.replace('.', '')
        column_name = column_name.replace('/', '_')
        column_name = column_name.replace('(', '')
        column_name = column_name.replace(')', '')
        stage_table_ddl += column_name + ' VARCHAR,\n'
    stage_table_ddl = stage_table_ddl[:-2] + '\n)'

    # Log the DDL, create the STAGE table in Snowflake, and print a status update message to the console.
    log_and_execute(stage_table_ddl + ';\n\n', True)
    print('Stage table ' + STAGE_TABLE_NAME + ' created')

    #####################################
    # Populate STAGE table in Snowflake #
    #####################################
    # Now the stage table exists.  Load the source data into it.
    # The PUT command will automatically compress and upload the file to an internal Snowflake stage within AWS/Azure.
    # The COPY INTO command will extract the contents of the staged file into the destination table.
    put_command = "PUT 'file://" + SOURCE_FILE_LOCATION.replace('\\', '/') + "' @%" + STAGE_TABLE_NAME
    copy_command = "COPY INTO " + STAGE_TABLE_NAME + " FILE_FORMAT='PUBLIC." + FILE_FORMAT + "' ON_ERROR=CONTINUE"
    
    # Log, execute, and print status message
    log_and_execute(put_command + ';\n', True)
    log_and_execute(copy_command + ';\n\n', False)
    
    # Log any data load errors 
    log_and_execute("INSERT INTO AUDIT.PUBLIC.FILE_LOAD_ERRORS (SELECT ERROR, FILE, LINE, CHARACTER, a.CATEGORY, '" + STAGE_TABLE_NAME + "', COLUMN_NAME, REJECTED_RECORD, CURRENT_TIMESTAMP::TIMESTAMP_NTZ FROM TABLE(VALIDATE(" + STAGE_TABLE_NAME + ", JOB_ID => '_last')) a);\n", False)

    # Delete the file that was uploaded to the internal Snowflake stage.  This is the same as setting "PURGE=TRUE" in the above "COPY INTO" command,
    # however if the file is purged as part of the COPY command, the VALIDATE function does not work and visibility to any loading errors is lost.
    log_and_execute('REMOVE @%' + STAGE_TABLE_NAME + ';\n\n', False)

    print('Stage table ' + STAGE_TABLE_NAME + ' populated')

    # Now begin profiling the data that was just loaded.
    # This columns list will hold all columns in the source CSV file, in the type of the "column" class defined above.
    columns = []

    #################################
    # Profile the column data types #
    #################################
    # Loop through all columns in the STAGE table that was just created, in the same order they are defined in the database.
    for record in cursor.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" + STAGE_TABLE_NAME.upper() + "' AND TABLE_SCHEMA = '" + SNOWFLAKE_SCHEMA.upper() + "' ORDER BY ORDINAL_POSITION").fetchall():
        # First check if the column contains values
        count = cursor.execute("""SELECT COUNT(*)
                                    FROM """ + STAGE_TABLE_NAME + """
                                    WHERE """ + record[0] + """ IS NOT NULL""").fetchone()

        if count[0] == 0:
            # This column is all null, so just ignore it and skip to the next one.  This column will not be created in the final table but will remain in the STAGE table.
            continue

        # There is something in the column.  Assume it's VARCHAR initially.
        # Store the column details in a class for simplicity and readability.
        # Note that 'None' is equivalent to NULL since the scale and date_mask properties aren't needed if the column truly is a VARCHAR.
        new_column = column(record[0], 'VARCHAR', None, None)
        
        # Check to see if the column is a number.  If this query returns a count of 0, the column is a number data type.
        # The REPLACE function is used in case the source file has commmas in the number strings to act as a thousands separator, which some files do.
        # The TRY_TO_NUMBER is a built-in Snowflake function that will just return NULL instead of throwing an error if a value can't be converted to a number.
        # See the comments in the "check_for_date" function for more details on the logic behind the use of the TRY_TO_% functions.
        count = cursor.execute("""SELECT COUNT(*)
                                    FROM """ + STAGE_TABLE_NAME + """
                                    WHERE TRY_TO_NUMBER(REPLACE(""" + new_column.name + """, ',', '')) IS NULL
                                    AND """ + new_column.name + """ IS NOT NULL""").fetchone()[0]
        
        if count == 0:
            new_column.data_type = 'NUMBER'

            # Now determine the appropriate scale for the number (how many digits to the right of the decimal point need to be allocated).
            # The DECIMAL_SCALE function is a UDF created in Snowflake to make the query simpler.  Use either of the two following commands in Snowflake for more details on it:
            # SHOW USER FUNCTIONS;
            # SELECT GET_DDL('FUNCTION', 'PUBLIC.DECIMAL_SCALE(VARCHAR)');
            new_column.scale = cursor.execute("""SELECT NVL(MAX(DECIMAL_SCALE(""" + new_column.name + """)), 0)
                                                FROM """ + STAGE_TABLE_NAME).fetchone()[0]

        if new_column.data_type != 'NUMBER':
            # This column isn't a number, but it might still be DATETIME.  If the check_for_date function returns anything other than the value 'NOT_A_DATE', this column
            # is a date, and the function will return the appropriate date mask that Snowflake recognizes for it.
            new_column.date_mask = check_for_date(new_column.name)
            if new_column.date_mask != 'NOT_A_DATE':
                new_column.data_type = 'DATETIME'

        # All profiling for the current colunmn is finished.  Add it to the columns list and loop to the next column.
        columns.append(new_column)
        print('Column ' + new_column.name + ' profiled: Type=' + new_column.data_type)

    
    #######################################
    # Create the final table in Snowflake #
    #######################################
    # If the source file is empty (other than the header row), there will not be anything in the columns list.
    # If that's the case, log it and exit rather than create an empty table.
    if len(columns) == 0:
        log_file.write('-- No data in source file')
        exit()
    
    # All of the columns have been processed.  Generate the final create table DDL statement with the appropriate data types.
    table_ddl = "CREATE OR REPLACE TABLE " + FINAL_TABLE_NAME + " \nCOMMENT = 'Source file: " + SOURCE_FILE_NAME + "' \n(\n"

    for column in columns:
        table_ddl += column.name + ' ' + column.data_type

        if column.data_type == 'NUMBER' and column.scale > 0:
            table_ddl += '(38, ' + str(column.scale) + ')' # Precision will always be 38 for numbers

        table_ddl += ',\n'

    # Strip off the final ',' and newline character
    table_ddl = table_ddl[:-2] + '\n)'
    
    # Create or replace the table
    log_and_execute(table_ddl + ';\n\n', True)
    print('Final table ' + FINAL_TABLE_NAME + ' created')

    #########################################
    # Populate the final table in Snowflake #
    #########################################
    # Generate the insert statement to copy date from the stage to final table.  This will handle data type conversions automatically.
    # No need to specify the column order since they will be processed in the same order the table was just created.
    data_copy_dml = 'INSERT INTO ' + FINAL_TABLE_NAME + ' \n( \nSELECT \n'

    for column in columns:
        if column.data_type == 'VARCHAR':
            data_copy_dml += column.name + ',\n'
        elif column.data_type == 'NUMBER':
            if column.scale == 0:
                data_copy_dml += "TO_NUMBER(REPLACE(" + column.name + ", ',', '')),\n"
            else:
                data_copy_dml += "TO_NUMBER(REPLACE(" + column.name + ", ',', ''), 38, " + str(column.scale) + "),\n"
        elif column.data_type == 'DATETIME':
            data_copy_dml += "TO_TIMESTAMP(" + column.name + ",'" + column.date_mask + "'),\n"

    data_copy_dml = data_copy_dml[:-2] + '\nFROM ' + STAGE_TABLE_NAME + '\n)'

    # Log and copy the data from the stage to final table.
    log_and_execute("ALTER SESSION SET TIMESTAMP_INPUT_FORMAT = 'AUTO';\n\n", False) # Reset the session timestamp format, or else errors can occur when using 'AUTO' conversion.
    log_and_execute(data_copy_dml + ';\n\n', True)
    log_and_execute('COMMIT;\n\n', False) # This is only included for the log file - commits are automatic when executed from Python.
    print('Final table ' + FINAL_TABLE_NAME + ' populated\n')

    # One last log of the timestamp for when the script finally ends.
    log_file.write('-- ' + datetime.now().strftime('%m-%d-%Y %H:%M:%S'))

    

    exit()
finally:
    cursor.close()
    log_file.close()


