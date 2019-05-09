'''
Author: Kirk Wilson

This script was created solely for internal use to identify a collection of invoice-related files provided by a SaaS vendor,
relate the files to a distinct record in tabular data (also provided by the vendor), and zip up all files into a single location.

It primarily demonstrates how to connect to and query a Snowflake database and how to zip files.  

It's intended to be used only for reference.

'''


'''
EXAMPLE CODE FOR HOW TO ADD ALL FILES WITHIN A FOLDER INTO A SINGLE .ZIP FILE 
import shutil
import os 

ROOT_DIRECTORY = 'C:/Users/kwilson1/Downloads/Certify/'
PREFIX = 'SM'

# Loop through sub-folders in ROOT_DIRECTORY, and for each directory
# create a zip file with its contents
for folder in os.listdir(ROOT_DIRECTORY):
    if folder != 'Compressed':
        file_name = ROOT_DIRECTORY + PREFIX + '_' + folder
        
        shutil.make_archive(file_name.replace(' ', ''), 'zip', ROOT_DIRECTORY + folder)

'''



from os import listdir
import snowflake.connector
import os 
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric import dsa
from cryptography.hazmat.primitives import serialization
import zipfile
from os.path import basename
from datetime import datetime # Used only for logging.

# Location where zip files will be created
TARGET_DIRECTORY = 'W:/Basware/Attachments/'

# Location where original attachment documents are located (files to be zipped)
SOURCE_DIRECTORY = 'W:/Basware_docs/'

# Used for Snowflake login authentication.
SNOWFLAKE_PRIVATE_KEY_PATH = 'C:/Python/rsa_key.p8'

SNOWFLAKE_DATABASE = 'ARCHIVE'
SNOWFLAKE_SCHEMA = 'BASWARE'
SNOWFLAKE_WAREHOUSE = 'DEV_WH'


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
        password='xxxxxxxxx'.encode(),
        backend=default_backend()
    )

pkb = p_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption())

snowflake_connection = snowflake.connector.connect(
    user='SCRIPT_USER',
    account='xxxxxxx',
    private_key=pkb)

snowflake_cursor = snowflake_connection.cursor()

# First argument is the name of the file to be created.
# Second argument is a list of files (with their full paths) to be added to the zip file.
def create_zip_file(zip_file_name, file_list):
    with zipfile.ZipFile(TARGET_DIRECTORY + zip_file_name + '.zip', 'w') as invoice_zip_file:
        for file in file_list:
            # The first argument is the full file path of the file to be zipped
            # The "basename" function in the second argument returns just the file name.
            # Without it, whatever folder structure is in the source file path would be written in the zip file.
            invoice_zip_file.write(file, basename(file))


try:
    snowflake_cursor.execute('USE DATABASE ' + SNOWFLAKE_DATABASE)
    snowflake_cursor.execute('USE SCHEMA ' + SNOWFLAKE_SCHEMA)
    snowflake_cursor.execute('USE WAREHOUSE ' + SNOWFLAKE_WAREHOUSE)

    total_invoice_file_count = snowflake_cursor.execute('''SELECT COUNT(*) FROM (
            SELECT V.INVOICE_ID, V.FILE_NAME
            FROM INVOICE_ATTACHMENTS_VIEW V,
            BASWARE_FILE_LIST L
            WHERE L.FILE_NAME = V.FILE_NAME
            )''').fetchone()[0]

    snowflake_cursor.execute('''SELECT V.INVOICE_ID, V.FILE_NAME
                                FROM INVOICE_ATTACHMENTS_VIEW V,
                                BASWARE_FILE_LIST L
                                WHERE L.FILE_NAME = V.FILE_NAME
                                ORDER BY 1, 2''')

    current_invoice = ''
    invoice_file_list = []

    start_time = datetime.now()
    print('Start time: ' + start_time.strftime('%m-%d-%Y %H:%M:%S'))
    
    for loop_count, record in enumerate(snowflake_cursor.fetchall(), 1):
        # Print the current status since this process can take a while for thousands of files.
        if loop_count % 1000 == 0:
            print('Executing iteration ' + str(loop_count) + ' of ' + str(total_invoice_file_count) + '  (' + str((loop_count / total_invoice_file_count) * 100) + '%)...')
            print('Duration: ' + str(datetime.now() - start_time))
        
        if current_invoice != record[0]:
            if len(invoice_file_list) > 0:
                # invoice_file_list is now populated with all files associated with this invoice.
                # Create the zip file and move on to the next invoice
                create_zip_file(current_invoice, invoice_file_list)

            # Reset the variables for the next invoice to be processed.
            invoice_file_list = []
            current_invoice = record[0]

        invoice_file_list.append(SOURCE_DIRECTORY + record[1])

    # Create the final zip file, since it won't be handled in the above loop
    create_zip_file(current_invoice, invoice_file_list)
    
finally:
    snowflake_cursor.close()
