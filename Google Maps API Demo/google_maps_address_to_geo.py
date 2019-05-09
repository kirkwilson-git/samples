''' 
Author: Kirk Wilson

Note this was created for a very specific use case for a real estate company.  It demonstrates how to use S3 and the Google Maps API.

High-level process flow:
Connect to Snowflake
Create an empty JSON file that will store API results
Issue Snowflake query to get all addresses that are missing their lat/long coordinates
	For each record, call the Google Maps API with the address and update the JSON file with the results after appending some custom JSON elements for later use
Upload the final JSON file to an AWS S3 bucket
'''

import requests
import snowflake.connector # pip install --upgrade snowflake-connector-python
import boto
from boto.s3.key import Key
import json
from datetime import datetime

# All of the below libraries are only used for Snowflake authentication.
# Code is taken from the "Key Pair Authentication" section at https://docs.snowflake.net/manuals/user-guide/python-connector-example.html
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric import dsa
from cryptography.hazmat.primitives import serialization

# Used for Snowflake login authentication.
SNOWFLAKE_PRIVATE_KEY_PATH = '/snowflake/private_rsa_key.p8'


# Establish Snowflake connection
# Establish the Snowflake connection first so it can be referenced anywhere easily.
# Key pair authentication is used rather than directly storing the DW_SYSTEM user's password in this script for extra security.
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
    user='DW_SYSTEM',
    account='xxxxxxxx',
    private_key=pkb)

GOOGLE_MAPS_API_URL = 'https://maps.googleapis.com/maps/api/geocode/json'
GOOGLE_API_KEY = google_key

BUCKET_NAME  = '<bucket_name>'
BUCKET_FOLDER_PATH = 'google/geo_address/'
OUTPUT_FILE_NAME = 'output.json'

cursor = snowflake_connection.cursor()

# Used just for logging
loop_counter = 0
startTime = datetime.now()

try:
    # Create a new file to store all of the Google Maps API JSON results in one place
    # This will overwrite the file if it already exists
    json_output_file = open(OUTPUT_FILE_NAME, 'w')

    # Initialize Snowflake query parameters
    cursor.execute('USE WAREHOUSE LOAD_WH_GOOGLE')
    cursor.execute('USE DATABASE STAGE')

    result_set = cursor.execute("SELECT ID, API_ADDRESS, RECORD_SOURCE FROM ABSTRACT.ADDRESS_GEO_MISSING_IDS").fetchall()

    for record in result_set:
        params = {'address': record[1], 'key': GOOGLE_API_KEY}

        # Issue Google Maps API call and get JSON response
        req = requests.get(GOOGLE_MAPS_API_URL, params=params)
        json_response = req.json()

        # Add new elements to JSON for future use in Snowflake
        json_response['ID'] = record[0]
        json_response['RECORD_SOURCE'] = record[2]

        # Write the updated JSON to the output file
        json.dump(json_response, json_output_file) 
        
        # Add a newline to the file for readability
        json_output_file.write('\n')

        # Print timing data every N records
        loop_counter += 1
        if loop_counter % 1000 == 0:
            print(str(loop_counter) + ': ' + str(datetime.now() - startTime))

    json_output_file.close()

    # Everything below is for connecting and uploading a file to S3
    try:
        boto.config.add_section("Boto")
    except ConfigParser.DuplicateSectionError:
        pass
    boto.config.set("Boto", "metadata_service_num_attempts", "20")
    
    # Connect to the S3 bucket
    s3 = boto.connect_s3()
    bucket = s3.lookup(BUCKET_NAME)

    # Upload the consolidated JSON file to the S3 bucket
    k = Key(bucket)
    k.key = BUCKET_FOLDER_PATH + OUTPUT_FILE_NAME
    k.set_contents_from_filename(OUTPUT_FILE_NAME)    
finally:
    # Close the Snowflake cursor regardless of any errors that were encountered.
    cursor.close()
