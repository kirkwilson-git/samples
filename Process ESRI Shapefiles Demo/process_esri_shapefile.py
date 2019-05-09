'''
Author: Kirk Wilson

Created for very specific internal use case, and as such is not documented exhaustively and some shortcuts are taken.  

Demonstrates how to open and parse an ESRI shapefile.

High-level process flow:
Connect to Snowflake
Read the MLS shapefile into memory
Create an empty JSON file that will store API results
Issue Snowflake query to get all lat/long coordinates for records that haven't been already processed
	For each coordinate, check if the point is contained within the shapefile and if so, write the corresponding metadata from that point in the shapefile into the JSON file
Upload the final JSON file to an AWS S3 bucket
'''

import sys
sys.path.insert(0, r'/shapefiles') # Needed to be able to import the below "shapefile" library, which is at "/shapefiles/shapefile.py"
import shapefile # pyshp library used to read the shapefile

import snowflake.connector
from shapely.geometry import shape, Point # Used to simplify process of working with shapes and points
from pyproj import Proj, transform # Used to translate coordinate systems
import json
import boto
from boto.s3.key import Key

# All of the below libraries are only used for Snowflake authentication.
# Code is taken from the "Key Pair Authentication" section at https://docs.snowflake.net/manuals/user-guide/python-connector-example.html
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric import dsa
from cryptography.hazmat.primitives import serialization

BUCKET_NAME  = '<bucket_name>'
BUCKET_FOLDER_PATH = 'shapefiles/'
OUTPUT_FILE_NAME = 'shapefile_output.json'

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
        password='xxxxxxx'.encode(),
        backend=default_backend()
    )

pkb = p_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption())

snowflake_connection = snowflake.connector.connect(
    user='DW_SYSTEM',
    account='xxxxxxx',
    private_key=pkb)

cursor = snowflake_connection.cursor()

try:
    boto.config.add_section("Boto")
except ConfigParser.DuplicateSectionError:
    pass
boto.config.set("Boto", "metadata_service_num_attempts", "20")

# Simple dictionary to contain the index mapping for the shapefile records, which contain all of the metadata about each shape.  
# Note that these fields are defined in shapefile_reader.fields, but there is an extra field 'DeletionFlag' that is defined as 
# index 0, but it doesn't seem to actually be used anywhere and so this dictionary makes it explicit which index is being used for which field.
record_indexes = {
  'ID': 0,
  'NWMLS': 1,
  'Name': 2,
  'Layer': 3,
  'Path': 4
  }

# These projections are used to convert lat/long coordinates to the same system used by the shapefile.  
# This is needed to be able to easily check if a specific lat/long point falls within a bounded area (MLS area) defined in the shapefile
# This was found at https://gis.stackexchange.com/questions/78838/converting-projected-coordinates-to-lat-lon-using-python
inputProjection = Proj(init='epsg:4326') # Standard latitude/longitude coordinate system
outputProjection = Proj(init='epsg:3857') # Coordinate system used by the shapefile

# Returns boolean value indicating if the parameterized lat/long falls within the parameterized polygon
def is_point_in_shapefile(polygon, longitude, latitude):
    # Use the pyproj library to translate coordinates accordingly so the lat/long are in the same system as the shapefile    
    translated_long, translated_lat = transform(inputProjection, outputProjection, longitude, latitude)

    # Build a shapely point from the converted coordinates that now match the shapefile coordinates
    point = Point(translated_long, translated_lat)
   
    return polygon.contains(point)


# Read the shapefile using the pyshp library
shapefile_reader = shapefile.Reader(r'/shapefiles/NWMLS Shapefiles v2.shp')

def check_lat_long(id, record_source, latitude, longitude, json_output_file):

   	#print(id)

  	# Loop through each shape (which is an MLS area) in the shapefile and check if it contains the desired point
	for shapefile_object in shapefile_reader:
	    # shapefile_object.shape is a collection of x, y points that make up the bounds of each MLS area
	    # The "shape" function converts the shapefile shape to a shapely object, which is much more convenient to work with than raw coordinates.
		if is_point_in_shapefile(shape(shapefile_object.shape), longitude, latitude):
			json_dict = {
				'ID': id,
				'RECORD_SOURCE': record_source,
				'MLS_NAME': shapefile_reader.fields[2][0],
				'MLS_AREA_NAME': shapefile_object.record[record_indexes['Name']],
				'COUNTY': shapefile_object.record[record_indexes['Layer']],
				'MLS_AREA_ID': shapefile_object.record[record_indexes['ID']]          
				}
            
			# Write the updated JSON to the output file
			json.dump(json_dict, json_output_file) 

			# Add a newline to the file for readability
			json_output_file.write('\n')
            
			print('Yes')
       	
        else:
        	print('No')
          
try:
    # Create a new file to store all of the JSON results in one place
    # This will overwrite the file if it already exists
    json_output_file = open(OUTPUT_FILE_NAME, 'w')

    # Initialize Snowflake query parameters
    cursor.execute('USE WAREHOUSE LOAD_WH_GOOGLE')
    cursor.execute('USE DATABASE STAGE')

    result_set = cursor.execute('SELECT ID, RECORD_SOURCE, LATITUDE, LONGITUDE FROM ABSTRACT.MLS_SHAPEFILE_MISSING_IDS').fetchall()
    
    for loop_count, record in enumerate(result_set, 1):

      	print(record[0])
       
        if loop_count % 100 == 0:
          print('Executing iteration ' + str(loop_count))
        
        check_lat_long(record[0], record[1], record[2], record[3], json_output_file)

    json_output_file.close()

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
