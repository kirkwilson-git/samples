'''
Author: Kirk Wilson

This script will upload all files within a parameterized directory into the
parameterized S3 path in the S3 bucket.  This was created only for internal use and as a result takes some shortcuts.

Note that the target S3 path should not start with a "/" or else an empty folder will be created,
but a "/" should be included at the end of the path for this script to work properly.

To perform a count validation that all of the files were uploaded properly, install the AWS CLI utility and 
use this command:
	aws s3 ls s3://<bucket_name>/file/path/ --recursive | wc -l

'''

import boto3
import os
import sys

if len(sys.argv) != 3:
    print('Incorrect number of arguments specified.  Required arguments: ')
    print('  Folder containing files to upload to S3 (with "/" slash at the end)')
    print('  Target S3 path (with no leading "/", but with a trailing "/")')
    print('\nExample: python ' + sys.argv[0] + ' W:/Certify/Attachments/Canada/ CERTIFY/CANADA/')
    sys.exit()

SOURCE_FOLDER = sys.argv[1]
S3_TARGET_PATH = sys.argv[2]
BUCKET_NAME = '<bucket_name>'

session = boto3.Session(
    aws_access_key_id='xxxxxxxxxxxxx',
    aws_secret_access_key='xxxxxxxxxxxx',
)

s3 = session.resource('s3')

file_count = len(os.listdir(SOURCE_FOLDER))

for count, file in enumerate(os.listdir(SOURCE_FOLDER)):
    s3.meta.client.upload_file(SOURCE_FOLDER + file, BUCKET_NAME, S3_TARGET_PATH + file)
    print('Uploaded file ' + str(count + 1) + ' of  ' + str(file_count) + ':  ' + S3_TARGET_PATH + file)



