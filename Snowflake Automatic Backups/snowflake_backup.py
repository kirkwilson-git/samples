'''
Author: Kirk Wilson

This script will create clones for each given Snowflake database, which will act as a complete backup of all objects and data within that database.
It will then drop the backup from <NUMBER_OF_DAYS_HISTORY_TO_RETAIN> ago.  Additionally, it will create a full text DDL backup of the database, saved
at the given location.  The DDL backups are not automatically deleted after a certain time period.

Since Snowflake clones are "zero-copy" and generally very quick to create (a few minutes, depending on the size and number of objects), this is an
extremely simple and effective solution with virtually no disadvantages.

Note that depending on your ETL architecture, this process could significantly increase your overall Snowflake storage costs.  If you are implementing a 
truncate-and-reload solution every day for example, Snowflake would by default keep the truncated data around for 1 day (default time travel retention)
plus an extra 7 days for fail safe.  By creating clones, all of that data will be retained for signfiicantly longer depending on how you have this script
configured.  However, given that stroage is relatively cheap, that should be a minor consideration given the potentially huge benefits this backup solution
can provide.

If storage cost is a concern, this script also uses the GET_DDL('DATABSAE', '<DATABASE_NAME>') statement to retain full text DDL backups as an extra layer of
redundancy with at least the database object definitions (but not the data itself) backed up effectively forever.

References for Snowflaking cloning;
https://docs.snowflake.net/manuals/sql-reference/sql/create-clone.html
https://www.snowflake.com/blog/snowflake-fast-clone/
'''

import snowflake.connector
import datetime
# All of the below libraries are only used for Snowflake authentication.
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric import dsa
from cryptography.hazmat.primitives import serialization

# These are the only variables you need to configure for your envioronment.
SNOWFLAKE_ACCOUNT = 'xxxxxx'
SNOWFLAKE_USER = 'SCRIPT_USER'
SNOWFLAKE_RSA_KEY_PASS = 'xxxxxxx' # Ideally store this as an OS environment variable
SNOWFLAKE_RSA_PRIVATE_KEY_PATH = 'c:/python/rsa_key.p8'
NUMBER_OF_DAYS_HISTORY_TO_RETAIN = 15
DATABASES_TO_BACKUP = ('PROD_DB', 'PROD_POR_DB')
DDL_BACKUP_LOCATION = 'C:/alteryx_wf/Snowflake_Backups/'

# Prefix the backups with Z so they will always appear at the bottom of the database list in the Snowflake UI to minimize confusion/annoyance.
BACKUP_PREFIX = 'zBACKUP_' 

# Get zero-padded date strings for the current date and the "old date", which is the date for the backup that will be deleted.
current_date = datetime.datetime.now().strftime('%m_%d_%Y')
old_date = (datetime.datetime.now() - datetime.timedelta(days=NUMBER_OF_DAYS_HISTORY_TO_RETAIN)).strftime('%m_%d_%Y')

# Establish the Snowflake connection
# Key pair authentication is used rather than directly storing the SCRIPT_USER user's password in this script for extra security.
# Currently the private key passphrase is stored directly below, but it only works if the user also has the private key itself which is in an external file.
# The private key passphrase can be easily externalized from this script as well for an additional layer of security if needed.
# Code is taken from the "Key Pair Authentication" section at https://docs.snowflake.net/manuals/user-guide/python-connector-example.html
with open(SNOWFLAKE_RSA_PRIVATE_KEY_PATH, 'rb') as key:
    p_key = serialization.load_pem_private_key(
        key.read(),
        password=SNOWFLAKE_RSA_KEY_PASS.encode(),
        backend=default_backend()
    )

pkb = p_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption())

snowflake_connection = snowflake.connector.connect(
    user=SNOWFLAKE_USER,
    account=SNOWFLAKE_ACCOUNT,
    private_key=pkb)

cursor = snowflake_connection.cursor()

try:
    for database in DATABASES_TO_BACKUP:
        print('Current database: ' + database)
        print('Creating clone...')
        cursor.execute('CREATE OR REPLACE DATABASE ' + BACKUP_PREFIX + database + '_' + current_date + ' CLONE ' + database)

        print('Dropping ' + old_date + ' backup...')
        cursor.execute('DROP DATABASE IF EXISTS ' + BACKUP_PREFIX + database + '_' + old_date)

        print('Creating DDL backup at ' + DDL_BACKUP_LOCATION + '...')
        ddl = cursor.execute("SELECT GET_DDL('DATABASE', '" + database + "')").fetchone()

        with open(DDL_BACKUP_LOCATION + database + '_' + current_date + '_DDL.txt', 'w') as f: 
            f.write(ddl[0]) 

        print('')
finally:
    cursor.close()
