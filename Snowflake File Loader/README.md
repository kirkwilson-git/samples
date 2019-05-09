# Automatically upload and profile data from flat file to Snowflake

This is an extremely useful script for automatically uploading CSV data to Snowflake and profiling data types.  Note that this will work with not only CSV files types, but any data format that can be defined within Snowflake.  This script can also be easily used in a batch file to upload multiple files at once (example batch script is provided).  

All statements executed in Snowflake are logged.  The script also logs any errors during the load process to a table in Snowflake.

See the comments in the script for more details.
