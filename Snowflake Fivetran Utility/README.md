# Snowflake / Fivetran Utility Script

Given the Snowflake/Fivetran architecture in use at one of my clients, this script automates the process of deploying a schema within the Snowflake FIVETRAN database into its own DATABASE.SCHEMA (defined in an input parameter to the script) where all data is exposed through views where any _FIVETRAN_DELETED = ‘Y’ records are filtered out.  

See the comments in the script for more details.
