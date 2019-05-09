# This provides samples for managing Smartsheet webhooks.  It requires a running webserver to respond to the 
# Smartsheet requests.  Code for a sample webserver that does respond appropriately is in the "web_server.py" file.

# PREREQ: Install the smartsheet sdk with the command: pip install smartsheet-python-sdk
import smartsheet

# Used for formatting Smartsheet response messages for display
import json
from pprint import pprint

# Smartsheet API access token (functions as the Smartsheet password)
smartsheet_api_token = 'xxxxxxxxxxxxxxxxxx'

# ID of the Smartsheet sheet to read (identified by sheet properties in UI)
smartsheeet_sheet_id = 123456789

# Publically accessible URL where the Python web server is running that will
# listen for and process webhook callback messages.  Must be HTTPS and must have
# the '/' character at the end.  Use ngrok for a simple, free way to get a publically accessible URL.
callback_url = 'https://xxxxxx.ngrok.io/'

# Initialize Smartsheet client
ss = smartsheet.Smartsheet(smartsheet_api_token)

# Make sure we don't miss any errors (will cause the script to fail if Smartsheet returns any error)
ss.errors_as_exceptions(True)

def create_webhook(webhook_name):
    json_str = ss.Webhooks.create_webhook(
      ss.models.Webhook({
        'name': webhook_name,
        'callbackUrl': callback_url, 
        'scope': 'sheet', # This is the only supported scope currently (per Smartsheet documentation)
        'scopeObjectId': smartsheeet_sheet_id,
        'events': ['*.*'], # This is on the only supported event currently (everything)
        'version': 1}))

    # Display the result output in a slightly prettier format
    json_str = json.loads(str(json_str))
    pprint(json_str['result'])

def enable_webhook(webhook_id):
    json_str = ss.Webhooks.update_webhook(
      webhook_id,       # webhook_id
      ss.models.Webhook({
        'enabled': True}))

    json_str = json.loads(str(json_str))
    pprint(json_str['result'])

def list_webhooks():
    json_str = ss.Webhooks.list_webhooks(
      page_size=100,
      page=1,
      include_all=False)

    json_str = json.loads(str(json_str))
    pprint(json_str['result'])    

def delete_webhook(webhook_id):
    json_str = ss.Webhooks.delete_webhook(webhook_id)

    json_str = json.loads(str(json_str))
    pprint(json_str) 

##################
# Usage examples #
##################

# Use this to get list of all existing webhooks and their IDs
#list_webhooks()

# Pass in webhook ID to be deleted.
#delete_webhook(7815563633289092)

# To create a new webhook, first run this.  Copy the unique webhook ID and pass it
# as the parameter in the 'enable_webhook' function below.
#create_webhook('Opportunities')

#enable_webhook(8941463540131716)
