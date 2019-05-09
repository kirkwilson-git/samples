# This was originally created for a very specific business case and has been anonymized.  
# It is not intended to be used as a general solution, but it is a good starting point.

# This script will read a specific Smartsheet document, determine if a new record has been
# created by an associated web form, and if so send out a notification e-mail with the contents of the new record
# to the business development distribution list.

# The flow of the program is:
# 1. Connect to Smartsheet and read the entire sheet
# 2. Check for any records with the value 'Form - New Entry' in the "Record Source" column.
#    That is the default value added by the web form when a new entry is added, so that is effectively the triggering
#    even for a notification e-mail to get sent.
# 3. Update the "Record Source" column from 'Form - New Entry' to 'Form', to indicate that this record has been processed.
# 4. Construct the HTML to display the new record contents and send that e-mail, from the biz-dev DL address to the biz-dev DL address.
#    The e-mail is sent from the DL so that any replies will automatically be sent back to the entire DL.
# 5. Add any comment entered on the web form to the built-in comments functionality on the Smartsheet record.
#    This isn't possible through the Smartsheet web form, but is possible through Python and the Smartsheet API.

# Much of the Smartsheet-related code was derived from the sample at:
# https://github.com/smartsheet-samples/python-read-write-sheet/blob/master/python-read-write-sheet.py
# General Smartsheet API documentation is at: https://smartsheet-platform.github.io/api-docs/

# PREREQ: Install the smartsheet sdk with the command: pip install smartsheet-python-sdk
import smartsheet

# Python has native SMTP support, so no extra libraries are required to download.
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# Needed for the web server components
from http.server import BaseHTTPRequestHandler, HTTPServer
import socketserver

# Needed to get the current system timestamp
from datetime import datetime

# Smartsheet API access token (functions as the Smartsheet password)
smartsheet_api_token = 'xxxxxxx'

# ID of the Smartsheet sheet to read (identified by sheet properties in UI)
smartsheeet_sheet_id = 123456789

# The API identifies columns by Id, but it's more convenient to refer to column names.  This dictionary will add simplicity
column_map = {}

# Declaring the Smartsheet client variable here so it can be referenced in below functions without having to pass it every time as an argument.
smartsheet_client = None

# Helper function to find Smartsheet cell in a row based on the column name instead of ID.
def get_cell_by_column_name(row, column_name):
    # Use the value of the global variables defined at the top of this script.  Needed so these don't get defined as local variables to this function.
    global column_map
    column_id = column_map[column_name]
    return row.get_column(column_id)

# This takes a pre-formed HTML string as input, and sends it as an e-mail with the below parameters.
# All of these commands were found at the below two sites:
# https://stackoverflow.com/questions/882712/sending-html-email-using-python
# http://www.pythonforbeginners.com/code-snippets-source-code/using-python-to-send-email
def send_email(email_subject, html):
    smtp_host = 'smtp.office365.com'
    smtp_port = 587
    smtp_login = 'user@domain.com'
    smtp_pass = 'xxxxxx'
    email_from = 'dl-bizdev@domain.com'
    email_to = 'dl-bizdev@domain.com'
        
    email_message = MIMEMultipart('alternative')
    email_message['Subject'] = email_subject
    email_message['From'] = email_from
    email_message['To'] = email_to
    
    email_message.attach(MIMEText(html, 'html'))
    
    server = smtplib.SMTP(smtp_host, smtp_port)
    server.connect(smtp_host, smtp_port)
    server.ehlo()
    server.starttls()
    server.ehlo()
    server.login(smtp_login, smtp_pass)
    server.sendmail(email_from, email_to, email_message.as_string())
    server.quit()

    print('Email notification for "' + email_subject + '" sent at: ' + datetime.now().strftime('%Y-%m-%d %I:%M:%S %p'))

# Given the input parameters, this will update a single cell in Smartsheet.
# The Smartsheet API is confusing and cumbersome with regards to how it handles updates to an existing row, but this works.
def update_smartsheet_row(row_id_of_row_to_update, column_id_of_cell_to_update, new_cell_value):
    # Use the value of the global variables defined at the top of this script.  Needed so these don't get defined as local variables to this function.
    global smartsheet_client
    global smartsheeet_sheet_id
    
    updated_cell = smartsheet_client.models.Cell()
    updated_cell.column_id = column_id_of_cell_to_update
    updated_cell.value = new_cell_value

    updated_row = smartsheet_client.models.Row()
    updated_row.id = row_id_of_row_to_update
    updated_row.cells.append(updated_cell)

    smartsheet_client.Sheets.update_rows(smartsheeet_sheet_id, updated_row)

# Given the parameterized row and column name, get the column value from the row and output some pre-formed HTML that will add a new row in a table
# in the notification e-mail.
def construct_html_table_row(smartsheet_row, smartsheet_column_name, display_column_name, alternate_background_color):
    # Need to use the "display_value" attribute for the "Value" field so that Smartsheet will automatically format it as a currency.
    # For all others, use the "value" attribute.  The date values specifically don't show up with "display_value", so we can't use it for everything.
    if smartsheet_column_name == 'Value':
        cell_value = str(get_cell_by_column_name(smartsheet_row, smartsheet_column_name).display_value)
    else:
        cell_value = str(get_cell_by_column_name(smartsheet_row, smartsheet_column_name).value)

    # 'None' is Smartsheet's NULL-equivalent in the display_value attribute.  Just show nothing in the notification e-mail rather than 'None'.
    if cell_value == 'None':
        cell_value = ''

    background_color = 'ffffff'
    if alternate_background_color:
        background_color = 'f2f2f2'
    
    html = '\n<tr style="background-color:#' + background_color + ';"><td width="50" style="border: 1px solid #ddd;padding: 8;"><div style="font-size: 110%;">' + display_column_name
    html = html + '</div></td><td width="250" style="border: 1px solid #ddd;padding: 8;"><div style="font-size: 110%;">' + cell_value + '</div></td></tr>'
    
    return html

# This handles all of the HTML-related work.  It's broken down into three sections:
# 1. Top part of the e-mail, which is just a green line to make things look pretty and beginning the table that has the actual values.
#    Note that the green line is just an empty table with one row and column, formatted to have the company's green as its background.
# 2. Call the "construct_html_table_row" function for each column in Smartsheet we want to include in the notification e-mail.
# 3. Bottom part of the e-mail, which is just closing the table tag, another green line for formatting, a few line breaks, and the company logo.
def construct_html(row):
    # Top part of the e-mail (all static HTML that was tested first outside of this script for formattting).
    # Note that triple-quotes allow you to create multi-line strings easily in Python.
    html = """\n
    <table style="font-family: Calibri, sans-serif;width: 85%;margin-left: auto;margin-right: auto;">
      <tr>
        <td style="border-bottom: 8px solid #A1CC3A;">&nbsp;</td>
      </tr>
    </table>
    <br>
    <table style="font-family: Calibri, sans-serif;width: 85%;margin-left: auto;margin-right: auto; border-collapse: collapse;">
    """

    # Build the individual table rows for the notification e-mail.
    html = html + construct_html_table_row(row, 'Owner', 'Submitted By', False)
    html = html + construct_html_table_row(row, 'Project Name', 'Project Name', True)
    html = html + construct_html_table_row(row, 'Client', 'Client', False)
    html = html + construct_html_table_row(row, 'Comments', 'Comments', True)
    html = html + construct_html_table_row(row, 'Address', 'Address', False)
    html = html + construct_html_table_row(row, 'Market', 'Market', True)
    html = html + construct_html_table_row(row, 'Value', 'Value', False)
    html = html + construct_html_table_row(row, 'Precon Start Date', 'Precon Start Date', True)
    html = html + construct_html_table_row(row, 'Construction Start Date', 'Construction Start Date', False)
    html = html + construct_html_table_row(row, 'Const Completion Date', 'Completion Date', True)
    html = html + construct_html_table_row(row, 'Client Contact', 'Client Contact', False)
    html = html + construct_html_table_row(row, 'Designer', 'Designer', True)
     
    # Bottom part of the e-mail.  Just more static HTML for formatting.
    html = html + """
    </table>
    <table style="font-family: Calibri, sans-serif;width: 85%;margin-left: auto;margin-right: auto;">
      <tr>
        <td style="border-bottom: 8px solid #A1CC3A;">&nbsp;</td>
      </tr>
    </table>
    <br>
    <br>
    <br>
    <img src="https://www.domain.com/logo.png" alt="Logo" height="35">"""
        
    return html

# For the parameterized row, take the value from the "Comments" column and add them to the built-in comments feature on the Smartsheet row.
# This isn't a feature that the Smartsheet web form can handle, and so the comments are stored in a regular column just like everything else.
# This copies the text from that column and adds it as a new comment on the row.
def add_comment_to_row(row):
    comment_text = str(get_cell_by_column_name(row, 'Comments').display_value)

    # Only add the comment if there was something entered.
    if comment_text != 'None':
        global smartsheeet_sheet_id
        smartsheet_client.Discussions.create_discussion_on_row(smartsheeet_sheet_id, row.id, smartsheet_client.models.Discussion({'comment': smartsheet_client.models.Comment({'text': comment_text}) }) )

def check_sheet_for_new_row():
    # Use the value of the global variables defined at the top of this script.  Needed so these don't get defined as local variables to this function.
    global smartsheet_client
    
    # Initialize Smartsheet client
    smartsheet_client = smartsheet.Smartsheet(smartsheet_api_token)

    # Make sure we don't miss any error
    smartsheet_client.errors_as_exceptions(True)

    # Load entire sheet
    sheet = smartsheet_client.Sheets.get_sheet(smartsheeet_sheet_id)

    # Build column map for later reference - translates column names to column id
    for column in sheet.columns:
        column_map[column.title] = column.id

    # This is effectively the starting point of this entire script.  Loop through every row in Smartsheet.
    # For each row, get the value of the "Record Source" column and check to see if it has a value of "Form - New Entry".
    # If it does, that's the indication that the current record is new and a notification has not yet been sent out.
    # In that case, call a function that will send out the notificaiton e-mail and also update the value of the "Record Source"
    # column to "Form" so that another notification e-mail won't be sent out.  
    for row in sheet.rows:
        source_cell = get_cell_by_column_name(row, 'Record Source')

        # Check if the current row is a new one that hasn't yet had a notification e-mail sent out.
        if source_cell.value == 'Form - New Entry':
            
            # Construct the e-mail subject line
            subject_cell = get_cell_by_column_name(row, 'Project Name')
            email_subject = 'Project Opportunity: ' + str(subject_cell.value)

            # Update the "Record Source" column for the current row, changing its value from "Form - New Entry" to "Form" to indicate that
            # this row has been processed and a notification has been sent out.
            # Do this update before actually sending the e-mail notification (which is the next step below) just in case something
            # goes wrong with forming the HTML or sending the notification.  Better to have no notification than multiple notifications.
            update_smartsheet_row(row.id, source_cell.column_id, 'Form')

            # Send the notification e-mail, while also calling the "construct_html" function which does the bulk of the prep work.
            send_email(email_subject, construct_html(row))

            # Finally, take any comments entered on the form and add them to the built-in comments feature on the Smartsheet row.
            # This isn't a feature that the Smartsheet form can handle, and so the comments are stored in a regular column just like everything else.
            # This copies the text from that column and adds it as a new comment on the row.
            add_comment_to_row(row)

# The main web server class to process incoming Smartsheet notifications.
# This runs as an infinite loop, always waiting for incoming GET or POST messages.
class S(BaseHTTPRequestHandler):
    # Respond with a HTTP 200 SUCCESS message in the header.
    def _set_headers(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()

    def do_GET(self):
        # Print a simple message if someone tries to access this via a web browser.
        self._set_headers()
        self.wfile.write("<html><body><h1>Permision denied.</h1></body></html>".encode())

    def do_HEAD(self):
        self._set_headers()

    # Note that in Python, prefixing an object name (such as a function like this) makes the
    # object private within the module (private to the class in this case).
    def _process_challenge_request(self):
        # Documented at http://smartsheet-platform.github.io/api-docs/#webhook-verification
        # After a webhook is created, Smartsheet will assign it a status of "NEW_NOT_VERIFIED".
        # To enable the webhook, a "update_webhook" command must be issued through the API to
        # enable it.  After that command is issued, Smartsheet will respond with a unique
        # challenge response, which is a string like "1a2a1447-a24c-45b2-941f-9880704e914e".
        # To successfully enable the webhook, the listening app (this server) must accept
        # that challenge message, parse it and send it back to Smartsheet along with a HTTP 200
        # status code.
        
        # IMPORTANT: Smartsheet will redo this validation process after every 100 callbacks.
        # This same response process must be repeated each time or the webhook will be disabled.

        # Format the JSON response message.  The response format is:
        # {
        #  "smartsheetHookResponse": "d78dd1d3-01ce-4481-81de-92b4f3aa5ab1"
        # }
        
        # The type of "self.headers" is documented at:
        # https://docs.python.org/3.4/library/email.message.html#email.message.Message
        # View the entire header with: print(self.headers.as_string())
        challenge_response = '{\r\n'
        challenge_response = challenge_response + '  "smartsheetHookResponse": "'
        challenge_response = challenge_response + self.headers.get('Smartsheet-Hook-Challenge')
        challenge_response = challenge_response + '"'
        challenge_response = challenge_response + '\r\n}'

        # Send the challenge response back to the originating Smartsheet server
        self.wfile.write(challenge_response.encode())
        
    def do_POST(self):
        # This will send the HTTP 200 response
        self._set_headers()
        header = self.headers.as_string()

        # Check if this is a challenge, if so handle accordingly
        if header.startswith('Smartsheet-Hook-Challenge'):
            self._process_challenge_request()
        # Make sure the message is still from Smartsheet, and if so then this
        # is just a notification that something has changed.  Call the below
        # function to check to determine if a new row was added by the web form and if
        # so, send out the notification e-mail.
        elif header.startswith('Smartsheet'):
            print('Webhook notification received at: ' + datetime.now().strftime('%Y-%m-%d %I:%M:%S %p')) 
            check_sheet_for_new_row()


        # Use this for debugging if needed.
        #print('HEADER')
        #print(header)

        #print('BODY')
        #content_length = int(self.headers.get('content-length', 0))
        #body = self.rfile.read(content_length)
        #print(body.decode("utf-8"))


# Initialize the web server once the Python script is started.        
def run(server_class=HTTPServer, handler_class=S, port=80):
    server_address = ('', port)
    httpd = server_class(server_address, handler_class)
    print ('Web server listening on port ' + str(port) + ' for Smartsheet webhook messages')
    httpd.serve_forever()

if __name__ == "__main__":
    from sys import argv

    if len(argv) == 2:
        run(port=int(argv[1]))
    else:
        run()
