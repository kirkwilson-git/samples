'''
Author: Kirk Wilson

Sample Python script showing how to access the Viewpoint Field View SOAP API (and APIs in general).  This was used as part of a much larger
solution (in Python) that contained business-specific logic.  That is not included here.

This module contains all components required to access the Field View API and return results.
It does not parse the output (other than stripping XML tags) or contain any business logic.
The 'call_api' function is generic and directly responsible for accessing the API, while the other functions
are used for calling the 'call_api' function for specific requests.
Field View API is documented at https://fvdocs.viewpoint.com/Admin_web_topics/APIs/c_APIs.html

The Field View API relies on SOAP calls for access.  To manually make a call and just to learn more about SOAP, I recommend using SoapUI (soapui.org).
   Click the SOAP icon on the toolbar to create a new SOAP project
   For Initial WSDL, enter one of the following depending on which API you want to access:
			https://us.fieldview.viewpoint.com/FieldViewWebServices/WebServices/JSON/API_ConfigurationServices.asmx?WSDL
			https://us.fieldview.viewpoint.com/FieldViewWebServices/WebServices/JSON/API_FormsServices.asmx?WSDL
			https://us.fieldview.viewpoint.com/FieldViewWebServices/WebServices/JSON/API_TasksServices.asmx?WSDL
			https://us.fieldview.viewpoint.com/FieldViewWebServices/WebServices/JSON/API_ProcessServices.asmx?WSDL
   Enter a project name, leave all other defaults.  You'll then see all of the available API calls listed in the tree on the left, broken into two separate sections for SOAP 1.1 and 1.2 (I always used 1.2 - not sure what the difference is)
   Choose what function to call, and then double-click on the default "Request 1" under it.  You'll then see the XML template for the SOAP call.  Fill in the appropriate values and execute the call to see the XML/JSON results.
   NOTE: To pass in NIL values, you also have to add xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" to the envelope header (SoapUI doesn't do this automatically).  
	Example API request:
			<soap:Envelope xmlns:soap="http://www.w3.org/2003/05/soap-envelope" xmlns:json="https://localhost.priority1.uk.net/Priority1WebServices/JSON" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
			   <soap:Header/>
			   <soap:Body>
			      <json:GetProjectTasksList>
			         <json:apiToken>2CAF5F78A5E5CD6FA2EA32CEDEA1FBC7FB1313</json:apiToken>
			         <json:projectId>5414</json:projectId>
			         <!--Optional:-->
			         <json:taskTypeLinkIds>
			            <!--Zero or more repetitions:-->
			            <json xsi:nil="true"/>
			         </json:taskTypeLinkIds>
			         <json:createdDateFrom xsi:nil="true"/>
			         <json:createdDateTo xsi:nil="true"/>
			         <json:statusChangedDateFrom xsi:nil="true"/>
			         <json:statusChangedDateTo xsi:nil="true"/>
			         <json:lastmodifiedDateFrom>2018-01-12Z</json:lastmodifiedDateFrom>
			         <json:lastmodifiedDateTo>2018-01-24Z</json:lastmodifiedDateTo>
			      </json:GetProjectTasksList>
			   </soap:Body>
			</soap:Envelope>
	Request result:
			<soap:Envelope xmlns:soap="http://www.w3.org/2003/05/soap-envelope" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema">
			   <soap:Body>
			      <GetProjectTasksListResponse xmlns="https://localhost.priority1.uk.net/Priority1WebServices/JSON">
			         <GetProjectTasksListResult>{"ProjectTasksListInformation":[{"TaskID":"T24314.59","TaskTypeLinkID":5516,"Location":"Interior>Level 09>Units>T03 (Partial Split)>Apartment General","Description":"BMP Unacceptable","Status":"Sellen Review","StatusColour":"#33ffff","StatusDate":"2018-01-23T23:47:03","IssuedDate":"2018-01-23T23:39:29","TaskType":"(NCR) Non-Conformance","IssuedToPackageID":104865,"IssuedToPackage":"Painting","IssuedToOrganisation":"Aaron's Biz Unit","TargetDate":"2018-01-30T08:00:00","IssuedByUser":"Robert Achilles","IssuedByOrganisation":"Sellen Construction Co, Inc","IssuedByOrganisationType":"General Contractor","Priority":"High","Cause":"4 - Quality of Workmanship","CausedByOrganisation":"Sellen Construction Co, Inc","ActualFinishDate":null,"Cost":0.0,"OverDue":false,"Complete":false,"Closed":true,"Resolution":"Correct ASAP","LastModified":"2018-01-23T23:47:03"}],"Status":{"Code":2,"Message":"Success. [ActivityId]: 14144a49-ad58-40f5-87f0-fbdee5a810d9"}}</GetProjectTasksListResult>
			      </GetProjectTasksListResponse>
			   </soap:Body>
			</soap:Envelope>
'''

# NOTE - the 'requests' module is not native to Python.  Install it via the command line with 'pip install requests'
import requests # Needed for SOAP request 
import xml.etree.ElementTree # Needed to strip XML tags from SOAP response
import json # Needed to parse JSON output
import time # Needed to allow a sleep functionality (in cases where API call quota is exceeded)

# API Tokens in Field View can be managed at:
# FV Classic > Business Setup > Security > API Tokens
API_TOKEN = 'XXXXXX'

# This function is ultimately responsible for making all Field View API requests.
# It will return the JSON results from the API call.
#
# The 'api' parameter will be one of the following values:
#   API_ConfigurationServices
#   API_FormsServices
#   API_TasksServices
#   API_ProcessServices
# These correspond to the currently available North America API URLs documented at:
# http://mcsforum.info/wiki/index.php?title=NAAPIURLs
#
# The 'request_body' parameter is all text in the API request included in the <soap:Body> tag.
# The 'response_header' indicates the header in the API response that contains the JSON data to be returned.
def call_api(api, request_body, response_header):
        api_url = 'https://us.fieldview.viewpoint.com/FieldViewWebServices/WebServices/JSON/' + api + '.asmx?WSDL'
        headers = {'content-type': 'text/xml; charset=utf-8'}

        # Header of the API request body (declaring the SOAP envelope and body)
        body = """<soap:Envelope xmlns:soap="http://www.w3.org/2003/05/soap-envelope" xmlns:json="https://localhost.priority1.uk.net/Priority1WebServices/JSON" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                   <soap:Header/>
                   <soap:Body>"""

        # The calling function will handle this
        body += request_body

        # Footer of the API request body (closing the SOAP body and envelope)
        body += """</soap:Body>
                </soap:Envelope>"""

        # Issue the API call to Field View
        response = requests.post(api_url, data=body, headers=headers)

        # Strip out XML tags from the API response to get to the raw JSON string
        tree = xml.etree.ElementTree.fromstring(response.content)
        json_text = xml.etree.ElementTree.tostring(tree, encoding='utf8', method='text').decode("utf-8")

        # Load the result into an internal JSON-specific structure
        parsed_json = json.loads(json_text)

        # Make sure the API call worked as expected.
        # Note that the API status codes are documented at https://fvdocs.viewpoint.com/Admin_web_topics/APIs/r_API_Status_Codes.html
        if parsed_json['Status']['Code'] == 2: # Status code 2 = SUCCESS
                return parsed_json[response_header]
        elif parsed_json['Status']['Code'] == 11: # Status code 11 = QUOTA_EXCEEDED
                sleep_duration = 30 # This is in seconds

                print("Quota exceeded for API call '" + api + '.' + str.replace(request_body[request_body.find('<')+1:request_body.find('>')], 'json:', '') + "'")
                print('Waiting ' + str(sleep_duration) + ' seconds before retrying API call...')
                
                # Wait some time (defined in seconds) and try again.
                time.sleep(sleep_duration)

                print ('Resuming script execution...')

                # Recurse this function and hopefully the quota limit will be reset
                return call_api(api, request_body, response_header)
        else:
                # There was some unexpected error with the API call.
                raise ValueError('The Field View API call resulted in the error: \n' + str(parsed_json['Status']['Code']) + ' - ' + parsed_json['Status']['Message'])

# A Field View business unit is the top level of the hierarchy (i.e.: Business Unit > Project > Task).
def get_business_units():
        global API_TOKEN
        
        body = """<json:GetBusinessUnits>
                         <!--Optional:-->
                         <json:apiToken>""" + API_TOKEN + """</json:apiToken>
                      </json:GetBusinessUnits>"""

        return call_api('API_ConfigurationServices', body, 'BusinessUnitInformation')

# Get all projects and their IDs, which are needed for most other API calls.
# Note that the documentation states that the businessUnitIDs parameter is optional,
# but testing indicates that is not true.  Passing in a NULL value for it will
# return a SUCCESS message from the API but empty results.
def get_projects(business_unit_id):
        global API_TOKEN
        
        body = """<json:GetProjects>
                     <!--Optional:-->
                     <json:apiToken>""" + API_TOKEN + """</json:apiToken>
                     <!--Optional:-->
                     <json:projectName></json:projectName>
                     <!--Optional:-->
                     <json:businessUnitIDs>
                        <!--Zero or more repetitions:-->
                        <json:int>""" + business_unit_id + """</json:int> 
                     </json:businessUnitIDs>
                     <json:activeOnly>0</json:activeOnly>
                     <json:startRow>0</json:startRow>
                     <json:pageSize>1000</json:pageSize>
                  </json:GetProjects>"""

        return call_api('API_ConfigurationServices', body, 'ProjectInformation')

# The input date parameters are strings, formatted as YYYY-MM-DD
# Note that the Field View API only allows a date range of up to three months.
# Also note that at least one type of date range (Created Date, Status Changed Date,
# or Last Modified Date) must be provided or the API will return an error message.
def get_tasks(project_id, date_from, date_to):
        global API_TOKEN

        body = """<json:GetProjectTasksList>
                     <json:apiToken>""" + API_TOKEN + """</json:apiToken>
                     <json:projectId>""" + project_id + """</json:projectId>
                     <!--Optional:-->
                     <json:taskTypeLinkIds>
                        <!--Zero or more repetitions:-->
                        <json xsi:nil="true"/>
                     </json:taskTypeLinkIds>
                     <json:createdDateFrom xsi:nil="true"/>
                     <json:createdDateTo xsi:nil="true"/>
                     <json:statusChangedDateFrom xsi:nil="true"/>
                     <json:statusChangedDateTo xsi:nil="true"/>
                     <json:lastmodifiedDateFrom>""" + date_from + """Z</json:lastmodifiedDateFrom>
                     <json:lastmodifiedDateTo>""" + date_to + """Z</json:lastmodifiedDateTo>
                  </json:GetProjectTasksList>"""

        return call_api('API_TasksServices', body, 'ProjectTasksListInformation')              

# In Field View, the division information is stored separately as a "trade" associated with a "package".
# A task is associated with a package, which in turn is associated with a trade.  This function
# handles that package to trade (division) lookup.
def get_division(package_id):
        global API_TOKEN

        body = """<json:GetPackageTrades>
                         <!--Optional:-->
                         <json:apiToken>""" + API_TOKEN + """</json:apiToken>
                         <json:packageID>""" + package_id + """</json:packageID>
                      </json:GetPackageTrades>"""

        return call_api('API_ConfigurationServices', body, 'PackageTradeInformation')

# Effectively the same as get_tasks (with the same limitations), except for forms associated with a project instead of tasks.
def get_forms(project_id, date_from, date_to):
        global API_TOKEN
        
        body = """<json:GetProjectFormsList>
                 <!--Optional:-->
                 <json:apiToken>""" + API_TOKEN + """</json:apiToken>
                 <json:projectId>""" + project_id + """</json:projectId>
                 <!--Optional:-->
                 <json:formTemplateLinkIds>
                    <!--Zero or more repetitions:-->
                    <json xsi:nil="true"/>
                 </json:formTemplateLinkIds>
                 <json:createdDateFrom xsi:nil="true"/>
                 <json:createdDateTo xsi:nil="true"/>
                 <json:statusChangedDateFrom xsi:nil="true"/>
                 <json:statusChangedDateTo xsi:nil="true"/>
                 <json:lastmodifiedDateFrom>""" + date_from + """Z</json:lastmodifiedDateFrom>
                 <json:lastmodifiedDateTo>""" + date_to + """Z</json:lastmodifiedDateTo>
              </json:GetProjectFormsList>"""

        return call_api('API_FormsServices', body, 'ProjectFormsListInformation')

# Given a specific form, this will return all results for it, such as has a question been answered, what the answer was, question type, etc.
def get_form_results(form_id):
        global API_TOKEN

        body = """<json:GetForm>
                 <!--Optional:-->
                 <json:apiToken>""" + API_TOKEN + """</json:apiToken>
                 <!--Optional:-->
                 <json:formId>""" + form_id + """</json:formId>
              </json:GetForm>"""


        return call_api('API_FormsServices', body, 'FormInformation')        



