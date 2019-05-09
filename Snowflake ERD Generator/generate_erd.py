# This script requires installation of graphviz (graphviz.org).
# Generate an ER diagram based on object naming conventions from a Snowflake database.
# Note that graphviz doesn't give fine control over the layout of the generated diagram, so this is far from an optimal solution.
# This also is intended to be used as an example of what's possible, not as a copy-and-paste solution since it uses logic
# that is very focused on specific naming standards being consistently followed within the Snowflake database.
# This was also created for internal use, so it does not follow a number of best practices and there are some shortcuts taken...

import snowflake.connector
import os

# Establish the Snowflake connection.  Note that this is a quick-and-dirty way to connect and not best practice.  See the Snowflake documentation
# for more secure options.
snowflake_connection = snowflake.connector.connect(
user='xxxxx',
password='xxxxx',
account='xxxxxx'
)

cursor = snowflake_connection.cursor()

# Used for preventing duplicates being written (this is somewhat explained below in the code)
entities = []

erd_file = open('erd','w')

# This function is used simply to avoid having to add the newline character at the end of every line
def file_write(text):
    erd_file.write(text + '\n')

def write_entity(table_name):
    global entities

    entity_color = ''

    # The output table will have a different color depending on the table type
    if table_name[-3:] == 'DIM':
        entity_color = '#d0e0d0' # light green
    elif table_name[-5:] == 'DFACT':
        entity_color = '#ececfc' # # light blue
    else:
        entity_color = '#fcecec' # light red
        
    query = """SELECT COLUMN_NAME, DATA_TYPE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = '""" + table_name + """'
                ORDER BY ORDINAL_POSITION;"""
    
    cursor.execute(query)
    result_set = cursor.fetchall()

    # Write the table header details to the file
    file_write(table_name)
    file_write('[label=<')
    file_write('\t<table border="0" cellborder="1" cellspacing="0" cellpadding="3">')
    file_write('\t\t<tr><td bgcolor="' + entity_color + '"><font face="helvetica"><b>' + table_name + '</b></font></td></tr>')
    
    # Write the details for each table column to the file
    for record in result_set:
        file_write('\t\t<tr><td align="left" bgcolor="' + entity_color + '" port="' + str(record[0]) + '"><font face="helvetica">' + str(record[0]) + ': ' + str(record[1]) + '</font></td></tr>')

    # Close the table
    file_write('\t</table>')
    file_write('>]')
    file_write('')

    # Mark this table as being written to the file to avoid duplicates
    entities.append(table_name)

def write_relationship(table_1, column_1, table_2, column_2):
    file_write(table_1 + ':' + column_1 + ' -> ' + table_2 + ':' + column_2 + ' [dir="both"];')
    
try:
    file_write('digraph G {')

    # Write the ERD title
    file_write('fontsize=40')
    file_write('fontname = "helvetica-bold"')
    file_write('label = "Snowflake ERD"')
    file_write('labelloc = "t"') # Put the title at the top instead of the default bottom
    file_write('splines = polyline')
    file_write('')

    # Set some layout defaults
    file_write('graph [pad="0.5", nodesep="0.5", ranksep="3"]')
    file_write('node [shape=none, margin=0]')
    file_write('rankdir=TB') # Diagram will be written top-to-bottom, but because of the nature of the model it will actually be more left-to-right oriented
    file_write('')

    # Modify as needed
    cursor.execute('USE WAREHOUSE ETL_WH')
    cursor.execute('USE DATABASE PROD_DB')
    
    # This is extremely custom, assuming consistent use of naming conventions for database objects.  Ideally this would be driven by a metadata table.
    cursor.execute("""-- Join logic is currently completely based on naming conventions.  Logic is applied in this order:
                        -- 1. REPLACE %_KEY WITH %_DIM AND SEE IF THERE IS CORRESPONDING TABLE
                        -- 2. REPLACE %_KEY WITH %_DFACT AND SEE IF THERE IS CORRESPONDING TABLE
                        -- 3. IF COLUMN LIKE '%MONTH%', JOIN TO MONTH_DIM
                        -- 4. IF COLUMN LIKE '%DATE%, JOIN TO DATE_DIM
                        SELECT  
                        CL.TABLE_NAME LEFT_TABLE, CL.COLUMN_NAME LEFT_COLUMN, TR.TABLE_NAME RIGHT_TABLE, 'PKEY' RIGHT_COLUMN
                        FROM INFORMATION_SCHEMA.COLUMNS CL, INFORMATION_SCHEMA.TABLES TL, 
                        INFORMATION_SCHEMA.COLUMNS CR, INFORMATION_SCHEMA.TABLES TR 
                        WHERE CL.TABLE_NAME = TL.TABLE_NAME
                        AND TL.TABLE_SCHEMA = 'PUBLIC'
                        AND CL.COLUMN_NAME LIKE '%KEY'
                        AND CL.COLUMN_NAME NOT IN ('MD_NATURAL_KEY', 'PKEY')
                        AND CR.TABLE_NAME = TR.TABLE_NAME
                        AND TR.TABLE_SCHEMA = 'PUBLIC'
                        AND TR.TABLE_NAME = SUBSTR(CL.COLUMN_NAME, 0, LENGTH(CL.COLUMN_NAME) - 4) || '_DIM'
                        UNION
                        SELECT  
                        CL.TABLE_NAME, CL.COLUMN_NAME, TR.TABLE_NAME, 'PKEY'
                        FROM INFORMATION_SCHEMA.COLUMNS CL, INFORMATION_SCHEMA.TABLES TL, 
                        INFORMATION_SCHEMA.COLUMNS CR, INFORMATION_SCHEMA.TABLES TR 
                        WHERE CL.TABLE_NAME = TL.TABLE_NAME
                        AND TL.TABLE_SCHEMA = 'PUBLIC'
                        AND CL.COLUMN_NAME LIKE '%KEY'
                        AND CL.COLUMN_NAME NOT IN ('MD_NATURAL_KEY', 'PKEY')
                        AND CR.TABLE_NAME = TR.TABLE_NAME
                        AND TR.TABLE_SCHEMA = 'PUBLIC'
                        AND TR.TABLE_NAME = SUBSTR(CL.COLUMN_NAME, 0, LENGTH(CL.COLUMN_NAME) - 4) || '_DFACT'
                        UNION
                        SELECT C.TABLE_NAME, C.COLUMN_NAME, 'MONTH_DIM_VIEW', 'PKEY'
                        FROM INFORMATION_SCHEMA.COLUMNS C, INFORMATION_SCHEMA.TABLES T
                        WHERE C.TABLE_NAME = T.TABLE_NAME
                        AND T.TABLE_SCHEMA = 'PUBLIC'
                        AND C.COLUMN_NAME LIKE '%MONTH%KEY'
                        AND C.COLUMN_NAME NOT IN ('MD_NATURAL_KEY', 'PKEY')
                        UNION 
                        SELECT C.TABLE_NAME, C.COLUMN_NAME, 'DATE_DIM', 'PKEY'
                        FROM INFORMATION_SCHEMA.COLUMNS C, INFORMATION_SCHEMA.TABLES T
                        WHERE C.TABLE_NAME = T.TABLE_NAME
                        AND T.TABLE_SCHEMA = 'PUBLIC'
                        AND C.COLUMN_NAME LIKE '%DATE%KEY'
                        AND C.COLUMN_NAME NOT LIKE '%MONTH%'
                        AND C.COLUMN_NAME NOT IN ('MD_NATURAL_KEY', 'PKEY', 'DATE_KEY')
                        ORDER BY 3, 1;""")

    result_set = cursor.fetchall()

    # Output the entities (tables) by looping through both the left table and right table.
    # The 'entities' list is used to contain each table that has already been processed so that duplicates aren't written to the file
    for record in result_set:
        if str(record[0]) not in entities:
            write_entity(str(record[0]))
        
        if str(record[2]) not in entities:
            write_entity(str(record[2]))

    # Write all of the relationships after all of the entities have been written to the file
    for record in result_set:
        write_relationship(str(record[0]), str(record[1]), str(record[2]), str(record[3]))

    file_write('}')
    erd_file.close()

    # Create the output PDF
    os.system(r'dot -Tpdf -O "' + os.getcwd() + '\erd"')

    # Delete the temporary dot file
    os.remove(os.getcwd() + '\erd')

finally:
    cursor.close()
    
