#Daily Fine Particle Matter (PM2.5) (µg/m³) in counties in USA from 2003 to 2011.
# different table for each year.
# Powered by CDC Wonder

import ssl
import urllib.request, urllib.parse, urllib.error
from urllib.parse import urljoin
from urllib.parse import urlparse
#import re
import requests
from bs4 import BeautifulSoup as bs

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

b_parameters = {
    "B_1":"D73.V2-level2", #county
    "B_2":"D73.V3", #year
    "B_3":"*None*", 
    "B_4":"*None*",
    "B_5":"*None*"
}

m_parameters = {
    "M_1":"D73.M1",
#    "M_11":"D73.M11", #number of observations
#    "M_12":"D73.M12", #range
#    "M_14":"D73.M14" #standard deviation
}

v_parameters = {
    "V_D73.V2":"Clear",
#    "V_D73.V1":"Clear",
    "V_D73.V3":"*All*", #all years
    "V_D73.V4":"*All*",#all months
    "V_D73.V8":"*All*", #all days
    "V_D73.V6":"*All*", #all days [of year]
#    "V_D73.V7":"Clear", 
    "V_D73.V10":"*All*" #all values
}

f_parameters = {
    "F_D73.V2":["*All*"], #all (The United States)
#    "F_D73.V1":["*All*"], #all (The United States)
#    "F_D73.V7":["*All*"] #all (All Dates)
}

o_parameters = {
    "O_javascript":"off",
    "O_title":"",
    "O_location":"D73.V2",
    "O_V2_fmode":"freg",
    "O_V1_fmode":"freg",
    "O_dates":"D73.V7_range",
    "O_dates_2":"D73.V8",
    "O_V7_fmode":"freg",
    "O_pm":"pm_range",
    "O_change_action-Send-Export Results":"Export Results",
    "O_show_totals":"true", #show totals for
    "O_show_zeros":"true",
    "O_precision":"2", #decimal places
    "O_timeout":"900"
}

rd_parameters = {
    "RD1_M_D73.V7":"01", #From month (Jan)
    "RD1_D_D73.V7":"01", #From day (01)
    "RD1_Y_D73.V7":"2003", #From year (2003)
    "RD2_M_D73.V7":"12", #Until month (Dec)
    "RD2_D_D73.V7":"31", #Until day (31)
    "RD2_Y_D73.V7":"2011" #Until year (2011)
}

r_parameters = {
    "R1_D73.V10":"",
    "R2_D73.V10":""
}

i_parameters = {
    "I_D73.V2":"*All* (The United States)",
    "I_D73.V1":"*All* (The United States)",
    "I_D73.V7":"*All* (All Dates)"
}

misc_parameters = {
#    "utf8":"&#x2713;",
#    "affiliate":"cdc-main",
    "action-Send": "Send",
    "finder-stage-D73.V2": "codeset",
    "finder-stage-D73.V1": "codeset",
    "finder-stage-D73.V7": "codeset",
    "stage": "request",
#    "dataset_code":"D73",
#    "dataset_label":"Fine Particulate Matter (PM2.5) (&micro;g/m&sup3;) (2003-2011)",
#    "dataset_vintage":"2011",
#    "dataset_id":"D73"
}

def createParameterList(parameterList):
#    """Helper function to create a parameter list from a dictionary object"""
    
    parameterString = ""
    
    for key in parameterList:
        parameterString += "<parameter>\n"
        parameterString += "<name>" + key + "</name>\n"
        
        if isinstance(parameterList[key], list):
            for value in parameterList[key]:
                parameterString += "<value>" + value + "</value>\n"
        else:
            parameterString += "<value>" + parameterList[key] + "</value>\n"
        
        parameterString += "</parameter>\n"
        
    return parameterString

xml_request = "<request-parameters>\n"
xml_request += createParameterList(b_parameters)
xml_request += createParameterList(m_parameters)
xml_request += createParameterList(f_parameters)
xml_request += createParameterList(i_parameters)
xml_request += createParameterList(o_parameters)
xml_request += createParameterList(rd_parameters)
xml_request += createParameterList(r_parameters)
xml_request += createParameterList(v_parameters)
xml_request += createParameterList(misc_parameters)
xml_request += "</request-parameters>"

payload = {"request_xml":xml_request, "accept_datause_restrictions":"true"}

url = "https://wonder.cdc.gov/controller/datarequest/D73"
response = requests.post(url, data=payload)
#print(response.text)
if response.status_code ==200:
    dump = response.text
else:
    print("something went wrong")

#dump = urllib.request.urlopen(response.text, context=ctx)
#clean = ET.fromstring(dump.read().decode())
#print(clean)

import xml.etree.ElementTree as ET

mess = []
tree = ET.fromstring(dump)
lst = tree.findall("./response/data-table/r/c")
for item in lst:
    mess.append(item.get('l'))
    mess.append(item.get('v'))

clean = [x for x in mess if x != None]

it = list(zip(*[iter(clean)]*19)) #iterated into groups of 19 since each county data uses 19 nodes

import sqlite3

conn = sqlite3.connect("airq3.sqlite")
cur = conn.cursor()

# future update, dont inlcude year in year tables
cur.execute('''
CREATE TABLE IF NOT EXISTS County (
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    name TEXT UNIQUE
)
''')

cur.execute('''
CREATE TABLE IF NOT EXISTS Yr03 (
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    county_id INTEGER UNIQUE,
    year INTEGER,
    value INTEGER
)
''')

cur.execute('''
CREATE TABLE IF NOT EXISTS Yr04 (
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    county_id INTEGER UNIQUE,
    year INTEGER,
    value INTEGER
)
''')

cur.execute('''
CREATE TABLE IF NOT EXISTS Yr05 (
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    county_id INTEGER UNIQUE,
    year INTEGER,
    value INTEGER
)
''')

cur.execute('''
CREATE TABLE IF NOT EXISTS Yr06 (
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    county_id INTEGER UNIQUE,
    year INTEGER,
    value INTEGER
)
''')

cur.execute('''
CREATE TABLE IF NOT EXISTS Yr07 (
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    county_id INTEGER UNIQUE,
    year INTEGER,
    value INTEGER
)
''')

cur.execute('''
CREATE TABLE IF NOT EXISTS Yr08 (
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    county_id INTEGER UNIQUE,
    year INTEGER,
    value INTEGER
)
''')

cur.execute('''
CREATE TABLE IF NOT EXISTS Yr09 (
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    county_id INTEGER UNIQUE,
    year INTEGER,
    value INTEGER
)
''')

cur.execute('''
CREATE TABLE IF NOT EXISTS Yr10 (
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    county_id INTEGER UNIQUE,
    year INTEGER,
    value INTEGER
)
''')

cur.execute('''
CREATE TABLE IF NOT EXISTS Yr11 (
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    county_id INTEGER UNIQUE,
    year INTEGER,
    value INTEGER
)
''')

for line in it:

    cur.execute('''INSERT OR IGNORE INTO County
        (name) 
        VALUES ( ? )''', 
        (line[0], ) )  
    cur.execute('SELECT id FROM County WHERE name = ? ', (line[0], ))
    county_id = cur.fetchone()[0]

    cur.execute('''INSERT OR IGNORE INTO Yr03 
        (county_id, year, value)    
        VALUES ( ?, ?, ? )''',
        (county_id, line[1], line[2]))
    
    cur.execute('''INSERT OR IGNORE INTO Yr04 
        (county_id, year, value)    
        VALUES ( ?, ?, ? )''',
        (county_id, line[3], line[4]))
    
    cur.execute('''INSERT OR IGNORE INTO Yr05 
        (county_id, year, value)    
        VALUES ( ?, ?, ? )''',
        (county_id, line[5], line[6]))

    cur.execute('''INSERT OR IGNORE INTO Yr06 
        (county_id, year, value)    
        VALUES ( ?, ?, ? )''',
        (county_id, line[7], line[8]))   

    cur.execute('''INSERT OR IGNORE INTO Yr07 
        (county_id, year, value)    
        VALUES ( ?, ?, ? )''',
        (county_id, line[9], line[10]))

    cur.execute('''INSERT OR IGNORE INTO Yr08 
        (county_id, year, value)    
        VALUES ( ?, ?, ? )''',
        (county_id, line[11], line[12]))

    cur.execute('''INSERT OR IGNORE INTO Yr09 
        (county_id, year, value)    
        VALUES ( ?, ?, ? )''',
        (county_id, line[13], line[14]))

    cur.execute('''INSERT OR IGNORE INTO Yr10 
        (county_id, year, value)    
        VALUES ( ?, ?, ? )''',
        (county_id, line[15], line[16]))

    cur.execute('''INSERT OR IGNORE INTO Yr11 
        (county_id, year, value)    
        VALUES ( ?, ?, ? )''',
        (county_id, line[17], line[18]))
    conn.commit()

conn.commit()    

print('')
print('Fine Particle Matter (PM2.5) (µg/m³) in counties in USA from 2003 to 2011')
print('powered by CDC WONDER\n')
print('For top 25 highest fine particle matter counties type " h ". \nFor top 25 lowest fine particle matter counties type " l ".\n')

# possible to remove year from printed results, since user has to choose a year and table link doesn't need to be displayed.

choice1 = input('  Type h or l : \n      ')
if choice1 == "h":
    choice = int(input('Input four digit year from 2003 to 2011:  \n      '))
    if choice == 2003:
        print("\nHighest fine particle matter in air by county for 2003")
        for row in cur.execute('''
            select County.name, Yr03.year, Yr03.value from County join Yr03 on Yr03.county_id = County.id 
            ORDER BY value DESC LIMIT 25
            '''):
            print(row)
    if choice == 2004:
        print("\nHighest fine particle matter in air by county for 2004")
        for row in cur.execute('''
            select County.name, Yr04.year, Yr04.value from County join Yr04 on Yr04.county_id = County.id 
            ORDER BY value DESC LIMIT 25
            '''):
            print(row)
    if choice == 2005:
        print("\nHighest fine particle matter in air by county for 2005")
        for row in cur.execute('''
            select County.name, Yr05.year, Yr05.value from County join Yr05 on Yr05.county_id = County.id 
            ORDER BY value DESC LIMIT 25
            '''):
            print(row)
    if choice == 2006:
        print("\nHighest fine particle matter in air by county for 2006")
        for row in cur.execute('''
            select County.name, Yr06.year, Yr06.value from County join Yr06 on Yr06.county_id = County.id 
            ORDER BY value DESC LIMIT 25
            '''):
            print(row)
    if choice == 2007:
        print("\nHighest fine particle matter in air by county for 2007")
        for row in cur.execute('''
           select County.name, Yr07.year, Yr07.value from County join Yr07 on Yr07.county_id = County.id 
           ORDER BY value DESC LIMIT 25
            '''):
            print(row)
    if choice == 2008:
        print("\nHighest fine particle matter in air by county for 2008")
        for row in cur.execute('''
            select County.name, Yr08.year, Yr08.value from County join Yr08 on Yr08.county_id = County.id 
            ORDER BY value DESC LIMIT 25
            '''):
            print(row)
    if choice == 2009:
        print("\nHighest fine particle matter in air by county for 2009")
        for row in cur.execute('''
            select County.name, Yr09.year, Yr09.value from County join Yr09 on Yr09.county_id = County.id 
            ORDER BY value DESC LIMIT 25
            '''):
            print(row)
    if choice == 2010:
        print("\nHighest fine particle matter in air by county for 2010")
        for row in cur.execute('''
            select County.name, Yr10.year, Yr10.value from County join Yr10 on Yr10.county_id = County.id 
            ORDER BY value DESC LIMIT 25
            '''):
            print(row)
    if choice == 2011:
        print("\nHighest fine particle matter in air by county for 2011")
        for row in cur.execute('''
            select County.name, Yr11.year, Yr11.value from County join Yr11 on Yr11.county_id = County.id 
            ORDER BY value DESC LIMIT 25
            '''):
            print(row)

    else: print('')
    
elif choice1 == "l":
    choice = int(input('Input four digit year from 2003 to 2011:  \n      '))
    if choice == 2003:
        print("\nLowest fine particle matter in air by county for 2003")
        for row in cur.execute('''
            select County.name, Yr03.year, Yr03.value from County join Yr03 on Yr03.county_id = County.id 
            ORDER BY value ASC LIMIT 25
            '''):
            print(row)
    if choice == 2004:
        print("\nLowest fine particle matter in air by county for 2004")
        for row in cur.execute('''
            select County.name, Yr04.year, Yr04.value from County join Yr04 on Yr04.county_id = County.id 
            ORDER BY value ASC LIMIT 25
            '''):
            print(row)
    if choice == 2005:
        print("\nLowest fine particle matter in air by county for 2005")
        for row in cur.execute('''
            select County.name, Yr05.year, Yr05.value from County join Yr05 on Yr05.county_id = County.id 
            ORDER BY value ASC LIMIT 25
            '''):
            print(row)
    if choice == 2006:
        print("\nLowest fine particle matter in air by county for 2006")
        for row in cur.execute('''
            select County.name, Yr06.year, Yr06.value from County join Yr06 on Yr06.county_id = County.id 
            ORDER BY value ASC LIMIT 25
            '''):
            print(row)
    if choice == 2007:
        print("\nLowest fine particle matter in air by county for 2007")
        for row in cur.execute('''
           select County.name, Yr07.year, Yr07.value from County join Yr07 on Yr07.county_id = County.id 
           ORDER BY value ASC LIMIT 25
            '''):
            print(row)
    if choice == 2008:
        print("\nLowest fine particle matter in air by county for 2008")
        for row in cur.execute('''
            select County.name, Yr08.year, Yr08.value from County join Yr08 on Yr08.county_id = County.id 
            ORDER BY value ASC LIMIT 25
            '''):
            print(row)
    if choice == 2009:
        print("\nLowest fine particle matter in air by county for 2009")
        for row in cur.execute('''
            select County.name, Yr09.year, Yr09.value from County join Yr09 on Yr09.county_id = County.id 
            ORDER BY value ASC LIMIT 25
            '''):
            print(row)
    if choice == 2010:
        print("\nLowest fine particle matter in air by county for 2010")
        for row in cur.execute('''
            select County.name, Yr10.year, Yr10.value from County join Yr10 on Yr10.county_id = County.id 
            ORDER BY value ASC LIMIT 25
            '''):
            print(row)
    if choice == 2011:
        print("\nLowest fine particle matter in air by county for 2011")
        for row in cur.execute('''
            select County.name, Yr11.year, Yr11.value from County join Yr11 on Yr11.county_id = County.id 
            ORDER BY value ASC LIMIT 25
            '''):
            print(row)

else: print('Invalid h or l')


cur.close()
