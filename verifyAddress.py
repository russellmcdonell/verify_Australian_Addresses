#!/usr/bin/env python

# pylint: disable=line-too-long, invalid-name, pointless-string-statement, broad-exception-caught, attribute-defined-outside-init
# pylint: disable=unused-argument, arguments-differ, unused-variable, unnecessary-pass, global-statement, missing-function-docstring
# pylint: disable=missing-class-docstring, too-many-lines

'''
A script to test if an address is plausible, or questionable.
The address can be free text, so we need to look for the normalized parts.
[The address is a dictionary, which can have 'state', 'postcode' and 'suburb', but must have one or more 'addressLines' of data]

This script uses the G-NAF (Geocoded National Address Files) which can be loaded into a MySQL database or read as CSV files.
This script also uses a file of Australia Post suburb, postcode, state data. That data has to be merged with the Australian
Bureau of Statistics SA1 and LGA data; namely the boundaries for SA1 and LGA areas. A pre-processing script assigns SA1 and LGA
codes to each suburb, postcode, state combination.

The core concept here is a locality (a.k.a suburb). States and postcode have one or more localities, although a locality can cross
state and/or postcode boundaries. Localities contain streets which can contain addresses.
A street can cross locality borders, but in G-NAF an address must have a house number, a street and locality.



SYNOPSIS
$ python verifyAddress.py [-I inputDir|--inputDir=inputDir] [-O outputDir|--outputDir=outputDir]
                         [-C configDir|--configDir=configDir] [-c configFile|--configFile=configFile]
                         [-H|--hasHeading] [-m headingsMappingFile|--headingsMappingFile=headingsMappingFile]
                         [-S|--verifyAddressService] [-P verifyAddressPort|--verifyAddressPort=verifyAddressPort]
                         [-G GNAFdir|--GNAFdir=GNAFdir] [-A ABSdir|--ABSdir=ABSdir]
                         [-F dataFilesDirectory|--DataFilesDirectory=dataFilesDirectory]
                         [-N|--NTpostcodes] [-R|--region]
                         [-D DatabaseType|--DatabaseType=DatabaseType]
                         [-s server|--server=server]
                         [-u username|--username=username] [-p password|--password=password]
                         [-d databaseName|--databaseName=databaseName]
                         [-x|--addExtras] [-W|configWeights]
                         [-a|--abbreviate] [-b|--returnBoth] [-|filename]...
                         [-v loggingLevel|--verbose=logingLevel] [-L logDir|--logDir=logDir] [-l logfile|--logfile=logfile]

REQUIRED


OPTIONS
-I inputDir|--inputDir=inputDir
The directory where the input file will be found (default='.')

-O outputDir|--outputDir=outputDir
The directory where the output file will be written (default='.')

-C configDir|--configDir=configDir
The directory where the configuration files can be found (default='.')

-c configFile|--configFile=configFile
The configuration file (default=verifyAddress.json)

-H|--hasHeading
Files of addresses to be verified are CSV files and have a heading line
(mapping of heading to data items is defined in the headingsMappingFile)
Output file will also have headings.

-m headingsMappingFile|--headingsMappingFile=headingsMappingFile
The name of headings mapping file (must be in the configuration directory(default='headings.json')

-S|--verifyAddressService
Run verifyAddress as a service (default=False)

-P verifyAddressPort|--verifyAddressPort=verifyAddressPort
The port for the verifyAddress service (default=8088)

-G GNAFdir|--GNAFdir=GNAFdir
Use the standard G-NAF psv files from this folder

-A ABSdir|--GNAFdir=GNAFdir
The directory where the standard ABS csv files will be found (default='./G-NAF')

-F dataFilesDirectory|--DataFilesDirectory=dataFilesDirectory
The directory containing the compact data files (default='./data')

-N|--NTpostcodes
Assume that 8dd is an NT postcode of 08dd

-R|--region
Assume Australian region (State/Territory) if no state/territory supplied, but a unique suburb found

-D DatabaseType|--DatabaseType=DatabaseType
The type of database [choice:MSSQL/MySQL]

-s server|--server=server]
The address of the database server

-u userName|--userName=userName]
The user name require to access the database

-p password|--userName=userName]
The user password require to access the database

-d databaseName|--databaseName=databaseName]
The name of the database

-x|-addExtras
Use additional flat text, level text, trims

-W|configWeights
Use suburb/state weights and fuzz levels from the config file

-a|--abbreviate
Return abbreviated street types

-b|--returnBoth
Return both full and abbreviated street types

-v loggingLevel|--verbose=loggingLevel
Set the level of logging that you want (defaut INFO).

-L logDir
The directory where the log file will be written (default='.')

-l logfile|--logfile=logfile
The name of a logging file where you want all messages captured (default=None)


This script receives and processes a string of text (called an 'Address').
'''

# Import all the modules that make life easy
import sys
import os
import argparse
import logging
import csv
import json
import collections
import re
import copy
import threading
import socketserver
from urllib.parse import parse_qs
from http.server import BaseHTTPRequestHandler, HTTPServer
import jellyfish
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError
from sqlalchemy_utils import database_exists



class VerifyData:
    '''
The Verify Address Data - required for threading
    '''

    def __init__(self, thisProgName):

        self.logfmt = thisProgName + ' [%(asctime)s]: %(message)s'
        self.formatter = logging.Formatter(fmt=self.logfmt, datefmt='%d/%m/%y %H:%M:%S %p')
        self.result = {}            # The structured result



# This next section is plagurised from /usr/include/sysexits.h
EX_OK = 0           # successful termination
EX_WARN = 1         # non-fatal termination with warnings

EX_USAGE = 64        # command line usage error
EX_DATAERR = 65      # data format error
EX_NOINPUT = 66      # cannot open input
EX_NOUSER = 67       # addressee unknown
EX_NOHOST = 68       # host name unknown
EX_UNAVAILABLE = 69  # service unavailable
EX_SOFTWARE = 70     # internal software error
EX_OSERR = 71        # system error (e.g., can't fork)
EX_OSFILE = 72       # critical OS file missing
EX_CANTCREAT = 73    # can't create (user) output file
EX_IOERR = 74        # input/output error
EX_TEMPFAIL = 75     # temp failure; user is invited to retry
EX_PROTOCOL = 76     # remote error in protocol
EX_NOPERM = 77       # permission denied
EX_CONFIG = 78       # configuration error


# The command line arguments and their related globals
inputDir = '.'                  # The directory where the input files will be found
outputDir = '.'                 # The directory where the output files will be written
configDir = '.'                 # The directory where the config files will be found
configFile = 'verifyAddress.json'    # The default configuration file
verifyAddressService = None     # Run as a service
verifyAddressPort = None        # The service port
GNAFdir = None                  # Use the standard G-NAF psv files from this folder
ABDdir = None                   # The directory where the standard ABS csv files will be found (default='./G-NAF')
DataDir = '.'                   # The directory where the data files will be found
NTpostcodes = False             # Assume 8xx in NT postcode 08xx
region = False                  # Assume Australian region (State/Territory) if no state/territory supplied, but a unique suburb found
DatabaseType = None             # The database type
engine = None                   # The database engine
conn = None                     # The database connection
Session = None                  # The database session maker
databaseName = None             # The database name
addExtras = None                # Strip of extra trims
logDir = '.'                    # The directory where the log files will be written
logging_levels = {0:logging.CRITICAL, 1:logging.ERROR, 2:logging.WARNING, 3:logging.INFO, 4:logging.DEBUG}
loggingLevel = logging.NOTSET        # The default logging level
logFile = None                  # The name of the logfile (output to stderr if None)
fh = None                       # The logging handler for file things
sh = None                       # The logging handler for stdin things
abbreviate = False              # Output abbreviated street types
returnBoth = False              # Output returnBothd street types

# The global data
mydb = None                     # The database connector for tables
cursor = None                   # The database cursor for tables
states = {}                     # The stateAbbrev, regex(stateName), regex(stateAbbrev) for each statePid
postcodes = {}                  # Postcodes and their states and suburbs
suburbs = {}                    # Locality and Suburb data
suburbLen = {}                  # Length of each suburb name, soundex code and list of suburbs
suburbCount = {}                # Count of properties within each suburb/state combination
maxSuburbLen = None             # Length of the longest suburb
localities = {}                 # List of tuples of (statePid, localityName, alias) for each localityPid
localityNames = set()           # Set of all locality names
stateLocalities = {}            # Sets of localityPids for each statePid
postcodeLocalities = {}         # Postcodes and their set of localityPids
localityPostcodes = {}          # Localities and their set  of postcodes
neighbours = {}                 # LocalityPids with their set of neighbouring locality pids
streetNames = {}                # Street Name/Type/Suffix, localityPid and alias for each streetPid
streets = {}                    # Streets by soundCode, streetKey, source and streetPid
streetLen = {}                  # Length of street name with all the matching streets
shortStreets = {}               # Street with no street type and their geocode data
streetTypes = {}                # Street type and list of streetTypeAbbrev, regex(streetType), regex(streetTypeAbbrev)
streetTypeCount = {}            # Street type and count of properties with this street type
streetTypeSuburbs = {}          # Suburbs containing this street type as part of their name (regex of preceeding word and street type)
streetTypeSound = {}            # Unique soundex for street types
streetSuffixes = {}             # Street suffix and list of regex(streetSuffix), streetSuffixAbbrev)
streetNos = {}                  # Streets with their houses and geocode data
stateStreets = {}               # Sets of streetPids for each statePid
streetLocalities = {}           # Sets of localityPid for each streetPid
localityStreets = {}            # Sets of streetPids for each localityPid
buildings = {}                  # Building name, streetPid, regex and details
buildingPatterns = {}           # Building name, regular expresson for finding building name
flats = []                      # List of regular expressions for finding flat types
levels = []                     # List of regular expressions for finding unit types
extraTrims = []                 # Any extra trims to be removed
services = []                   # Postal Delivery Services
SA1map = {}                     # key=Mesh Block 2016 code, value=SA1 code
LGAmap = {}                     # key=Mesh Block 2016 code, value=LGA code
SandTs = ['ACT', 'NSW', 'NT', 'OT', 'QLD', 'SA', 'TAS', 'VIC', 'WA']

# Set up the default configuration for suburb/street weights and fuzz levels
# These can be overridden from the configuration file
suburbSourceWeight = {'G':10, 'GA':9, 'GN':8, 'GS':5, 'GL':4, 'GAS':2, 'GAL':1, '':0}
streetSourceWeight = {'G':10, 'GA':9, 'GS':5, 'GL':4, 'GAS':2, 'GAL':1, '':0}
                                # fuzzLevels
fuzzLevels = [ 1, 2,  3, 4, 5, 6, 7, 8, 9, 10 ]

slash = re.compile(r'\\')
oneSpace = re.compile(r'\s\s+')
dashSpace = re.compile(r'\s*-\s*')
endHyphen = re.compile(r'-$')
deliveryNumber = r'\b([A-Z]{1,2})?(\d{1,6})([A-Z]{1,2})?\b(?<!([ 2-9]1ST|[ 2-9]2ND|[ 2-9]3RD|[ 0-9][4-9]TH|1[1-3]TH))'
deliveryRange = deliveryNumber + r'(( *- *)' + deliveryNumber + r')?'
LOTpattern = re.compile(r'(LOT *)' + deliveryRange)
lastDigit = re.compile(deliveryRange)
period = re.compile(r'\.')


# Create the class for handline http request
class verifyAddressHandler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *logArgs):
        return

    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        self.message = '<html><head><title>Geocode an Australian Address</title><link rel="icon" href="data:,"></head><body>'
        self.message += '<h1>Geocode and Normalize/Standardize an Australian Address</h1>'
        self.message += '<form method="post" action ="' + self.path + '">'
        self.message += '<h2>Paste your Australian Address as a single line below</h2>'
        self.message += '<input type="text" name="line" style="width:70%"></input>'
        self.message += '<h1>OR</h1>'
        self.message += '<h2>Paste your semi-structured Australian Address below - then click the Geocode button</h2>'
        self.message += '<table style="width:70%"><tr>'
        self.message += '<td style="width:20%;text-align=right">Line1</td>'
        self.message += '<td><input type="text" name="line1" style="width:80%;text-align=left"></input></td>'
        self.message += '</tr><tr>'
        self.message += '<td style="width:20%;text-align=right">Line2</td>'
        self.message += '<td><input type="text" name="line2" style="width:80%;text-align=left"></input></td>'
        self.message += '</tr><tr>'
        self.message += '<td style="width:20%;text-align=right">Suburb</td>'
        self.message += '<td><input type="text" name="suburb" style="width:80%;text-align=left"></input></td>'
        self.message += '</tr><tr>'
        self.message += '<td style="width:20%;text-align=right">State</td>'
        self.message += '<td><input type="text" name="state" style="width:80%;text-align=left"></input></td>'
        self.message += '</tr><tr>'
        self.message += '<td style="width:20%;text-align=right">Postcode</td>'
        self.message += '<td><input type="text" name="postcode" style="width:80%;text-align=left"></input></td>'
        self.message += '</tr></table>'
        self.message += '<h2>then click the Geocode button</h2>'
        self.message += '<p><input type="submit" value="Geocode this please"/></p>'
        self.message += '</form></body></html>'
        self.wfile.write(self.message.encode('utf-8'))
        return

    def do_POST(self):                # We only handle POST requests

        # Reset all the globals
        self.data = VerifyData('[verifyAddressService-' + threading.currentThread().getName() + ']')

        # Set up logging for this new thread
        self.data.logger = logging.getLogger()

        # Get the address data
        content_len = int(self.headers['Content-Length'])
        content_type = self.headers['Content-Type'].casefold()
        try:
            accept_type = self.headers['Accept'].casefold()
        except Exception as expt:
            accept_type = 'text/html'
        body = self.rfile.read(content_len)    # Get the URL encoded body
        if content_type == 'application/x-www-form-urlencoded':
            try:
                # Create self.data.params to mirror the JSON payload
                params = parse_qs(body)
                self.data.params = {}
                line0 = ''
                line1 = ''
                line2 = ''
                suburb = ''
                state = ''
                postcode = ''
                if b'line' in params:
                    line0 = params[b'line'][0].decode('ASCII').strip()
                if b'line1' in params:
                    line1 = params[b'line1'][0].decode('ASCII').strip()
                if b'line2' in params:
                    line2 = params[b'line2'][0].decode('ASCII').strip()
                if b'suburb' in params:
                    suburb = params[b'suburb'][0].decode('ASCII').strip()
                if b'state' in params:
                    state = params[b'state'][0].decode('ASCII').strip()
                if b'postcode' in params:
                    postcode = params[b'postcode'][0].decode('ASCII').strip()
                if line0 == '':
                    # Looks like structured data
                    if line1 == '':
                        if line2 == '':        # No address lines of any sort
                            self.data.logger.critical('no lines')
                            # Return Bad Request
                            # Now output the web page
                            self.send_response(200)
                            self.send_header('Content-type', 'text/html')
                            self.end_headers()
                            # Assembling the HTML content
                            self.message = '<html><head><title>Geocode an Australian Address</title><link rel="icon" href="data:,"></head><body>'
                            self.message += '<h1>Geocoded and Normalized/Standardized Address</h1>'
                            self.message += '<h2>Error - no address lines entered</h2>'
                            self.message += '<h3>Please enter a single line address or a semi-structured address</h3>'
                            self.message += '<br><a href="' + self.path + '">Click here to Geocode and Normalize/Standardize another Australian Address</a><br>'
                            self.message += '</body></html>'
                            self.wfile.write(self.message.encode('utf-8'))
                            # Shutdown logging
                            for this_hdlr in self.data.logger.handlers:
                                this_hdlr.flush()
                            del self.data
                            return
                        self.data.params['addressLines'] = [line2]
                    elif line2 == '':
                        self.data.params['addressLines'] = [line1]
                    else:
                        self.data.params['addressLines'] = []
                        self.data.params['addressLines'].append(line1)
                        self.data.params['addressLines'].append(line2)
                    if suburb != '':
                        self.data.params['suburb'] = suburb
                    if state != '':
                        self.data.params['state'] = state
                    if postcode != '':
                        self.data.params['postcode'] = postcode
                elif (line1 == '') and (line2 == '') and (suburb == '') and (state == '') and (postcode == ''):
                    # Looks like unstructured data
                    self.data.params['addressLines'] = [line0]
                else:
                    self.data.logger.critical('both single line and semi-structured addresses)')
                    # It's a mess - both structured and unstructured
                    # Now output the web page
                    self.send_response(200)
                    self.send_header('Content-type', 'text/html')
                    self.end_headers()
                    # Assembling the HTML content
                    self.message = '<html><head><title>Geocode an Australian Address</title><link rel="icon" href="data:,"></head><body>'
                    self.message += '<h1>Geocoded and Normalized/Standardized Address</h1>'
                    self.message += '<h2>Error - both single line and semi-structured address entered</h2>'
                    self.message += '<h3>Please enter a single line address or a semi-structured address</h3>'
                    self.message += '<br><a href="' + self.path + '">Click here to Geocode and Normalize/Standardize another Australian Address</a><br>'
                    self.message += '</body></html>'
                    self.wfile.write(self.message.encode('utf-8'))
                    # Shutdown logging
                    for this_hdlr in self.data.logger.handlers:
                        this_hdlr.flush()
                    del self.data
                    return
            except Exception as ee:
                # Return Bad Request
                # Shutdown logging
                for this_hdlr in self.data.logger.handlers:
                    this_hdlr.flush()
                del self.data
                self.send_error(400)
                return
        else:
            # Read in the JSON payload
            try:
                self.data.params = json.loads(body)    # JSON payload
            except Exception as expt:
                self.data.logger.critical('Bad JSON')
                # Return Bad Request
                # Shutdown logging
                for this_hdlr in self.data.logger.handlers:
                    this_hdlr.flush()
                del self.data
                self.send_error(400)
                return

        # Process the request - get the Address to verify
        self.data.Address = {}
        for eachAddressPart in self.data.params:
            self.data.Address[eachAddressPart] = self.data.params[eachAddressPart]

        self.data.logger.info('verifyAddress address(%s)', self.data.Address)

        # verify the address
        verifyAddress(self.data)

        # Check if JSON or HTML response required
        if accept_type == 'application/json':
            # Return the results
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()

            # Return the results dictionary
            self.data.response = json.dumps(self.data.result)
            self.data.response = self.data.response.encode('utf-8')
            self.wfile.write(self.data.response)
        else:
            # Now output the web page
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()

            # Assembling the HTML content
            self.data.message = '<html><head><title>Geocode an Australian Address</title><link rel="icon" href="data:,"></head><body>'
            self.data.message += '<h1>Geocoded and Normalized/Standardized Address</h1>'
            self.data.message += '<h2>Geocoded Meta Data</h2>'
            self.data.message += '<table style="width:70%"><tr>'
            self.data.message += '<td style="width:20%;text-align=right">Latitude</td>'
            self.data.message += '<td style="width:80%;text-align=left">' + self.data.result['latitude'] + '</td>'
            self.data.message += '</tr><tr>'
            self.data.message += '<td style="width:20%;text-align=right">Longitude</td>'
            self.data.message += '<td style="width:80%;text-align=left">' + self.data.result['longitude'] + '</td>'
            self.data.message += '</tr><tr>'
            self.data.message += '<td style="width:20%;text-align=right">Mesh Block</td>'
            self.data.message += '<td style="width:80%;text-align=left">' + self.data.result['Mesh Block'] + '</td>'
            self.data.message += '</tr><tr>'
            self.data.message += '<td style="width:20%;text-align=right">SA1</td>'
            self.data.message += '<td style="width:80%;text-align=left">' + self.data.result['SA1'] + '</td>'
            self.data.message += '</tr><tr>'
            self.data.message += '<td style="width:20%;text-align=right">LGA</td>'
            self.data.message += '<td style="width:80%;text-align=left">' + self.data.result['LGA'] + '</td>'
            self.data.message += '</tr></table>'

            self.data.message += '<h2>Normalized/Standardized Address</h2>'
            self.data.message += '<table style="width:70%"><tr>'
            if self.data.result['isPostalService'] and (self.data.result['buildingName'] != ''):
                self.data.message += '<td style="width:30%;text-align=right">Building Name</td>'
                self.data.message += '<td style="width:60%;text-align=left">' + self.data.result['buildingName'] + '</td>'
                self.data.message += '</tr><tr>'
            if (self.data.result['addressLine1'] != '') and (self.data.result['addressLine1'][-1] == ','):
                self.data.result['addressLine1'] = self.data.result['addressLine1'][:-1]
            self.data.message += '<td style="width:30%;text-align=right">Address Line 1</td>'
            self.data.message += '<td style="width:60%;text-align=left">' + self.data.result['addressLine1'] + '</td>'
            self.data.message += '</tr><tr>'
            if (self.data.result['addressLine2'] != '') and (self.data.result['addressLine2'][-1] == ','):
                self.data.result['addressLine2'] = self.data.result['addressLine2'][:-1]
            self.data.message += '<td style="width:30%;text-align=right">Address Line 2</td>'
            self.data.message += '<td style="width:60%;text-align=left">' + self.data.result['addressLine2'] + '</td>'
            self.data.message += '</tr><tr>'
            if not self.data.result['isPostalService'] and (self.data.result['buildingName'] != ''):
                self.data.message += '<td style="width:30%;text-align=right">Building Name</td>'
                self.data.message += '<td style="width:60%;text-align=left">' + self.data.result['buildingName'] + '</td>'
                self.data.message += '</tr><tr>'
            self.data.message += '<td style="width:30%;text-align=right">House Number</td>'
            self.data.message += '<td style="width:60%;text-align=left">' + self.data.result['houseNo'] + '</td>'
            self.data.message += '</tr><tr>'
            self.data.message += '<td style="width:30%;text-align=right">Street</td>'
            self.data.message += '<td style="width:60%;text-align=left">' + self.data.result['street'] + '</td>'
            self.data.message += '</tr><tr>'
            self.data.message += '<td style="width:30%;text-align=right">Suburb</td>'
            self.data.message += '<td style="width:60%;text-align=left">' + self.data.result['suburb'] + '</td>'
            self.data.message += '</tr><tr>'
            self.data.message += '<td style="width:30%;text-align=right">Postcode</td>'
            self.data.message += '<td style="width:60%;text-align=left">' + self.data.result['postcode'] + '</td>'
            self.data.message += '</tr><tr>'
            self.data.message += '<td style="width:30%;text-align=right">State</td>'
            self.data.message += '<td style="width:60%;text-align=left">' + self.data.result['state'] + '</td>'
            self.data.message += '</tr></table>'

            if returnBoth:
                self.data.message += '<h2>Abbreviated Normalized/Standardized Address</h2>'
                self.data.message += '<table style="width:70%"><tr>'
                if (self.data.result['addressLine1Abbrev'] != '') and (self.data.result['addressLine1Abbrev'][-1] == ','):
                    self.data.result['addressLine1Abbrev'] = self.data.result['addressLine1Abbrev'][:-1]
                self.data.message += '<td style="width:30%;text-align=right">Abbreviated Address Line 1</td>'
                self.data.message += '<td style="width:60%;text-align=left">' + self.data.result['addressLine1Abbrev'] + '</td>'
                self.data.message += '</tr><tr>'
                if (self.data.result['addressLine2Abbrev'] != '') and (self.data.result['addressLine2Abbrev'][-1] == ','):
                    self.data.result['addressLine2Abbrev'] = self.data.result['addressLine2Abbrev'][:-1]
                self.data.message += '<td style="width:30%;text-align=right">Abbreviated Address Line 2</td>'
                self.data.message += '<td style="width:60%;text-align=left">' + self.data.result['addressLine2Abbrev'] + '</td>'
                self.data.message += '</tr></table>'

            self.data.message += '<h2>G-NAF ID, Accuracy, Score and Messages</h2>'
            self.data.message += '<table style="width:70%"><tr>'
            self.data.message += '<td style="width:20%;text-align=right">G-NAF ID</td>'
            self.data.message += '<td style="width:80%;text-align=left">' + str(self.data.result['G-NAF ID']) + '</td>'
            self.data.message += '</tr><tr>'
            self.data.message += '<td style="width:20%;text-align=right">Accuracy</td>'
            self.data.message += '<td style="width:80%;text-align=left">' + str(self.data.result['accuracy']) + '</td>'
            self.data.message += '</tr><tr>'
            self.data.message += '<td style="width:20%;text-align=right">Fuzz Level</td>'
            self.data.message += '<td style="width:80%;text-align=left">' + str(self.data.result['fuzzLevel']) + '</td>'
            self.data.message += '</tr><tr>'
            self.data.message += '<td style="width:20%;text-align=right">Score</td>'
            self.data.message += '<td style="width:80%;text-align=left">' + str(self.data.result['score']) + '</td>'
            self.data.message += '</tr><tr>'
            self.data.message += '<td style="width:20%;text-align=right">Status</td>'
            self.data.message += '<td style="width:80%;text-align=left">' + self.data.result['status'] + '</td>'
            if len(self.data.result['messages']) > 0:
                self.data.message += '</tr><tr>'
                firstMessage = True
                for mess in range(len(self.data.result['messages'])):
                    if firstMessage:
                        self.data.message += '<td style="width:20%;text-align=right">Messages</td>'
                        firstMessage = False
                    else:
                        self.data.message += '<td style="width:20%;text-align=right"></td>'
                    self.data.message += '<td style="width:80%;text-align=left">' + self.data.result['messages'][mess] + '</td>'
            self.data.message += '</tr></table>'

            self.data.message += '<p><b><a href="' + self.path + '">Click here to Geocode and Normalize/Standardize another Australian Address</a></b><br>'
            self.data.message += '</body></html>'
            self.data.response = self.data.message.encode('utf-8')
            self.wfile.write(self.data.response)

        # Shutdown logging
        for this_hdlr in self.data.logger.handlers:
            this_hdlr.flush()
        del self.data
        return


class ThreadedHTTPServer(socketserver.ThreadingMixIn, HTTPServer) :
    '''
Handle requests in a separate thread.
    '''
    pass


# fork() NOT AVAILABLE ON WINDOWS
# class ForkingHTTPServer(socketserver.ForkingMixIn, HTTPServer) :
#     '''
# Handle requests in a separate thread.
#     '''
#     pass


def cleanText(thisText, removeCommas):
    if thisText is not None:
        thisText = str(thisText).upper()            # Convert to upper case
        thisText = thisText.replace(':', '')        # Remove colons
        if removeCommas:
            thisText = thisText.replace(',', '')    # Remove commas
        thisText = slash.sub('/', thisText)            # Change backslash to slash so we don't acccidentally crash regular expressions
        thisText = oneSpace.sub(' ', thisText)        # Collapse mutiple white space to a single space
        thisText = dashSpace.sub('-', thisText)        # Remove white space around the hyphen in hyphenated streets, suburbs
        thisText = endHyphen.sub('', thisText)        # Remove hyphens at the end of streets, suburbs
        thisText = thisText.strip()                    # Remove white space from start and end of text
        return thisText
    else:
        return ''


def addPostcode(this, postcode, suburb, statePid, sa1, lga, latitude, longitude):
    '''
    Add postcode data from postcodeSA1LGA, postcode_SA1LGA.csv
    '''
    # this.logger.debug('Adding postcode (%s), suburb (%s)', postcode, suburb)

    global maxSuburbLen

    if postcode not in postcodes:
        postcodes[postcode] = {}
        postcodes[postcode]['states'] = set()
    postcodes[postcode]['states'].add(statePid)
    if suburb == '':
        postcodes[postcode][suburb] = [sa1, lga, latitude, longitude]
    else:
        if suburb not in postcodes[postcode]:
            postcodes[postcode][suburb] = {}
        postcodes[postcode][suburb][statePid] = [sa1, lga, latitude, longitude]
    if statePid not in postcodes[postcode]:
        postcodes[postcode][statePid] = set()
    postcodes[postcode][statePid].add(suburb)
    soundCode = jellyfish.soundex(suburb)
    if soundCode not in suburbs:
        suburbs[soundCode] = {}
    if suburb not in suburbs[soundCode]:
        suburbs[soundCode][suburb] = {}
    if statePid not in suburbs[soundCode][suburb]:
        suburbs[soundCode][suburb][statePid] = {}
    if 'A' not in suburbs[soundCode][suburb][statePid]:
        suburbs[soundCode][suburb][statePid]['A'] = {}
    suburbs[soundCode][suburb][statePid]['A'][postcode] = [sa1, lga, latitude, longitude]
    suburbLength = len(suburb)
    if (maxSuburbLen is None) or (suburbLength > maxSuburbLen):
        maxSuburbLen = suburbLength
    if suburbLength not in suburbLen:
        suburbLen[suburbLength] = {}
    if soundCode not in suburbLen[suburbLength]:
        suburbLen[suburbLength][soundCode] = []
    if suburb not in suburbLen[suburbLength][soundCode]:
        suburbLen[suburbLength][soundCode].append(suburb)

    return


def addSuburb(this, localityPid, statePid, suburb, alias, sa1, lga, latitude, longitude):
    '''
    Add suburb data from localitySA1LGA, locality_SA1LGA.psv
    '''
    # this.logger.debug('Adding suburb %s', suburb)

    global maxSuburbLen

    # Add to streetTypeSuburb if any word in suburb is streetType
    theseWords = suburb.split(' ')
    if len(theseWords) > 1:
        for ii, word in enumerate(theseWords[1:]):
            if word in streetTypes:
                if word not in streetTypeSuburbs:
                    streetTypeSuburbs[word] = set()
                streetTypeSuburbs[word].add(re.compile(theseWords[ii] + r'\s+' + word))

    localities[localityPid].add((statePid, suburb, alias))      # Add suburb name to localities (if not already there)
    localityNames.add(suburb)

    soundCode = jellyfish.soundex(suburb)
    if soundCode not in suburbs:
        suburbs[soundCode] = {}
    if suburb not in suburbs[soundCode]:
        suburbs[soundCode][suburb] = {}
    if statePid not in suburbs[soundCode][suburb]:
        suburbs[soundCode][suburb][statePid] = {}
    if alias == 'P':
        if 'G' not in suburbs[soundCode][suburb][statePid]:
            suburbs[soundCode][suburb][statePid]['G'] = {}
        suburbs[soundCode][suburb][statePid]['G'][localityPid] = [sa1, lga, latitude, longitude]
    else:
        if 'GA' not in suburbs[soundCode][suburb][statePid]:
            suburbs[soundCode][suburb][statePid]['GA'] = {}
        suburbs[soundCode][suburb][statePid]['GA'][localityPid] = [sa1, lga, latitude, longitude]
    done = set()
    addNeighbours(this, localityPid, soundCode, suburb, statePid, sa1, lga, latitude, longitude, done, 4)
    suburbLength = len(suburb)
    if (maxSuburbLen is None) or (suburbLength > maxSuburbLen):
        maxSuburbLen = suburbLength
    if suburbLength not in suburbLen:
        suburbLen[suburbLength] = {}
    if soundCode not in suburbLen[suburbLength]:
        suburbLen[suburbLength][soundCode] = []
    if suburb not in suburbLen[suburbLength][soundCode]:
        suburbLen[suburbLength][soundCode].append(suburb)

    return


def addLocality(this, localityPid, suburb, postcode, statePid, alias):
    '''
    Add locality data from LOCALITY, LOCALITY_ALIAS, locality.psv
    '''
    # this.logger.debug('Adding locality %s', suburb)

    if localityPid not in localities:
        localities[localityPid] = set()
    localities[localityPid].add((statePid, suburb, alias))
    localityNames.add(suburb)
    if statePid not in stateLocalities:
        stateLocalities[statePid] = set()
    stateLocalities[statePid].add(localityPid)
    if (postcode is not None) and (postcode != ''):
        if postcode not in postcodeLocalities:
            postcodeLocalities[postcode] = set()
        postcodeLocalities[postcode].add(localityPid)
        if localityPid not in localityPostcodes:
            localityPostcodes[localityPid] = set()
        localityPostcodes[localityPid].add(postcode)
    if alias != 'P':            # Don't clone postcodes for locality aliases
        return
    soundCode = jellyfish.soundex(suburb)
    if (soundCode in suburbs) and (suburb in suburbs[soundCode]):
        if (statePid in suburbs[soundCode][suburb]) and ('A' in suburbs[soundCode][suburb][statePid]):
            for postcode in suburbs[soundCode][suburb][statePid]['A']:
                if postcode not in postcodeLocalities:
                    postcodeLocalities[postcode] = set()
                postcodeLocalities[postcode].add(localityPid)
                if localityPid not in localityPostcodes:
                    localityPostcodes[localityPid] = set()
                localityPostcodes[localityPid].add(postcode)
    return


def addStreetName(this, streetPid, streetName, streetType, streetSuffix, localityPid, alias):
    '''
    Add street names from STREET_LOCALITY, STREET_LOCALITY_ALIAS, xxx_STREET_LOCALITY_psv.psv, xxx_STREET_LOCALITY_ALIAS_psv.psv, street_details.psv
    '''
    # this.logger.debug('Adding street name %s %s %s', streetName, streetType, streetSuffix)

    # Deal with street names that contain abbreviations
    # Build up a list of acceptable equivalent street names
    names = [(streetName, alias)]
    if streetName[:3] == 'MT ':
        names.append(('MOUNT ' + streetName[3:], alias))
    # For hyphenated street names we allow both halves as street names aliases
    # Plus the names in the reverse order, plus the parts in either order separated by a space instead of a hyphen
    hyphenParts = streetName.split('-')
    if len(hyphenParts) == 2:
        names.append((hyphenParts[0], 'A'))
        names.append((hyphenParts[1], 'A'))
        names.append((hyphenParts[1] + '-' + hyphenParts[0], 'A'))
        names.append((hyphenParts[0] + ' ' + hyphenParts[1], 'A'))
        names.append((hyphenParts[1] + ' ' + hyphenParts[0], 'A'))

    if streetPid not in streetNames:
        streetNames[streetPid] = []
    for name, thisAlias in names:
        if streetType is None:
            if streetSuffix is None:
                streetNames[streetPid].append([name, '', '', localityPid, thisAlias])
            else:
                streetNames[streetPid].append([name, '', streetSuffix, localityPid, thisAlias])
        elif streetSuffix is None:
            streetNames[streetPid].append([name, streetType, '', localityPid, thisAlias])
        else:
            streetNames[streetPid].append([name, streetType, streetSuffix, localityPid, thisAlias])
    if localityPid not in localityStreets:
        localityStreets[localityPid] = set()
    localityStreets[localityPid].add(streetPid)
    if streetPid not in streetLocalities:
        streetLocalities[streetPid] = localityPid
    if localityPid not in localities:
        return
    for thisStatePid, thisSuburb, thisAlias in localities[localityPid]:
        statePid = thisStatePid
        if statePid not in stateStreets:
            stateStreets[statePid] = set()
        stateStreets[statePid].add(streetPid)
    if streetType not in streetTypeCount:
        streetTypeCount[streetType] = 1
    else:
        streetTypeCount[streetType] += 1

    return


def addStreet(this, streetPid, sa1, lga, latitude, longitude):
    '''
    Add street geocode data from streetSA1LGA, street_SA1LGA.psv
    '''
    # this.logger.debug('Adding street sa1 %s', sa1)

    for name in range(len(streetNames[streetPid])):
        streetName = streetNames[streetPid][name][0]
        streetType = streetNames[streetPid][name][1]
        streetSuffix = streetNames[streetPid][name][2]
        alias = streetNames[streetPid][name][4]
        soundCode = jellyfish.soundex(streetName)
        if streetType ==  '':
            if streetSuffix == '':
                streetKey = streetName + '~~'
            else:
                streetKey = '~'.join([streetName, '', streetSuffix])
        elif streetSuffix == '':
            streetKey = '~'.join([streetName, streetType, ''])
        else:
            streetKey = '~'.join([streetName, streetType, streetSuffix])
        if soundCode not in streets:
            streets[soundCode] = {}
        if streetKey not in streets[soundCode]:
            streets[soundCode][streetKey] = {}
        if alias == 'P':
            if 'G' not in streets[soundCode][streetKey]:
                streets[soundCode][streetKey]['G'] = {}
            streets[soundCode][streetKey]['G'][streetPid] = [sa1, lga, latitude, longitude]
        else:
            if 'GA' not in streets[soundCode][streetKey]:
                streets[soundCode][streetKey]['GA'] = {}
            streets[soundCode][streetKey]['GA'][streetPid] = [sa1, lga, latitude, longitude]

        if streetType == '':
            if streetSuffix == '':
                shortKey = streetName
                shortRegex = streetName
            else:
                shortKey = ' '.join([streetName, streetSuffix]).strip()
                shortRegex = streetName + r'\s+' + streetSuffix
            if shortKey not in shortStreets:
                shortStreets[shortKey] = {}
                shortStreets[shortKey]['regex'] = re.compile(r'\b' + shortRegex + r'\b')
                shortStreets[shortKey]['SK'] = streetKey
            if alias == 'P':
                if 'G' not in shortStreets[shortKey]:
                    shortStreets[shortKey]['G'] = {}
                shortStreets[shortKey]['G'][streetPid] = [sa1, lga, latitude, longitude]
            else:
                if 'GA' not in shortStreets[shortKey]:
                    shortStreets[shortKey]['GA'] = {}
                shortStreets[shortKey]['GA'][streetPid] = [sa1, lga, latitude, longitude]
        streetLength = len(streetName)
        if streetLength not in streetLen:
            streetLen[streetLength] = []
        streetLen[streetLength].append([soundCode, streetName, streetKey])

    return


def addStreetNumber(this, buildingName, streetPid, localityPid, lotNumber, numberFirst, numberLast, mbCode, latitude, longitude, addressPid):
    '''
    Add street number from ADDRESS_DETAIL table, xxx_ADDRESS_DETAIL_psv.psv or address_detail.psv
    '''
    # this.logger.debug('Adding street number %s', str(numberFirst))
    if localityPid in localities:       # Count properties in this suburb
        for thisStatePid, thisSuburb, thisAlias in localities[localityPid]:
            if thisSuburb not in suburbCount:
                suburbCount[thisSuburb] = {}
            if thisStatePid not in suburbCount[thisSuburb]:
                suburbCount[thisSuburb][thisStatePid] = 0
            suburbCount[thisSuburb][thisStatePid] += 1
    if lotNumber is not None:
        if (buildingName is not None) and (buildingName != ''):
            if buildingName not in buildings:
                buildings[buildingName] = []
            buildings[buildingName].append([lotNumber, streetPid, localityPid])
            if buildingName not in buildingPatterns:
                buildingPatterns[buildingName] = re.compile(r'\b' + buildingName.replace(' ', r'\s+') + r'\b')
        if streetPid not in streetNos:
            streetNos[streetPid] = {}
        streetNos[streetPid][lotNumber] = [mbCode, latitude, longitude, True, addressPid]
    if numberFirst is not None:
        if streetPid not in streetNos:
            streetNos[streetPid] = {}
        if numberLast is None:
            if (buildingName is not None) and (buildingName != ''):
                if buildingName not in buildings:
                    buildings[buildingName] = []
                buildings[buildingName].append([numberFirst, streetPid, localityPid])
                if buildingName not in buildingPatterns:
                    buildingPatterns[buildingName] = re.compile(r'\b' + buildingName.replace(' ', r'\s+') + r'\b')
            streetNos[streetPid][numberFirst] = [mbCode, latitude, longitude, False, addressPid]
        else:
            step = 2
            if streetPid in streetNames:
                streetType = streetNames[streetPid][0][1]
                if streetType in ['CLOSE', 'COURT', 'PLACE', 'CUL-DE-SAC']:
                    step = 1
            for houseNo in range(int(numberFirst), int(numberLast) + 1, step):
                if (buildingName is not None) and (buildingName != ''):
                    if buildingName not in buildings:
                        buildings[buildingName] = []
                    buildings[buildingName].append([houseNo, streetPid, localityPid])
                    if buildingName not in buildingPatterns:
                        buildingPatterns[buildingName] = re.compile(r'\b' + buildingName.replace(' ', r'\s+') + r'\b')
                streetNos[streetPid][houseNo] = [mbCode, latitude, longitude, False, addressPid]

    return


def addNeighbours(this, localityPid, soundCode, suburb, statePid, sa1, lga, latitude, longitude, done, depth):
    '''
Add neigbouring locality_pids, for this locality, to this.neiboursSet
    '''
    # this.logger.debug('Adding neighbour for suburb(%s), soundCode(%s), locality(%s) in state(%s), depth(%d)', suburb, soundCode, localityPid, states[statePid][0], depth)

    # Assemble the neighbouring localities
    if localityPid in neighbours:
        done.add(localityPid)
        for neighbour in sorted(neighbours[localityPid]):
            if 'GN' not in suburbs[soundCode][suburb][statePid]:
                suburbs[soundCode][suburb][statePid]['GN'] = {}
            if neighbour not in suburbs[soundCode][suburb][statePid]['GN']:
                # this.logger.debug('addNeighbour - adding %s', neighbour)
                suburbs[soundCode][suburb][statePid]['GN'][neighbour] = [sa1, lga, latitude, longitude]
            # Do neighbours of this neighbour if required
            if (depth > 0) and (neighbour in neighbours) and (neighbour not in done):
                addNeighbours(this, neighbour, soundCode, suburb, statePid, sa1, lga, latitude, longitude, done, depth - 1)


def initData(this):

    '''
    Read in the G-NAF tables, Australia Post data and any Other data
    from the specified database (if any) and build up the data structures used to verify addresses.
    '''

    this.logger.info('Starting to initialize data')

    # Read in the States and compile regular expressions for both the full and abbreviated name
    # We use the state_pid as the key so we can use it to join to other tables
    this.logger.info('Fetching states')
    sts = []
    if DatabaseType is not None:    # Use the database tables
        dfStates = pd.read_sql_query(text('SELECT state_pid, date_retired, state_name, state_abbreviation FROM STATE WHERE date_retired IS NULL'), engine.connect())
        results = dfStates.values.tolist()
        for (statePid, date_retired, name, state) in results:
            if date_retired is not None:
                continue
            sts.append([statePid, cleanText(name, True), state])
    elif GNAFdir is not None:       # Use the standard G-NAF PSV files
        # STATE_PID|DATE_CREATED|DATE_RETIRED|STATE_NAME|STATE_ABBREVIATION
        for SandT in SandTs:
            with open(os.path.join(GNAFdir, 'Standard', SandT + '_STATE_psv.psv'), 'rt', newline='', encoding='utf-8') as stateFile:
                stateReader = csv.DictReader(stateFile, dialect=csv.excel, delimiter='|')
                for rrow in stateReader:
                    if rrow['DATE_RETIRED'] != '':        # Skip if retired
                        continue
                    sts.append([rrow['STATE_PID'], cleanText(rrow['STATE_NAME'], True), rrow['STATE_ABBREVIATION']])
    else:           # Use the optimised PSV files
        # STATE_PID|STATE_NAME|STATE_ABBREVIATION
        with open(os.path.join(DataDir, 'state.psv'), 'rt', newline='', encoding='utf-8') as stateFile:
            stateReader = csv.DictReader(stateFile, dialect=csv.excel, delimiter='|')
            for rrow in stateReader:
                sts.append([rrow['STATE_PID'], cleanText(rrow['STATE_NAME'], True), rrow['STATE_ABBREVIATION']])

    # Now build up states
    this.logger.info('Building states')
    for state in sts:
        statePid = state[0]
        stateName = state[1]
        stateAbbrev = state[2]
        if statePid not in states:
            states[statePid] = []                # The stateAbbrev, regex(stateName), regex(stateAbbrev) for each statePid
        states[statePid] = [stateAbbrev,
                           re.compile(r'\b' + stateName.replace(' ', r'\s+') + r'\b'),
                           re.compile(r'\b' + stateAbbrev.replace(' ', r'\s+') + r'\b')]

    # Read in any extra state abbreviation
    if addExtras:
        if os.path.isfile(os.path.join(DataDir, 'extraStates.psv')):
            # stateAbbrev|abbrev
            with open(os.path.join(DataDir, 'extraStates.psv'), 'rt', newline='', encoding='utf-8') as stateFile:
                stateReader = csv.DictReader(stateFile, dialect=csv.excel, delimiter='|')
                for rrow in stateReader:
                    for statePid, statesInfo in states.items():
                        if statesInfo[0] == rrow['stateAbbrev']:
                            abbrev = rrow['abbrev']
                            abbrev = abbrev.replace('.', r'\.')
                            if abbrev[-1] == '.':
                                states[statePid].append(re.compile(r'\b' + rrow['abbrev'].replace(' ', r'\s+')))
                            else:
                                states[statePid].append(re.compile(r'\b' + rrow['abbrev'].replace(' ', r'\s+') + r'\b'))
    this.logger.info('%d states fetched', len(states))

    this.logger.info('Fetching street types and street suffixes')
    if DatabaseType is not None:    # Use the database tables
        dfStreetType = pd.read_sql_query(text('SELECT code, name, description FROM STREET_TYPE_AUT'), engine.connect())
        results = dfStreetType.values.tolist()
        for (code, name, description) in results:
            if code not in streetTypes:
                streetTypes[code] = []
                streetTypes[code].append(name)
            streetTypes[code].append(re.compile(r'\b' + cleanText(code, True) + r'\b'))
            if name != code:
                streetTypes[code].append(re.compile(r'\b' + cleanText(name, True) + r'\b'))
            if (description != code) and (description != name):
                streetTypes[code].append(re.compile(r'\b' + cleanText(description, True) + r'\b'))
        dfStreetSuffix = pd.read_sql_query(text('SELECT code, name, description FROM STREET_SUFFIX_AUT'), engine.connect())
        results = dfStreetSuffix.values.tolist()
        for (code, name, description) in results:
            if code not in streetSuffixes:
                streetSuffixes[code] = []
            streetSuffixes[code].append(re.compile(r'^' + cleanText(code, True) + r'\b'))
            if name != code:
                streetSuffixes[code].append(re.compile(r'^' + cleanText(name, True) + r'\b'))
            if (description != code) and (description != name):
                streetSuffixes[code].append(re.compile(r'^' + cleanText(description, True) + r'\b'))
    elif GNAFdir is not None:       # Use the standard G-NAF PSV files
        # CODE|NAME|DESCRIPTION
        with open(os.path.join(GNAFdir, 'Authority Code', 'Authority_Code_STREET_TYPE_AUT_psv.psv'), 'rt', newline='', encoding='utf-8') as sTypeFile:
            sTypeReader = csv.DictReader(sTypeFile, dialect=csv.excel, delimiter='|')
            for rrow in sTypeReader:
                if rrow['CODE'] not in streetTypes:
                    streetTypes[rrow['CODE']] = []
                    streetTypes[rrow['CODE']].append(rrow['NAME'])
                streetTypes[rrow['CODE']].append(re.compile(r'\b' + cleanText(rrow['CODE'], True) + r'\b'))
                if rrow['NAME'] != rrow['CODE']:
                    streetTypes[rrow['CODE']].append(re.compile(r'\b' + cleanText(rrow['NAME'], True) + r'\b'))
                if (rrow['DESCRIPTION'] != rrow['CODE']) and (rrow['DESCRIPTION'] != rrow['NAME']):
                    streetTypes[rrow['CODE']].append(re.compile(r'\b' + cleanText(rrow['DESCRIPTION'], True) + r'\b'))
        # CODE|NAME|DESCRIPTION
        with open(os.path.join(GNAFdir, 'Authority Code', 'Authority_Code_STREET_SUFFIX_AUT_psv.psv'), 'rt', newline='', encoding='utf-8') as sSuffixFile:
            sSuffixReader = csv.DictReader(sSuffixFile, dialect=csv.excel, delimiter='|')
            for rrow in sSuffixReader:
                if rrow['CODE'] not in streetSuffixes:
                    streetSuffixes[rrow['CODE']] = []
                streetSuffixes[rrow['CODE']].append(re.compile(r'^' + cleanText(rrow['CODE'], True) + r'\b'))
                if rrow['NAME'] != rrow['CODE']:
                    streetSuffixes[rrow['CODE']].append(re.compile(r'^' + cleanText(rrow['NAME'], True) + r'\b'))
                if (rrow['DESCRIPTION'] != rrow['CODE']) and (rrow['DESCRIPTION'] != rrow['NAME']):
                    streetSuffixes[rrow['CODE']].append(re.compile(r'^' + cleanText(rrow['DESCRIPTION'], True) + r'\b'))
    else:           # Use the optimised PSV files
        # CODE|NAME|DESCRIPTION
        with open(os.path.join(DataDir, 'street_type.psv'), 'rt', newline='', encoding='utf-8') as sTypeFile:
            sTypeReader = csv.DictReader(sTypeFile, dialect=csv.excel, delimiter='|')
            for rrow in sTypeReader:
                if rrow['CODE'] not in streetTypes:
                    streetTypes[rrow['CODE']] = []
                    streetTypes[rrow['CODE']].append(rrow['NAME'])
                streetTypes[rrow['CODE']].append(re.compile(r'\b' + cleanText(rrow['CODE'], True) + r'\b'))
                if rrow['NAME'] != rrow['CODE']:
                    streetTypes[rrow['CODE']].append(re.compile(r'\b' + cleanText(rrow['NAME'], True) + r'\b'))
                if (rrow['DESCRIPTION'] != rrow['CODE']) and (rrow['DESCRIPTION'] != rrow['NAME']):
                    streetTypes[rrow['CODE']].append(re.compile(r'\b' + cleanText(rrow['DESCRIPTION'], True) + r'\b'))
        # CODE|NAME|DESCRIPTION
        with open(os.path.join(DataDir, 'street_suffix.psv'), 'rt', newline='', encoding='utf-8') as sSufixFile:
            sSufixReader = csv.DictReader(sSufixFile, dialect=csv.excel, delimiter='|')
            for rrow in sSufixReader:
                if rrow['CODE'] not in streetSuffixes:
                    streetSuffixes[rrow['CODE']] = []
                    streetSuffixes[rrow['CODE']].append(re.compile(r'^' + cleanText(rrow['CODE'], True) + r'\b'))
                if rrow['NAME'] != rrow['CODE']:
                    streetSuffixes[rrow['CODE']].append(re.compile(r'^' + cleanText(rrow['NAME'], True) + r'\b'))
                if (rrow['DESCRIPTION'] != rrow['CODE']) and (rrow['DESCRIPTION'] != rrow['NAME']):
                    streetSuffixes[rrow['CODE']].append(re.compile(r'^' + cleanText(rrow['DESCRIPTION'], True) + r'\b'))

    # Compute the street type sound codes
    for streetType, streetTypeInfo in streetTypes.items():
        soundCode = jellyfish.soundex(streetType)
        if soundCode not in streetTypeSound:
            streetTypeSound[soundCode] = []
        streetTypeSound[soundCode].append(streetType)
        soundCodeAbbrev = jellyfish.soundex(streetTypeInfo[0])
        if soundCodeAbbrev != soundCode:
            if soundCodeAbbrev not in streetTypeSound:
                streetTypeSound[soundCodeAbbrev] = []
            streetTypeSound[soundCodeAbbrev].append(streetType)
    soundCodes = list(streetTypeSound)
    for soundCode in soundCodes:       # Remove any non-unique ones - two or more different street types that sound the same
        if len(streetTypeSound[soundCode]) > 1:
            # this.logger.info('Deleting duplicate street type sound (%s) - %s', soundCode, streetTypeSound[soundCode])
            del streetTypeSound[soundCode]

    # Read in any extra STREET_TYPEs - if required
    if addExtras:
        # streetType|abbrev
        with open(os.path.join(DataDir, 'extraStreetTypes.psv'), 'rt', newline='', encoding='utf-8') as sTypeFile:
            sTypeReader = csv.DictReader(sTypeFile, dialect=csv.excel, delimiter='|')
            for rrow in sTypeReader:
                if rrow['streetType'] is not None:
                    if rrow['streetType'] not in streetTypes:
                        streetTypes[rrow['streetType']] = []
                        streetTypes[rrow['streetType']].append(rrow['abbrev'])
                    streetTypes[rrow['streetType']].append(re.compile(r'\b' + cleanText(rrow['streetType'], True) + r'\b'))
                    if rrow['abbrev'] != rrow['streetType']:
                        streetTypes[rrow['streetType']].append(re.compile(r'\b' + cleanText(rrow['abbrev'], True) + r'\b'))
        # streetSuffix|abbrev
        with open(os.path.join(DataDir, 'extraStreetSuffixes.psv'), 'rt', newline='', encoding='utf-8') as sSuffixFile:
            sSuffixReader = csv.DictReader(sSuffixFile, dialect=csv.excel, delimiter='|')
            for rrow in sSuffixReader:
                if rrow['streetSuffix'] is not None:
                    if rrow['streetSuffix'] not in streetSuffixes:
                        streetSuffixes[rrow['streetSuffix']] = []
                        streetSuffixes[rrow['streetSuffix']].append(re.compile(r'^' + cleanText(rrow['streetSuffix'], True) + r'\b'))
                    if rrow['abbrev'] != rrow['streetSuffix']:
                        streetSuffixes[rrow['streetSuffix']].append(re.compile(r'^' + cleanText(rrow['abbrev'], True) + r'\b'))

    this.logger.info('%d street types and %d street suffixes fetched', len(streetTypes), len(streetSuffixes))

    # Read in the neighbouring localities
    this.logger.info('Fetching neighbouring suburbs')
    nextDoor = []
    if DatabaseType is not None:    # Use the database tables
        dfNeighbour = pd.read_sql_query(text('SELECT date_retired, locality_pid, neighbour_locality_pid FROM LOCALITY_NEIGHBOUR'), engine.connect())
        results = dfNeighbour.values.tolist()
        for (date_retired, locality_pid, neighbour) in results:
            if date_retired is not None:
                continue
            if locality_pid is None:
                continue
            if neighbour is None:
                continue
            nextDoor.append([locality_pid, neighbour])
            nextDoor.append([neighbour, locality_pid])
    elif GNAFdir is not None:       # Use the standard G-NAF CSV files
        # LOCALITY_NEIGHBOUR_PID|DATE_CREATED|DATE_RETIRED|LOCALITY_PID|NEIGHBOUR_LOCALITY_PID
        for SandT in SandTs:
            with open(os.path.join(GNAFdir, 'Standard', SandT + '_LOCALITY_NEIGHBOUR_psv.psv'), 'rt', newline='', encoding='utf-8') as neighbourFile:
                neighbourReader = csv.DictReader(neighbourFile, dialect=csv.excel, delimiter='|')
                for rrow in neighbourReader:
                    if rrow['DATE_RETIRED'] != '':        # Skip if retired
                        continue
                    if rrow['LOCALITY_PID'] == '':
                        continue
                    if rrow['NEIGHBOUR_LOCALITY_PID'] == '':
                        continue
                    nextDoor.append([rrow['LOCALITY_PID'], rrow['NEIGHBOUR_LOCALITY_PID']])
                    nextDoor.append([rrow['NEIGHBOUR_LOCALITY_PID'], rrow['LOCALITY_PID']])
    else:           # Use the optimised CSV files
        # LOCALITY_PID|NEIGHBOUR_LOCALITY_PID
        with open(os.path.join(DataDir, 'neighbours.psv'), 'rt', newline='', encoding='utf-8') as neighbourFile:
            neighbourReader = csv.DictReader(neighbourFile, dialect=csv.excel, delimiter='|')
            for rrow in neighbourReader:
                if rrow['LOCALITY_PID'] == '':
                    continue
                if rrow['NEIGHBOUR_LOCALITY_PID'] == '':
                    continue
                nextDoor.append([rrow['LOCALITY_PID'], rrow['NEIGHBOUR_LOCALITY_PID']])
                nextDoor.append([rrow['NEIGHBOUR_LOCALITY_PID'], rrow['LOCALITY_PID']])

    # Now build up neighbours
    for nxdoor in nextDoor:
        locality_pid = nxdoor[0]
        neighbour = nxdoor[1]
        if locality_pid not in neighbours:
            neighbours[locality_pid] = set()
        neighbours[locality_pid].add(neighbour)
    neighbourCount = 0
    for locality_pid, neighbourList in neighbours.items():
        neighbourCount += len(neighbourList)
    this.logger.info('%d Neighbouring suburbs fetched', neighbourCount)

    # Read in ABS linked postcode and suburb data
    this.logger.info('Fetching suburb and locality data')
    # state_name|postcode|locality_name|SA1_MAINCODE_2016|LGA_CODE_2020|longitude|latitude
    with open(os.path.join(DataDir, 'postcode_SA1LGA.psv'), 'rt', newline='', encoding='utf-8') as SA1LGAfile:
        SA1LGAreader = csv.DictReader(SA1LGAfile, dialect=csv.excel, delimiter='|')
        for rrow in SA1LGAreader:
            stateName = rrow['state_name'].upper()
            if stateName in ['JERVIS BAY TERRITORY', 'EXTERNAL TERRITORY', 'AUSTRALIAN ANTARCTIC TERRITORY']:
                stateName = 'OTHER TERRITORIES'
            postcode = cleanText(rrow['postcode'], True)
            suburb = rrow['locality_name']
            statePid = None
            for thisState, stateInfo in states.items():              # Look for an exact match
                for pattern in stateInfo[1:]:
                    match = pattern.match(stateName)
                    if (match is not None) and (match.start() == 0) and (match.end() == len(stateName)):
                        # Perfect match - state found
                        statePid = thisState
                        break
                else:
                    continue
                break
            else:
                this.logger.warning('Invalid state(%s) for suburb(%s) in postcodeSA1LGA.psv file', str(rrow['state_name'].upper()), str(suburb))
                continue
            sa1 = rrow['SA1_MAINCODE_2016']
            lga = rrow['LGA_CODE_2020']
            longitude = rrow['longitude']
            latitude = rrow['latitude']
            addPostcode(this, postcode, suburb, statePid, sa1, lga, latitude, longitude)

    # Read in any extra postcode and suburb - if required
    if addExtras:
        # state_name|postcode|locality_name|SA1_MAINCODE_2016|LGA_CODE_2020|longitude|latitude
        with open(os.path.join(DataDir, 'extraPostcodeSA1LGA.psv'), 'rt', newline='', encoding='utf-8') as SA1LGAfile:
            SA1LGAreader = csv.DictReader(SA1LGAfile, dialect=csv.excel, delimiter='|')
            for rrow in SA1LGAreader:
                stateName = rrow['state_name'].upper()
                if stateName in ['JERVIS BAY TERRITORY', 'EXTERNAL TERRITORY', 'AUSTRALIAN ANTARCTIC TERRITORY']:
                    stateName = 'OTHER TERRITORIES'
                postcode = cleanText(rrow['postcode'], True)
                suburb = rrow['locality_name']
                statePid = None
                for thisState, stateInfo in states.items():              # Look for an exact match
                    for pattern in stateInfo[1:]:
                        match = pattern.match(stateName)
                        if (match is not None) and (match.start() == 0) and (match.end() == len(stateName)):
                            # Perfect match - state found
                            statePid = thisState
                            break
                    else:
                        continue
                    break
                else:
                    this.logger.warning('Invalid state(%s) for suburb(%s) in postcodeSA1LGA.psv file', str(rrow['state_name'].upper()), str(suburb))
                    continue
                sa1 = rrow['SA1_MAINCODE_2016']
                lga = rrow['LGA_CODE_2020']
                longitude = rrow['longitude']
                latitude = rrow['latitude']
                addPostcode(this, postcode, suburb, statePid, sa1, lga, latitude, longitude)

    # Read in the suburbs (locality names) and create regular expressions so we can look for them.
    this.logger.info('Fetching suburbs')
    if DatabaseType is not None:    # Use the database tables
        dfLocality = pd.read_sql_query(text('SELECT locality_pid, date_retired, locality_name, state_pid, primary_postcode, \'P\' as alias FROM LOCALITY UNION SELECT locality_pid, date_retired, name as locality_name, state_pid, postcode, \'A\' as alias FROM LOCALITY_ALIAS'), engine.connect())
        results = dfLocality.values.tolist()
        for (locality_pid, date_retired, suburb, state_pid, postcode, alias) in results:
            if date_retired is not None:
                continue
            addLocality(this, locality_pid, suburb, postcode, state_pid, alias)
    elif GNAFdir is not None:       # Use the standard G-NAF CSV files
        # LOCALITY_PID|DATE_CREATED|DATE_RETIRED|LOCALITY_NAME|PRIMARY_POSTCODE|LOCALITY_CLASS_CODE|STATE_PID|GNAF_LOCALITY_PID|GNAF_RELIABILITY_CODE
        for SandT in SandTs:
            with open(os.path.join(GNAFdir, 'Standard', SandT + '_LOCALITY_psv.psv'), 'rt', newline='', encoding='utf-8') as suburbFile:
                suburbReader = csv.DictReader(suburbFile, dialect=csv.excel, delimiter='|')
                for rrow in suburbReader:
                    if rrow['DATE_RETIRED'] != '':        # Skip if retired
                        continue
                    localityPid = rrow['LOCALITY_PID']
                    suburb = cleanText(rrow['LOCALITY_NAME'], True)
                    postcode = rrow['PRIMARY_POSTCODE']
                    statePid = rrow['STATE_PID']
                    addLocality(this, localityPid, suburb, postcode, statePid, 'P')
        # LOCALITY_ALIAS_PID|DATE_CREATED|DATE_RETIRED|LOCALITY_PID|NAME|POSTCODE|ALIAS_TYPE_CODE|STATE_PID
        for SandT in SandTs:
            with open(os.path.join(GNAFdir, 'Standard', SandT + '_LOCALITY_ALIAS_psv.psv'), 'rt', newline='', encoding='utf-8') as suburbFile:
                suburbReader = csv.DictReader(suburbFile, dialect=csv.excel, delimiter='|')
                for rrow in suburbReader:
                    if rrow['DATE_RETIRED'] != '':        # Skip if retired
                        continue
                    localityPid = rrow['LOCALITY_PID']
                    suburb = cleanText(rrow['NAME'], True)
                    postcode = rrow['POSTCODE']
                    statePid = rrow['STATE_PID']
                    addLocality(this, localityPid, suburb, postcode, statePid, 'A')

    else:           # Use the optimised PSV files
        # LOCALITY_PID|LOCALITY_NAME|PRIMARY_POSTCODE|STATE_PID|ALIAS
        with open(os.path.join(DataDir, 'locality.psv'), 'rt', newline='', encoding='utf-8') as localityFile:
            localityReader = csv.DictReader(localityFile, dialect=csv.excel, delimiter='|')
            for rrow in localityReader:
                localityPid = rrow['LOCALITY_PID']
                suburb = cleanText(rrow['LOCALITY_NAME'], True)
                postcode = rrow['PRIMARY_POSTCODE']
                statePid = rrow['STATE_PID']
                alias = rrow['ALIAS']
                addLocality(this, localityPid, suburb, postcode, statePid, alias)

    # Read in any extra localities - if required
    if addExtras:
        # locality_pid|locality_name|postcode|state_pid|alias
        with open(os.path.join(DataDir, 'extraLocality.psv'), 'rt', newline='', encoding='utf-8') as localityFile:
            localityReader = csv.DictReader(localityFile, dialect=csv.excel, delimiter='|')
            for rrow in localityReader:
                localityPid = rrow['locality_pid']
                suburb = cleanText(rrow['locality_name'], True)
                postcode = rrow['postcode']
                statePid = rrow['state_pid']
                alias = rrow['alias']
                addLocality(this, localityPid, suburb, postcode, statePid, alias)

    # Next read in the G-NAF locality data linked to ABS SA1 and LGA - locality_SA1LGA.psv
    # locality_pid|SA1_MAINCODE_2016|LGA_CODE_2020|longitude|latitude
    with open(os.path.join(DataDir, 'locality_SA1LGA.psv'), 'rt', newline='', encoding='utf-8') as SA1File:
        SA1Reader = csv.DictReader(SA1File, dialect=csv.excel, delimiter='|')
        for rrow in SA1Reader:
            localityPid = rrow['locality_pid']
            if localityPid not in localities:
                continue
            for statePid, suburb, alias in localities[localityPid]:
                sa1 = rrow['SA1_MAINCODE_2016']
                lga = rrow['LGA_CODE_2020']
                longitude = rrow['longitude']
                latitude = rrow['latitude']
                addSuburb(this, localityPid, statePid, suburb, alias, sa1, lga, latitude, longitude)
    countOfSuburbs = 0
    for soundCode, suburbList in suburbs.items():
        countOfSuburbs += len(suburbList)
    this.logger.info('%d suburbs and %d localities fetched', countOfSuburbs, len(localities))

    # Read in street data
    this.logger.info('Fetching street names data')
    if DatabaseType is not None:    # Use the database tables
        # Read in the street names
        dfStreetLocality = pd.read_sql_query(text('SELECT street_locality_pid, street_name, street_type_code, street_suffix_code, locality_pid FROM STREET_LOCALITY WHERE date_retired IS NULL'), engine.connect())
        results = dfStreetLocality.values.tolist()
        for (streetPid, streetName, streetType, streetSuffix, localityPid) in results:
            addStreetName(this, streetPid, cleanText(streetName, True), cleanText(streetType, True), cleanText(streetSuffix, True), localityPid, 'P')

        dfStreetLocality = pd.read_sql_query(text('SELECT street_locality_pid, street_name, street_type_code, street_suffix_code FROM STREET_LOCALITY_ALIAS WHERE date_retired IS NULL'), engine.connect())
        results = dfStreetLocality.values.tolist()
        for (streetPid, streetName, streetType, streetSuffix) in results:
            if streetPid not in streetNames:
                continue
            localityPid = streetNames[streetPid][0][3]
            addStreetName(this, streetPid, cleanText(streetName, True), cleanText(streetType, True), cleanText(streetSuffix, True), localityPid, 'A')

    elif GNAFdir is not None:       # Use the standard G-NAF PSV files
        # STREET_LOCALITY_PID|DATE_CREATED|DATE_RETIRED|STREET_CLASS_CODE|STREET_NAME|STREET_TYPE_CODE|STREET_SUFFIX_CODE|LOCALITY_PID|GNAF_STREET_PID|GNAF_STREET_CONFIDENCE|GNAF_RELIABILITY_CODE
        for SandT in SandTs:
            with open(os.path.join(GNAFdir, 'Standard', SandT + '_STREET_LOCALITY_psv.psv'), 'rt', newline='', encoding='utf-8') as streetFile:
                streetReader = csv.DictReader(streetFile, dialect=csv.excel, delimiter='|')
                for rrow in streetReader:
                    if rrow['DATE_RETIRED'] != '':        # Skip if retired
                        continue
                    streetPid = rrow['STREET_LOCALITY_PID']
                    streetName = cleanText(rrow['STREET_NAME'], True)
                    streetType = cleanText(rrow['STREET_TYPE_CODE'], True)
                    streetSuffix = cleanText(rrow['STREET_SUFFIX_CODE'], True)
                    localityPid = rrow['LOCALITY_PID']
                    addStreetName(this, streetPid, streetName, streetType, streetSuffix, localityPid, 'P')

        # STREET_LOCALITY_ALIAS_PID|DATE_CREATED|DATE_RETIRED|STREET_LOCALITY_PID|STREET_NAME|STREET_TYPE_CODE|STREET_SUFFIX_CODE|ALIAS_TYPE_CODE
        for SandT in SandTs:
            with open(os.path.join(GNAFdir, 'Standard', SandT + '_STREET_LOCALITY_ALIAS_psv.psv'), 'rt', newline='', encoding='utf-8') as streetFile:
                streetReader = csv.DictReader(streetFile, dialect=csv.excel, delimiter='|')
                for rrow in streetReader:
                    if rrow['DATE_RETIRED'] != '':        # Skip if retired
                        continue
                    streetPid = rrow['STREET_LOCALITY_PID']
                    streetName = cleanText(rrow['STREET_NAME'], True)
                    streetType = cleanText(rrow['STREET_TYPE_CODE'], True)
                    streetSuffix = cleanText(rrow['STREET_SUFFIX_CODE'], True)
                    if streetPid not in streetNames:
                        continue
                    localityPid = streetNames[streetPid][0][3]
                    addStreetName(this, streetPid, streetName, streetType, streetSuffix, localityPid, 'A')

    else:           # Use the optimised PSV files
        # STREET_LOCALITY_PID|STREET_NAME|STREET_TYPE_CODE|STREET_SUFFIX_CODE|LOCALITY_PID
        with open(os.path.join(DataDir, 'street_details.psv'), 'rt', newline='', encoding='utf-8') as street_detailsFile:
            street_detailsReader = csv.DictReader(street_detailsFile, dialect=csv.excel, delimiter='|')
            for rrow in street_detailsReader:
                streetPid = rrow['STREET_LOCALITY_PID']
                streetName = cleanText(rrow['STREET_NAME'], True)
                streetType = cleanText(rrow['STREET_TYPE_CODE'], True)
                streetSuffix = cleanText(rrow['STREET_SUFFIX_CODE'], True)
                localityPid = rrow['LOCALITY_PID']
                addStreetName(this, streetPid, streetName, streetType, streetSuffix, localityPid, 'P')
        # STREET_LOCALITY_PID|STREET_NAME|STREET_TYPE_CODE|STREET_SUFFIX_CODE
        with open(os.path.join(DataDir, 'street_details_alias.psv'), 'rt', newline='', encoding='utf-8') as street_detailsFile:
            street_detailsReader = csv.DictReader(street_detailsFile, dialect=csv.excel, delimiter='|')
            for rrow in street_detailsReader:
                streetPid = rrow['STREET_LOCALITY_PID']
                streetName = cleanText(rrow['STREET_NAME'], True)
                streetType = cleanText(rrow['STREET_TYPE_CODE'], True)
                streetSuffix = cleanText(rrow['STREET_SUFFIX_CODE'], True)
                if streetPid not in streetNames:
                    continue
                localityPid = streetNames[streetPid][0][3]
                addStreetName(this, streetPid, streetName, streetType, streetSuffix, localityPid, 'A')
    streetCount = 0
    for street_pid, namesList in streetNames.items():
        streetCount += len(namesList)
    this.logger.info('%d street names fetched', streetCount)

    # Read in street SA1/LGA data
    this.logger.info('Fetching street SA1/LGA data')
    # street_locality_pid|SA1_MAINCODE_2016|LGA_CODE_2020|longitude|latitude
    streetCount = 0
    with open(os.path.join(DataDir, 'street_SA1LGA.psv'), 'rt', newline='', encoding='utf-8') as SA1LGAfile:
        SA1LGAreader = csv.DictReader(SA1LGAfile, dialect=csv.excel, delimiter='|')
        for rrow in SA1LGAreader:
            streetCount += 1
            streetPid = rrow['street_locality_pid']
            sa1 = rrow['SA1_MAINCODE_2016']
            lga = rrow['LGA_CODE_2020']
            longitude = rrow['longitude']
            latitude = rrow['latitude']
            addStreet(this, streetPid, sa1, lga, latitude, longitude)
    this.logger.info('%d street SA1s/LGAs fetched', streetCount)

    # Read in street numbers
    this.logger.info('Fetching street numbers')
    if DatabaseType is not None:    # Use the database tables
        # We need some mesh block stuff
        addressMB = {}
        dfMB = pd.read_sql_query(text('SELECT address_detail_pid, mb_2016_pid FROM ADDRESS_MESH_BLOCK_2016 WHERE date_retired IS NULL'), engine.connect())
        results = dfMB.values.tolist()
        for (addressPid, mb_2016_pid) in results:
            addressMB[addressPid] = mb_2016_pid
        MB = {}
        dfMB = pd.read_sql_query(text('SELECT mb_2016_pid, mb_2016_code FROM MB_2016 WHERE date_retired IS NULL'), engine.connect())
        results = dfMB.values.tolist()
        for (mb_2016_pid, mb_2016_code) in results:
            MB[mb_2016_pid] = mb_2016_code

        # And some default geocode stuff
        defaultGeocode = {}
        dfMB = pd.read_sql_query(text('SELECT address_detail_pid, longitude, latitude FROM ADDRESS_DEFAULT_GEOCODE WHERE date_retired IS NULL'), engine.connect())
        results = dfMB.values.tolist()
        for (address_detail_pid, longitude, latitude) in results:
            defaultGeocode[address_detail_pid] = (str(latitude), str(longitude))


        # And then the address details
        dfAddr = pd.read_sql_query(text('SELECT address_detail_pid, building_name, lot_number, number_first, number_last, street_locality_pid, locality_pid, postcode, alias_principal FROM ADDRESS_DETAIL WHERE confidence > 0 AND date_retired IS NULL'), engine.connect())
        results = dfAddr.values.tolist()
        for (addressPid, buildingName, lotNumber, numberFirst, numberLast, streetPid, localityPid, postcode, alias) in results:
            mbCode = None
            if (addressPid in addressMB) and (addressMB[addressPid] in MB):
                buildingName = cleanText(buildingName, True)
                mbCode = MB[addressMB[addressPid]]
                try:
                    lotNumber = int(lotNumber)
                except (ValueError, TypeError):
                    lotNumber = None
                try:
                    numberFirst = int(numberFirst)
                except (ValueError, TypeError):
                    numberFirst = None
                try:
                    numberLast = int(numberLast)
                except (ValueError, TypeError):
                    numberLast = None
            longitude = None
            latitude = None
            if addressPid in defaultGeocode:
                latitude, longitude = defaultGeocode[addressPid]
            addStreetNumber(this, buildingName, streetPid, localityPid, lotNumber, numberFirst, numberLast, mbCode, latitude, longitude, addressPid)

    elif GNAFdir is not None:       # Use the standard G-NAF PSV files
        # We need some mesh block stuff
        # ADDRESS_MESH_BLOCK_2016_PID|DATE_CREATED|DATE_RETIRED|ADDRESS_DETAIL_PID|MB_MATCH_CODE|MB_2016_PID
        addressMB = {}
        for SandT in SandTs:
            with open(os.path.join(GNAFdir, 'Standard', SandT + '_ADDRESS_MESH_BLOCK_2016_psv.psv'), 'rt', newline='', encoding='utf-8') as mbFile:
                mbReader = csv.DictReader(mbFile, dialect=csv.excel, delimiter='|')
                for rrow in mbReader:
                    if rrow['DATE_RETIRED'] != '':        # Skip if retired
                        continue
                    addressMB[rrow['ADDRESS_DETAIL_PID']] = rrow['MB_2016_PID']
        MB = {}
        # MB_2016_PID|DATE_CREATED|DATE_RETIRED|MB_2016_CODE
        for SandT in SandTs:
            with open(os.path.join(GNAFdir, 'Standard', SandT + '_MB_2016_psv.psv'), 'rt', newline='', encoding='utf-8') as mbFile:
                mbReader = csv.DictReader(mbFile, dialect=csv.excel, delimiter='|')
                for rrow in mbReader:
                    if rrow['DATE_RETIRED'] != '':        # Skip if retired
                        continue
                    MB[rrow['MB_2016_PID']] = rrow['MB_2016_CODE']
        # And some default geocode stuff
        defaultGeocode = {}
        # ADDRESS_DEFAULT_GEOCODE_PID|DATE_CREATED|DATE_RETIRED|ADDRESS_DETAIL_PID|GEOCODE_TYPE_CODE|LONGITUDE|LATITUDE
        for SandT in SandTs:
            with open(os.path.join(GNAFdir, 'Standard', SandT + '_ADDRESS_DEFAULT_GEOCODE_psv.psv'), 'rt', newline='', encoding='utf-8') as defaultGeoFile:
                defaultGeoReader = csv.DictReader(defaultGeoFile, dialect=csv.excel, delimiter='|')
                for rrow in defaultGeoReader:
                    if rrow['DATE_RETIRED'] != '':        # Skip if retired
                        continue
                    addressPid = rrow['ADDRESS_DETAIL_PID']
                    defaultGeocode[addressPid] = (rrow['LATITUDE'], rrow['LONGITUDE'])
        # And then the address details
        # ADDRESS_DETAIL_PID|DATE_CREATED|DATE_LAST_MODIFIED|DATE_RETIRED|BUILDING_NAME|LOT_NUMBER_PREFIX|LOT_NUMBER|LOT_NUMBER_SUFFIX|FLAT_TYPE_CODE|FLAT_NUMBER_PREFIX|FLAT_NUMBER|FLAT_NUMBER_SUFFIX|LEVEL_TYPE_CODE|LEVEL_NUMBER_PREFIX|LEVEL_NUMBER|LEVEL_NUMBER_SUFFIX|NUMBER_FIRST_PREFIX|NUMBER_FIRST|NUMBER_FIRST_SUFFIX|NUMBER_LAST_PREFIX|NUMBER_LAST|NUMBER_LAST_SUFFIX|STREET_LOCALITY_PID|LOCATION_DESCRIPTION|LOCALITY_PID|ALIAS_PRINCIPAL|POSTCODE|PRIVATE_STREET|LEGAL_PARCEL_ID|CONFIDENCE|ADDRESS_SITE_PID|LEVEL_GEOCODED_CODE|PROPERTY_PID|GNAF_PROPERTY_PID|PRIMARY_SECONDARY
        # NOTE: ADDRESS_DETAIL contains a lot more than just postcodes, so we try and grab a much as we can in one pass
        for SandT in SandTs:
            with open(os.path.join(GNAFdir, 'Standard', SandT + '_ADDRESS_DETAIL_psv.psv'), 'rt', newline='', encoding='utf-8') as addressFile:
                addressReader = csv.DictReader(addressFile, dialect=csv.excel, delimiter='|')
                for rrow in addressReader:
                    if rrow['DATE_RETIRED'] != '':        # Skip if retired
                        continue
                    confidence = rrow['CONFIDENCE']
                    try:
                        confidence = int(confidence)
                    except (ValueError, TypeError):
                        confidence = 0
                    if confidence < 1:
                        continue
                    addressPid = rrow['ADDRESS_DETAIL_PID']
                    mbCode = None
                    if (addressPid in addressMB) and (addressMB[addressPid] in MB):
                        mbCode = MB[addressMB[addressPid]]
                    buildingName = cleanText(rrow['BUILDING_NAME'], True)
                    try:
                        lotNumber = int(rrow['LOT_NUMBER'])
                    except (ValueError, TypeError):
                        lotNumber = None
                    try:
                        numberFirst = int(rrow['NUMBER_FIRST'])
                    except (ValueError, TypeError):
                        numberFirst = None
                    try:
                        numberLast = int(rrow['NUMBER_LAST'])
                    except (ValueError, TypeError):
                        numberLast = None
                    streetPid = rrow['STREET_LOCALITY_PID']
                    localityPid = rrow['LOCALITY_PID']
                    longitude = None
                    latitude = None
                    if addressPid in defaultGeocode:
                        latitude, longitude = defaultGeocode[addressPid]
                    addStreetNumber(this, buildingName, streetPid, localityPid, lotNumber, numberFirst, numberLast, mbCode, latitude, longitude, addressPid)

    else:           # Use the optimised PSV files
        # We need some mesh block stuff
        addressMB = {}
        # ADDRESS_DETAIL_PID|MB_2016_PID
        with open(os.path.join(DataDir, 'addressMB.psv'), 'rt', newline='', encoding='utf-8') as addressMBFile:
            addressMBReader = csv.DictReader(addressMBFile, dialect=csv.excel, delimiter='|')
            for rrow in addressMBReader:
                addressMB[rrow['ADDRESS_DETAIL_PID']] = rrow['MB_2016_PID']
        MB = {}
        # MB_2016_PID|MB_2016_CODE
        with open(os.path.join(DataDir, 'MB.psv'), 'rt', newline='', encoding='utf-8') as MBFile:
            MBReader = csv.DictReader(MBFile, dialect=csv.excel, delimiter='|')
            for rrow in MBReader:
                MB[rrow['MB_2016_PID']] = rrow['MB_2016_CODE']
        # And some default geocode stuff
        defaultGeocode = {}
        # ADDRESS_DETAIL_PID|LONGITUDE|LATITUDE
        with open(os.path.join(DataDir, 'address_default_geocode.psv'), 'rt', newline='', encoding='utf-8') as defaultGeocodeFile:
            defaultGeocodeReader = csv.DictReader(defaultGeocodeFile, dialect=csv.excel, delimiter='|')
            for rrow in defaultGeocodeReader:
                defaultGeocode[rrow['ADDRESS_DETAIL_PID']] = (rrow['LATITUDE'], rrow['LONGITUDE'])
        # And then the address details
        # LOCALITY_PID|BUILDING_NAME|CONFIDENCE|POSTCODE|ADDRESS_DETAIL_PID|STREET_LOCALITY_PID|LOT_NUMBER|NUMBER_FIRST|NUMBER_LAST|ALIAS_PRINCIPAL|ADDRESS_SITE_PID
        with open(os.path.join(DataDir, 'address_detail.psv'), 'rt', newline='', encoding='utf-8') as address_detailFile:
            address_detailReader = csv.DictReader(address_detailFile, dialect=csv.excel, delimiter='|')
            for rrow in address_detailReader:
                localityPid = rrow['LOCALITY_PID']
                buildingName = cleanText(rrow['BUILDING_NAME'], True)
                confidence = rrow['CONFIDENCE']
                try:
                    confidence = int(confidence)
                except (ValueError, TypeError):
                    confidence = 0
                if confidence < 1:
                    continue
                addressPid = rrow['ADDRESS_DETAIL_PID']
                mbCode = None
                if (addressPid in addressMB) and (addressMB[addressPid] in MB):
                    mbCode = MB[addressMB[addressPid]]
                streetPid = rrow['STREET_LOCALITY_PID']
                try:
                    lotNumber = int(rrow['LOT_NUMBER'])
                except (ValueError, TypeError):
                    lotNumber = None
                try:
                    numberFirst = int(rrow['NUMBER_FIRST'])
                except (ValueError, TypeError):
                    numberFirst = None
                try:
                    numberLast = int(rrow['NUMBER_LAST'])
                except (ValueError, TypeError):
                    numberLast = None
                longitude = None
                latitude = None
                if addressPid in defaultGeocode:
                    latitude, longitude = defaultGeocode[addressPid]
                addStreetNumber(this, buildingName, streetPid, localityPid, lotNumber, numberFirst, numberLast, mbCode, latitude, longitude, addressPid)
    numbersCount = 0
    for street_pid, numbersList in streetNos.items():
        numbersCount += len(numbersList)
    this.logger.info('%d street numbers fetched', numbersCount)

    this.logger.info('Fetching flats, units and trims')
    if DatabaseType is not None:    # Use the database tables
        dfFlat = pd.read_sql_query(text('SELECT code, name, description FROM FLAT_TYPE_AUT'), engine.connect())
        results = dfFlat.values.tolist()
        for rrow in results:
            for flat in rrow:
                if flat is not None:
                    flats.append(re.compile(r'\b' + cleanText(flat, True) + r'( *' + deliveryNumber + r'\s*)'))
                    flats.append(re.compile(r'\b' + cleanText(flat, True) + r'S( *' + deliveryRange + r'\s*)'))
        dfLevel = pd.read_sql_query(text('SELECT code, name, description FROM LEVEL_TYPE_AUT'), engine.connect())
        results = dfLevel.values.tolist()
        for rrow in results:
            for level in rrow:
                if level is not None:
                    levels.append(re.compile(r'\b' + cleanText(level, True) + r'( *' + deliveryNumber + r'\s*)'))
    elif GNAFdir is not None:       # Use the standard G-NAF PSV files
        # CODE|NAME|DESCRIPTION
        with open(os.path.join(GNAFdir, 'Authority Code', 'Authority_Code_FLAT_TYPE_AUT_psv.psv'), 'rt', newline='', encoding='utf-8') as flatFile:
            flatReader = csv.DictReader(flatFile, dialect=csv.excel, delimiter='|')
            for rrow in flatReader:
                for flat in rrow.values():
                    if flat != '':
                        flats.append(re.compile(r'\b' + cleanText(flat, True) + r'( *' + deliveryNumber + r'\s*)'))
                        flats.append(re.compile(r'\b' + cleanText(flat, True) + r'S( *' + deliveryRange + r'\s*)'))
        # CODE|NAME|DESCRIPTION
        with open(os.path.join(GNAFdir, 'Authority Code', 'Authority_Code_LEVEL_TYPE_AUT_psv.psv'), 'rt', newline='', encoding='utf-8') as levelFile:
            levelReader = csv.DictReader(levelFile, dialect=csv.excel, delimiter='|')
            for rrow in levelReader:
                for level in rrow.values():
                    if level != '':
                        levels.append(re.compile(r'\b' + cleanText(level, True) + r'( *' + deliveryNumber + r'\s*)'))
    else:           # Use the optimised PSV files
        # code
        with open(os.path.join(DataDir, 'address_flat.psv'), 'rt', newline='', encoding='utf-8') as flatFile:
            flatReader = csv.DictReader(flatFile, dialect=csv.excel)
            for rrow in flatReader:
                flats.append(re.compile(r'\b' + cleanText(rrow['code'], True) + r'( *' + deliveryNumber + r'\s*)'))
                flats.append(re.compile(r'\b' + cleanText(rrow['code'], True) + r'S( *' + deliveryRange + r'\s*)'))
        with open(os.path.join(DataDir, 'address_level.psv'), 'rt', newline='', encoding='utf-8') as levelFile:
            levelReader = csv.DictReader(levelFile, dialect=csv.excel)
            for rrow in levelReader:
                levels.append(re.compile(r'\b' + cleanText(rrow['code'], True) + r'( *' + deliveryNumber + r'\s*)'))

    # Read in any extra flats, levels or trims - if required
    if addExtras:
        if os.path.isfile(os.path.join(DataDir, 'extraFlats.psv')):
            # code
            with open(os.path.join(DataDir, 'extraFlats.psv'), 'rt', newline='', encoding='utf-8') as flatFile:
                flatReader = csv.DictReader(flatFile, dialect=csv.excel)
                for rrow in flatReader:
                    extraTrims.append(re.compile(r'\b' + cleanText(rrow['code'], True) + r'( *' + deliveryNumber + r'\s*'))
        if os.path.isfile(os.path.join(DataDir, 'extraLevels.psv')):
            # code
            with open(os.path.join(DataDir, 'extraLevels.psv'), 'rt', newline='', encoding='utf-8') as levelFile:
                levelReader = csv.DictReader(levelFile, dialect=csv.excel)
                for rrow in levelReader:
                    extraTrims.append(re.compile(r'\b' + cleanText(rrow['code'], True) + r'( *' + deliveryNumber + r'\s*'))
        if os.path.isfile(os.path.join(DataDir, 'extraTrims.psv')):
            # code
            with open(os.path.join(DataDir, 'extraTrims.psv'), 'rt', newline='', encoding='utf-8') as trimFile:
                trimReader = csv.DictReader(trimFile, dialect=csv.excel)
                for rrow in trimReader:
                    extraTrims.append(re.compile(r'\b' + cleanText(rrow['code'], True) + r'\s*'))

    this.logger.info('%d extra flats, units and trims fetched', len(extraTrims))

    this.logger.info('Fetching postal delivery services')
    # Use the  PSV files
    if os.path.isfile(os.path.join(DataDir, 'serviceDelivery.psv')):
        # Code|Cardinality
        with open(os.path.join(DataDir, 'serviceDelivery.psv'), 'rt', newline='', encoding='utf-8') as deliveryFile:
            deliveryReader = csv.DictReader(deliveryFile, dialect=csv.excel, delimiter='|')
            for rrow in deliveryReader:
                code = cleanText(rrow['Code'], True)
                code = period.sub(r'\.', code)
                code = oneSpace.sub(' ', code)
                cardinality = rrow['Cardinality']
                if cardinality == '0':
                    services.append([re.compile(r'\b' + code + r'\s*'), cardinality])
                elif cardinality == '*':
                    services.append([re.compile(r'\b' + code + r'( *' + deliveryNumber + r')?\s*'), cardinality])
                else:
                    services.append([re.compile(r'\b' + code + r'( *' + deliveryNumber + r')\s*'), cardinality])
    this.logger.info('%d postal delivery services fetched', len(services))

    # Read in SA1 and LGA data
    this.logger.info('Fetching Mesh Block SA1 and LGA codes')
    if GNAFdir is not None:       # Use the standard ABS Mesh Block and LGA csv files
        # MB_CODE_2016,MB_CATEGORY_NAME_2016,SA1_MAINCODE_2016,SA1_7DIGITCODE_2016,SA2_MAINCODE_2016,SA2_5DIGITCODE_2016,SA2_NAME_2016,SA3_CODE_2016,SA3_NAME_2016,SA4_CODE_2016,SA4_NAME_2016,GCCSA_CODE_2016,GCCSA_NAME_2016,STATE_CODE_2016,STATE_NAME_2016,AREA_ALBERS_SQKM
        for SandT in SandTs:
            with open(os.path.join(ABSdir, 'MB', 'MB_2016_' + SandT + '.csv'), 'rt', newline='', encoding='utf-8') as mbFile:
                mbReader = csv.DictReader(mbFile, dialect=csv.excel)
                for rrow in mbReader:
                    SA1map[rrow['MB_CODE_2016']] = rrow['SA1_MAINCODE_2016']

        # MB_CODE_2016,LGA_CODE_2020,LGA_NAME_2020,STATE_CODE_2016,STATE_NAME_2016,AREA_ALBERS_SQKM
        for SandT in SandTs:
            with open(os.path.join(ABSdir, 'LGA', 'LGA_2020_' + SandT + '.csv'), 'rt', newline='', encoding='utf-8') as lgaFile:
                lgaReader = csv.DictReader(lgaFile, dialect=csv.excel)
                for rrow in lgaReader:
                    LGAmap[rrow['MB_CODE_2016']] = rrow['LGA_CODE_2020']
    else:   # Use the optimised CSV files
        # Read in SA1 data
        # MB_CODE_2016,SA1_MAINCODE_2016
        with open(os.path.join(DataDir, 'sa1.csv'), 'rt', newline='', encoding='utf-8') as mbFile:
            mbReader = csv.DictReader(mbFile, dialect=csv.excel)
            for rrow in mbReader:
                SA1map[rrow['MB_CODE_2016']] = rrow['SA1_MAINCODE_2016']

        # Read in LGA data
        # MB_CODE_2016,LGA_CODE_2020
        with open(os.path.join(DataDir, 'lga.csv'), 'rt', newline='', encoding='utf-8') as mbFile:
            mbReader = csv.DictReader(mbFile, dialect=csv.excel)
            for rrow in mbReader:
                LGAmap[rrow['MB_CODE_2016']] = rrow['LGA_CODE_2020']

    this.logger.info('%d Mesh Blocks and %d LGA codes fetched', len(SA1map), len(LGAmap))

    this.logger.info('Finished initializing data')

    return


def setupAddress1Address2(this, buildingName):
    '''
Assign addressLine1 and addressLine 2
    '''

    this.logger.debug('setupAddress1Address2 - isPostalService(%s), trim(%s), houseTrim(%s), houseNo(%s)',
                      this.isPostalService, this.trim, repr(this.houseTrim), repr(this.houseNo))

    if this.isPostalService:
        this.result['addressLine1'] = this.postalServiceText1
        this.result['addressLine2'] = this.postalServiceText2
        if returnBoth:
            this.result['addressLine1Abbrev'] = this.postalServiceText1
            this.result['addressLine2Abbrev'] = this.postalServiceText2
    elif this.trim is not None:            # Trim is everything up to the house number, houseTrim is the number
        # There is address Trim so put that in line 1 and the house number and street in line 2
        this.result['addressLine1'] = this.trim                                                        # Line 1 is the trim
        if returnBoth:
            this.result['addressLine1Abbrev'] = this.trim                                            # Abbrev Line 1 is the trim
        if this.houseTrim is not None:        # a house number
            # We have address Trim and house trim (and hence house number)
            if this.street is not None:
                if buildingName is not None:
                    buildingAt = this.trim.find(buildingName)                               # If this.trim contains the building name then let it be
                    if buildingAt == -1:
                        this.result['addressLine1'] = buildingName + ' ' + this.trim                        # Line 1 is the trim plus the building
                        if returnBoth:
                            this.result['addressLine1Abbrev'] = buildingName  + ' ' + this.trim                       # Line 1 is the trim plus the building
                # We have a street and street type (which may be abbreviated)
                if returnBoth:
                    this.result['addressLine2'] = this.houseTrim + ' ' + this.street                # Line 2 is [houseTrim]house no. + street
                    this.result['addressLine2Abbrev'] = this.houseTrim + ' ' + this.abbrevStreet    # Abbrev Line 2 is [houseTrim]house no. + abbrev street
                elif abbreviate:
                    this.result['addressLine2'] = this.houseTrim + ' ' + this.abbrevStreet            # Line 2 is [houseTrim]house no. + abbrev street
                else:
                    this.result['addressLine2'] = this.houseTrim + ' ' + this.street                # Line 2 is [houseTrim]house no. + street
            else:
                # We have address Trim and house trim, but no street
                if buildingName is not None:
                    buildingAt = this.trim.find(buildingName)                               # If this.trim contains the building name then let it be
                    if buildingAt == -1:
                        this.result['addressLine2'] = this.houseTrim + ' ' + buildingName                        # Line 1 is the trim plus the houseNo and the building
                        if returnBoth:
                            this.result['addressLine2Abbrev'] = this.houseTrim + ' ' + buildingName                        # Line 1 is the trim plus the houseNo and the building
                else:
                    this.result['addressLine2'] = this.houseTrim                                        # Line 2 is [houseTrim]house no.
                    if returnBoth:
                        this.result['addressLine2Abbrev'] = this.houseTrim                                # Abbrev Line 2 is [houseTrim]house no.
        else:   # No house trim (house number)
            if this.street is not None:
                if buildingName is not None:
                    buildingAt = this.trim.find(buildingName)                               # If this.trim contains the building name then let it be
                    if buildingAt == -1:
                        this.result['addressLine1'] += ' ' + buildingName                        # Line 1 is the trim plus the building
                        if returnBoth:
                            this.result['addressLine1Abbrev'] += ' ' + buildingName                        # Line 1 is the trim plus the building
                # We have a street and street type (which may be abbreviated)
                if returnBoth:
                    this.result['addressLine2'] = this.street                                            # Line 2 is street
                    this.result['addressLine2Abbrev'] = this.abbrevStreet                            # Abbrev Line 2 is abbrev street
                elif abbreviate:
                    this.result['addressLine2'] = this.abbrevStreet                                            # Line 2 is abbrev street
                else:
                    this.result['addressLine2'] = this.street                                            # Line 2 is street
            else:
                # We have no house trim and no street
                this.result['addressLine2'] = ''                                                    # Line 2 is blank
                if returnBoth:
                    this.result['addressLine2Abbrev'] = ''                                            # Abbrev Line 2 is blak
    elif buildingName is not None:            # Put building name in address1
        if this.houseTrim is not None:        # a house number
            # We have address Trim and house trim (and hence house number)
            if this.street is not None:
                this.result['addressLine1'] = buildingName                        # Line 1 is the trim plus the houseNo and the building
                if returnBoth:
                    this.result['addressLine1Abbrev'] = buildingName                        # Line 1 is the trim plus the houseNo and the building
                # We have a street and street type (which may be abbreviated)
                if returnBoth:
                    this.result['addressLine2'] = this.houseTrim + ' ' + this.street                # Line 2 is [houseTrim]house no. + street
                    this.result['addressLine2Abbrev'] = this.houseTrim + ' ' + this.abbrevStreet    # Abbrev Line 2 is [houseTrim]house no. + abbrev street
                elif abbreviate:
                    this.result['addressLine2'] = this.houseTrim + ' ' + this.abbrevStreet            # Line 2 is [houseTrim]house no. + abbrev street
                else:
                    this.result['addressLine2'] = this.houseTrim + ' ' + this.street                # Line 2 is [houseTrim]house no. + street
            else:
                # We have house trim, but no street
                this.result['addressLine1'] = this.houseTrim + ' ' + buildingName                        # Line 1 is the trim plus the houseNo and the building
                if returnBoth:
                    this.result['addressLine1Abbrev'] = this.houseTrim + ' ' + buildingName                        # Line 1 is the trim plus the houseNo and the building
                this.result['addressLine2'] = ''                                        # Line 2 is [houseTrim]house no.
                if returnBoth:
                    this.result['addressLine2Abbrev'] = ''                                # Abbrev Line 2 is [houseTrim]house no.
        else:   # No house trim (house number)
            this.result['addressLine1'] = buildingName                        # Line 1 is the trim plus the houseNo and the building
            if returnBoth:
                this.result['addressLine1Abbrev'] = buildingName                        # Line 1 is the trim plus the houseNo and the building
            if this.street is not None:
                # We have a street and street type (which may be abbreviated)
                if returnBoth:
                    this.result['addressLine2'] = this.street                                            # Line 2 is street
                    this.result['addressLine2Abbrev'] = this.abbrevStreet                            # Abbrev Line 2 is abbrev street
                elif abbreviate:
                    this.result['addressLine2'] = this.abbervStreet                                            # Line 2 is abbrev street
                else:
                    this.result['addressLine2'] = this.street                                            # Line 2 is street
            else:
                # We have no house trim and no street
                this.result['addressLine2'] = ''                                                    # Line 2 is blank
                if returnBoth:
                    this.result['addressLine2Abbrev'] = ''                                            # Abbrev Line 2 is blak
    else:                       # No trim
        # There's no trim and no building name so put everything in line 1
        this.result['addressLine2'] = ''                                                            # Line 2 is blank
        if returnBoth:
            this.result['addressLine2Abbrev'] = ''                                                    # Abbrev Line 2 is blank
        if this.houseTrim is not None:
            # We have house trim (a house no.)
            if this.street is not None:
                # We have a street and street type (which may be abbreviated)
                if returnBoth:
                    this.result['addressLine1'] = this.houseTrim + ' ' + this.street                # Line 1 is [houseTrim]house no. + street
                    this.result['addressLine1Abbrev'] = this.houseTrim + ' ' + this.abbrevStreet    # Abbrev Line 1 is [houseTrim]house no. + abbrev street
                elif abbreviate:
                    this.result['addressLine1'] = this.houseTrim + ' ' + this.abbrevStreet            # Line 1 is [houseTrim]house no. + abbrev street
                else:
                    this.result['addressLine1'] = this.houseTrim + ' ' + this.street                # Line 1 is [houseTrim]house no. + street
            else:
                # We have house trim, but no street
                this.result['addressLine1'] = this.houseTrim                                        # Line 1 is [houseTrim]house no.
                if returnBoth:
                    this.result['addressLine1Abbrev'] = this.houseTrim                                # Abbrev Line 1 is [houseTrim]house no.
        else:                       # No house number trim
            if this.street is not None:
                # We have a street and street type (which may be abbreviated)
                if returnBoth:
                    this.result['addressLine1'] = this.street                                        # Line 1 is the street
                    this.result['addressLine1Abbrev'] = this.abbrevStreet                            # Abbrev Line 1 is the abbrev street
                elif abbreviate:
                    this.result['addressLine1'] = this.abbrevStreet                                    # Line 1 is the abbrev street
                else:
                    this.result['addressLine1'] = this.street                                        # Line 1 is the street
            else:
                # We have no house trim and no street
                this.result['addressLine1'] = ''                                                    # Line 1 is blank
                if returnBoth:
                    this.result['addressLine1Abbrev'] = ''                                            # Abbrev Line 1 is blank
    if this.street is not None:
        this.result['street'] = this.street
    else:
        this.result['street'] = ''
    if this.houseNo is not None:
        this.result['houseNo'] = str(this.houseNo)
    else:
        this.result['houseNo'] = ''
    return


def scanForSuburb(this, thisText, direction, isAPI):
    '''
Scan for a suburb in text. Scan in 'direction'
Return anything left over
    '''

    this.logger.debug('Scanning for suburb in (%s)', thisText)
    parts = thisText.split(',')
    for ii in range(len(parts) - 1, -1, -1):
        parts[ii] = parts[ii].strip()
        if parts[ii] == '':
            del parts[ii]
            continue
        if parts[ii].startswith('MT '):
            parts[ii] = 'MOUNT' + parts[ii][2:]
        elif parts[ii].startswith('N '):
            parts[ii] = 'NORTH' + parts[ii][1:]
        elif parts[ii].startswith('N. '):
            parts[ii] = 'NORTH' + parts[ii][2:]
        elif parts[ii].startswith('NTH '):
            parts[ii] = 'NORTH' + parts[ii][3:]
        elif parts[ii].startswith('NTH. '):
            parts[ii] = 'NORTH' + parts[ii][4:]
        if parts[ii].endswith(' N'):
            parts[ii] = parts[ii][:-1] + 'NORTH'
        elif parts[ii].endswith(' N.'):
            parts[ii] = parts[ii][:-2] + 'NORTH'
        elif parts[ii].startswith(' NTH'):
            parts[ii] = parts[ii][:-3] + 'NORTH'
        elif parts[ii].startswith(' NTH.'):
            parts[ii] = parts[ii][:-4] + 'NORTH'
        if parts[ii].startswith('S '):
            parts[ii] = 'SOUTH' + parts[ii][1:]
        elif parts[ii].startswith('S. '):
            parts[ii] = 'SOUTH' + parts[ii][2:]
        elif parts[ii].startswith('STH '):
            parts[ii] = 'SOUTH' + parts[ii][3:]
        elif parts[ii].startswith('STH. '):
            parts[ii] = 'SOUTH' + parts[ii][4:]
        if parts[ii].endswith(' S'):
            parts[ii] = parts[ii][:-1] + 'SOUTH'
        elif parts[ii].endswith(' S.'):
            parts[ii] = parts[ii][:-2] + 'SOUTH'
        elif parts[ii].startswith(' STH'):
            parts[ii] = parts[ii][:-3] + 'SOUTH'
        elif parts[ii].startswith(' STH.'):
            parts[ii] = parts[ii][:-4] + 'SOUTH'
        if parts[ii].startswith('E '):
            parts[ii] = 'EAST' + parts[ii][1:]
        elif parts[ii].startswith('E. '):
            parts[ii] = 'EAST' + parts[ii][2:]
        if parts[ii].endswith(' E'):
            parts[ii] = parts[ii][:-1] + 'EAST'
        elif parts[ii].endswith(' E.'):
            parts[ii] = parts[ii][:-2] + 'EAST'
        if parts[ii].startswith('W '):
            parts[ii] = 'WEST' + parts[ii][1:]
        elif parts[ii].startswith('W. '):
            parts[ii] = 'WEST' + parts[ii][2:]
        if parts[ii].endswith(' W'):
            parts[ii] = parts[ii][:-1] + 'WEST'
        elif parts[ii].endswith(' W.'):
            parts[ii] = parts[ii][:-2] + 'WEST'

    for thisPart in parts:
        this.logger.debug('scanForSuburb[%s] - saving parts (%s)', direction, thisPart)
        if direction == 'forwards':
            this.foundSuburbText.append((thisPart, isAPI))       # Add all parts as possible suburbs
        else:
            this.foundSuburbText.insert(0, (thisPart, isAPI))
    if direction == 'forwards':
        partNo = 0
    else:
        partNo = len(parts) - 1
    while ((direction == 'forwards') and (partNo < len(parts)) or ((direction) != 'forwards') and (partNo >= 0)):
        this.logger.debug('scanForSuburb[%s] - scanning part (%s)', direction, parts[partNo])
        subParts = parts[partNo].split(' ')
        firstSubPart = 0
        endSubPart = len(subParts)
        while firstSubPart < endSubPart:
            while firstSubPart < endSubPart:
                thisSuburb = ' '.join(subParts[firstSubPart:endSubPart])
                this.logger.debug('scanForSuburb - scanning subParts(%s)', thisSuburb)
                soundCode = jellyfish.soundex(thisSuburb)
                # Only add exact matches for this foundSuburbText
                if (soundCode in suburbs) and (thisSuburb in suburbs[soundCode]):
                    this.logger.debug('scanForSuburb - adding suburb(%s) to validSuburbs', thisSuburb)
                    if thisSuburb not in this.validSuburbs:
                        this.validSuburbs[thisSuburb] = {}
                        this.validSuburbs[thisSuburb]['SX'] = [soundCode, isAPI]
                    for statePid in suburbs[soundCode][thisSuburb]:
                        if statePid not in this.validSuburbs[thisSuburb]:
                            this.validSuburbs[thisSuburb][statePid] = {}
                        for src in ['G', 'GA', 'A']:            # Only add primary sources
                            if (src in suburbs[soundCode][thisSuburb][statePid]) and (src not in this.validSuburbs[thisSuburb][statePid]):
                                this.logger.info('scanForSuburb - adding source(%s), for state(%s) for suburb(%s) to validSuburbs',
                                                  src, states[statePid][0], thisSuburb)
                                this.logger.debug('scanForSuburb - (%s)', repr(sorted(suburbs[soundCode][thisSuburb][statePid][src])))
                                this.validSuburbs[thisSuburb][statePid][src] = suburbs[soundCode][thisSuburb][statePid][src]
                    for ii in range(endSubPart - 1, firstSubPart - 1, -1):
                        del subParts[ii]
                    if direction == 'forwards':
                        endSubPart = len(subParts)
                    else:
                        endSubPart = firstSubPart
                        firstSubPart = 0
                else:            # Move this sliding point
                    if direction == 'forwards':
                        endSubPart -= 1
                    else:
                        firstSubPart += 1
            if direction == 'forwards':        # Move the anchor point and reset the sliding point
                firstSubPart += 1
                endSubPart = len(subParts)
            else:
                firstSubPart = 0
                endSubPart -= 1
        parts[partNo] = ' '.join(subParts)    # Restore what's left of this part and move on to the nex part
        if direction == 'forwards':
            partNo += 1
        else:
            partNo -= 1
    for ii in range(len(parts) -1, -1, -1):        # Clean up any parts that became empty (removed as they were matched)
        if parts[ii] == '':
            del parts[ii]
    return ' '.join(parts)        # Return what's left


def removeFlats(this, addressLine, trimEnd):
    '''
Remove Flats trim from addressLine
    '''

    if trimEnd == 0:        # Address started with a number
        return

    for flat in flats:
        matched = flat.search(addressLine[:trimEnd])
        if matched is not None:
            if (this.trim is None) or (len(this.trim) < matched.end()):
                this.trim = addressLine[:matched.end()].strip()

    return


def removeLevels(this, addressLine, trimEnd):
    '''
Remove Levels trim from addressLine
    '''

    if trimEnd == 0:        # Address started with a number
        return

    for level in levels:
        matched = level.search(addressLine[:trimEnd])
        if matched is not None:
            if (this.trim is None) or (len(this.trim) < matched.end()):
                this.trim = addressLine[:matched.end()].strip()

    return


def removePostalService(this, addressLine, trimEnd, houseEnd):
    '''
Look for a postal service.
If we have deliveryNumber, then start by checking for services that can/should have a deliveryNumber
which include this deliveryNumber.
If nothing found, then look for postal services than cannot include a deliveryNumber
    '''

    if trimEnd == 0:        # Address started with a number
        return False

    if (houseEnd is not None) and (not this.isLot) and (not this.isRange):
        # We have a deliveryNumber - check if it is a postal service delivery number
        for service in services:
            cardinality = service[1]
            if cardinality == '0':
                continue
            matched = service[0].search(addressLine[:trimEnd + houseEnd])
            if (matched is not None) and (matched.end() == trimEnd + houseEnd):            # This is a postal service delivery address
                this.logger.debug('removePostalService - postal service with delivery address')
                this.isPostalService = True
                this.houseNo = None
                this.result['isPostalService'] = True
                this.postalServiceText1 = addressLine[:matched.end()].strip()
                this.postalServiceText2 = addressLine[matched.end():].strip()
                this.postalService3 = None
                this.trim = this.postalServiceText1
                return True

    # Ignore the delivery number, if there was one, as it isn't a postal service delivery number
    # e.g. could be "RMB 56 86 RIVER ROAD"
    # Here "86" is the identified deliveryNumber. We need to find the "RMB 56",
    # but then continue and geocode the "86 RIVER ROAD"
    # We stop when we find the first delivery service!

    for service in services:
        matched = service[0].search(addressLine[:trimEnd])
        if matched is not None:
            this.isPostalService = True
            this.result['isPostalService'] = True
            this.postalServiceText1 = addressLine[:matched.end()].strip()
            this.postalServiceText2 = addressLine[matched.end():].strip()
            this.postalService3 = None
            this.trim = this.postalServiceText1
            return True

    return False


def removeExtraTrims(this, addressLine, trimEnd):
    '''
Remove Extra trimsss from addressLine
    '''

    if trimEnd == 0:        # Address started with a number
        return

    for trim in extraTrims:
        matched = trim.search(addressLine[:trimEnd])
        if matched is not None:
            if (this.trim is None) or (len(this.trim) < matched.end()):
                this.trim = addressLine[:matched.end()].strip()

    return


def bestSuburb(this):
    '''
Find the best suburb from this.validSuburbs
    '''

    this.suburbInState = set()
    this.suburbInPostcode = set()
    bestSuburbs = set()
    for suburb in this.validSuburbs:
        if (this.validPostcode is not None) and (suburb in postcodes[this.validPostcode]):
            this.logger.debug('bestSuburb - suburb(%s) in postcode(%s)', suburb, this.validPostcode)
            this.suburbInPostcode.add(suburb)
        if (this.validState is not None) and (this.validState in this.validSuburbs[suburb]):
            this.logger.debug('bestSuburb - suburb(%s) in state(%s)', suburb, states[this.validState][0])
            this.suburbInState.add(suburb)
    bestSuburbs = this.suburbInState.intersection(this.suburbInPostcode)
    this.logger.debug('bestSuburb - bestSuburbs(%s)', repr(sorted(bestSuburbs)))
    this.bestSuburb = None
    if len(bestSuburbs) == 0:
        return
    if len(bestSuburbs) == 1:
        this.bestSuburb = list(bestSuburbs)[0]
        return
    # Multiple best suburbs - if we have a house number, suburb, state but no street,
    # then choose the suburb with the smallest number of houses - a small community where each house is numbered
    if (this.houseNo is not None) and (this.streetName is None) and (this.validState is not None):
        minHouses = None
        pickedSuburb = None
        for suburb in bestSuburbs:
            if (suburb in suburbCount) and (this.validState in suburbCount[suburb]):
                if (minHouses is None) or (suburbCount[suburb][this.validState] < minHouses):
                    pickedSuburb = suburb
                    minHouses = suburbCount[suburb][this.validState]
        if pickedSuburb is not None:
            this.bestSuburb = pickedSuburb
            return
    # Otherwise pick the suburb that's the best fit for one of the free text 'foundSUburbText'
    nearestDist = None
    for suburb in bestSuburbs:
        for foundSuburb, isAPI in this.foundSuburbText:
            dist = jellyfish.levenshtein_distance(suburb, foundSuburb)
            if (nearestDist is None) or (dist < nearestDist):
                nearestDist = dist
                this.bestSuburb = suburb
    return


def scoreSuburb(this, thisSuburb, statePid):
    '''
Score this suburb
    '''

    this.logger.debug('scoreSuburb - %s, %s', thisSuburb, statePid)
    this.result['suburb'] = thisSuburb
    this.result['score'] &= ~240
    thisScore = 0
    if len(this.foundSuburbText) > 0:
        thisScore = 16
        # See if one of the found suburbs is the geocoded suburb
        for foundSuburb, isAPI in this.foundSuburbText:
            this.logger.debug('scoreSuburb - checking foundSuburbText(%s)', foundSuburb)
            if isAPI:        # API data
                if foundSuburb == thisSuburb:
                    this.logger.debug('scoreSuburb - is API match')
                    thisScore = 240
            elif foundSuburb == thisSuburb:                # Address line data
                if thisScore < 224:
                    this.logger.debug('scoreSuburb - is address line data match')
                    thisScore = 224
        if thisScore == 16:            # thisSuburb must be alias, sounds like, looks like, primary name for an alias or neighbouring suburb
            this.logger.debug('scoreSuburb - suburb(%s) not passed as data', thisSuburb)
            if (thisSuburb not in this.validSuburbs) or (statePid not in this.validSuburbs[thisSuburb]):
                # Must be a neighbouring suburb - possibly in a different state - find the passed suburb
                this.logger.debug('scoreSuburb - suburb(%s) must be a neighbouring suburb', thisSuburb)
                if thisSuburb in this.neighbourhoodSuburbs:        # A neighbour of something
                    for neighbour in this.neighbourhoodSuburbs[thisSuburb]:
                        if neighbour in this.validSuburbs:
                            suburb = neighbour
                            this.logger.debug('scoreSuburb - suburb(%s) is neighbour of (%s)', thisSuburb, neighbour)
                            break
                    else:
                        this.logger.warning('scoreSuburb - Serious configuration Error - unknown suburb(%s)', thisSuburb)
                        return
                else:
                    this.logger.warning('scoreSuburb - Serious Configuration Error - unknown suburb(%s)', thisSuburb)
                    return
                if this.validSuburbs[suburb]['SX'][1]:
                    thisScore = 64
                else:
                    thisScore = 48
                this.result['score'] |= thisScore
                return
            else:
                this.logger.debug('scoreSuburb - checking validSuburb(%s)', thisSuburb)
                isAPI = this.validSuburbs[thisSuburb]['SX'][1]
                if len({'G', 'A', 'GA'}.intersection(this.validSuburbs[thisSuburb][statePid])) > 0:
                    # Can't be an exact match or it would have been picked up above.
                    # Could be an alias, or a preferred name for an alias
                    this.logger.debug('scoreSuburb - G-NAF alias suburb')
                    if isAPI:
                        if thisScore < 208:
                            thisScore = 208
                    elif thisScore < 192:
                        thisScore = 192
                elif len({'GS', 'AS', 'GAS'}.intersection(this.validSuburbs[thisSuburb][statePid])) > 0:
                    this.logger.debug('scoreSuburb - G-NAF/AusPost sounds like suburb')
                    if isAPI:
                        if thisScore < 160:
                            thisScore = 160
                    elif thisScore < 144:
                        thisScore = 144
                elif len({'GL', 'AL', 'GAL'}.intersection(this.validSuburbs[thisSuburb][statePid])) > 0:
                    this.logger.debug('scoreSuburb - G-NAF/AusPost looks like suburb')
                    if isAPI:
                        if thisScore < 112:
                            thisScore = 112
                    elif thisScore < 96:
                        thisScore = 96
        this.result['score'] |= thisScore
        return
    return


def scoreBuilding(this, thisState, thisPostcode):
    '''
If we are looking for building in a community (we have a house number and suburbs, but no streets within those suburbs)
then look for a building, with a name that matches one of those suburbs, and a house number that matches our house number.
If not, then we only have state and postcode, so look for a building that is within the state and/or postcode.
    '''

    this.logger.debug('scoreBuilding - houseNo (%s), buildings(%s), thisState(%s), thisPostcode(%s)', this.houseNo, this.foundBuildings, thisState, thisPostcode)

    if len(this.foundBuildings) == 0:
        this.logger.debug('scoreBuilding - no buildings')
        if not this.result['isPostalService']:
            this.result['buildingName'] = ''
        return False
    thisFoundBuilding = {}
    for thisBuildingInfo in this.foundBuildings:
        # this.logger.debug('scoreBuilding - checking building (%s)', thisBuildingInfo)
        buildingName = thisBuildingInfo[0]
        houseNo = thisBuildingInfo[1]
        streetPid = thisBuildingInfo[2]
        localityPid = thisBuildingInfo[3]
        if streetPid not in streetNos:
            # this.logger.debug('scoreBuilding - invalid streetPid')
            continue
        if houseNo not in streetNos[streetPid]:
            # this.logger.debug('scoreBuilding - houseNo not in a street')
            continue
        if localityPid not in localities:
            # this.logger.debug('scoreBuilding - invalid localityPid - not in a locality')
            continue
        if localityPid not in localityPostcodes:
            # this.logger.debug('scoreBuilding - invalid localityPid - not in localityPostcodes')
            continue
        if thisState is None:           # Looking for a community house
            if houseNo != this.houseNo:
                # this.logger.debug('scoreBuilding - different houseNo')
                continue
            if buildingName not in localityNames:
                # this.logger.debug('scoreBuilding - buildingName not in localityNames')
                continue
            # Check that this building is in one of the valid suburbs in this state
            found = False
            for suburb in sorted(this.validSuburbs):
                if this.validState not in this.validSuburbs[suburb]:
                    continue
                for src in this.validSuburbs[suburb][this.validState]:
                    if (src in ['G', 'GA', 'GS', 'GL', 'GN']) and (localityPid in this.validSuburbs[suburb][this.validState][src]):
                        found = True
                        break
                if found:
                    break
            if found:
                if buildingName not in thisFoundBuilding:
                    thisFoundBuilding[buildingName] = []
                thisFoundBuilding[buildingName].append(thisBuildingInfo)
        else:       # Looking for a unique building in this state and postcode
            if (localityPid not in localityPostcodes) or (thisPostcode not in localityPostcodes[localityPid]):
                continue
            if buildingName not in thisFoundBuilding:
                thisFoundBuilding[buildingName] = []
            thisFoundBuilding[buildingName].append(thisBuildingInfo)

    if len(thisFoundBuilding) == 0:
        this.logger.debug('scoreBuilding - no matching buildings')
        if not this.result['isPostalService']:
            this.result['buildingName'] = ''
        return False
    # We could have multiple building names, and multiple instances of buildings with that name
    buildingName = None
    bulidingLocalityPid = None
    for name in thisFoundBuilding:
        for buildingInfo in thisFoundBuilding[name]:
            if buildingName is None:
                buildingName = name
                buildingLocalityPid = buildingInfo[3]
            elif (thisState is None) and (name == buildingName) and (buildingInfo[3] == buildingLocalityPid):    # Multiple buildings - same name, same locality - first will do
                continue
            else:                                           # Different name or different locality
                this.logger.debug('scoreBuilding - too many matching buildings (%s)', thisFoundBuilding)
                if not this.result['isPostalService']:
                    this.result['buildingName'] = ''
                return False
    if buildingName is None:
        this.logger.debug('scoreBuilding - too many matching buildings (%s)', thisFoundBuilding)
        if not this.result['isPostalService']:
            this.result['buildingName'] = ''
        return False

    # Return this building
    buildingInfo = thisFoundBuilding[buildingName][0]
    houseNo = buildingInfo[1]
    streetPid = buildingInfo[2]
    localityPid = buildingInfo[3]
    matchingSuburb = None
    matchingState = None
    for thisStatePid, thisLocalityName, thisAlias in localities[localityPid]:
        if thisStatePid != this.validState:
            continue
        matchingState = thisStatePid
        matchingSuburb = thisLocalityName
    if matchingState is None:           # No matching building in this state
        this.logger.debug('scoreBuilding - no matching buildings in this state (%s)', this.validState)
        if not this.result['isPostalService']:
            this.result['buildingName'] = ''
        return False

    this.houseNo = houseNo
    this.result['score'] |= 2048
    this.houseTrim = str(houseNo)
    this.result['houseNo'] = str(houseNo)
    streetName = None
    for streetInfo in streetNames[streetPid]:
        if (streetName is None) or (streetInfo[3] == 'P'):
            streetName = streetInfo[0]
            streetType = streetInfo[1]
            streetSuffix = streetInfo[2]
    this.street = streetName
    this.abbrevStreet = streetName
    if streetType != '':
        this.street += ' ' + streetType
        this.abbrevStreet += ' ' + streetTypes[streetType][0]
    if streetSuffix != '':
        this.street += ' ' + streetSuffix
        this.abbrevStreet += ' ' + streetSuffix
    this.result['street'] = this.street
    this.suburb = matchingSuburb
    this.result['suburb'] = this.suburb
    this.result['postcode'] = list(localityPostcodes[localityPid])[0]
    thisState = states[matchingState][0]
    this.result['state'] = thisState
    this.logger.debug('scoreBuilding - best building: buildingName (%s), houseNo (%s), street (%s), suburb (%s)', buildingName, houseNo, streetName, matchingSuburb)
    meshBlock = streetNos[streetPid][houseNo][0]
    sa1 = SA1map[meshBlock]
    lga = LGAmap[meshBlock]
    latitude = streetNos[streetPid][houseNo][1]
    longitude = streetNos[streetPid][houseNo][2]
    gnafid = streetNos[streetPid][houseNo][4]
    this.result['buildingName'] = buildingName
    this.result['Mesh Block'] = meshBlock
    this.result['SA1'] = sa1
    this.result['LGA'] = lga
    this.result['latitude'] = latitude
    this.result['longitude'] = longitude
    this.result['G-NAF ID'] = gnafid
    this.result['status'] = 'Address found'
    this.result['accuracy'] = '4'
    this.result['score'] |= 8192
    setupAddress1Address2(this, buildingName)
    return True


def accuracy2(this, thisSuburb, statePid):
    '''
Set up accuracy 2 return values
    '''

    soundCode = jellyfish.soundex(thisSuburb)
    for src in ['G', 'GA', 'A']:            # Select best suburb
        if statePid in suburbs[soundCode][thisSuburb]:
            if src in suburbs[soundCode][thisSuburb][statePid]:
                key = list(suburbs[soundCode][thisSuburb][statePid][src])[0]
                this.result['SA1'] = suburbs[soundCode][thisSuburb][statePid][src][key][0]
                this.result['LGA'] = suburbs[soundCode][thisSuburb][statePid][src][key][1]
                this.result['latitude'] = suburbs[soundCode][thisSuburb][statePid][src][key][2]
                this.result['longitude'] = suburbs[soundCode][thisSuburb][statePid][src][key][3]
                if src ==  'A':            # Australia Post suburbs
                    this.result['G-NAF ID'] = str(thisSuburb) + '~' + str(key)
                    this.result['postcode'] = key
                    this.result['score'] &= ~12
                    if this.validPostcode is not None:
                        if this.validPostcode == key:
                            if this.isAPIpostcode:
                                this.result['score'] |= 12
                            else:
                                this.result['score'] |= 8
                        else:
                            this.result['score'] |= 4
                else:
                    this.result['G-NAF ID'] = 'L-' + str(key)
                    if this.validPostcode is not None:
                        this.result['score'] &= ~12
                        if key in localityPostcodes:
                            if this.validPostcode in localityPostcodes[key]:
                                if this.isAPIpostcode:
                                    this.result['score'] |= 12
                                else:
                                    this.result['score'] |= 8
                            else:
                                this.result['score'] |= 4
                                this.result['postcode'] = list(localityPostcodes[key])[0]
                        else:
                            this.result['postcode'] = ''
                this.result['status'] = 'Suburb found'
                this.result['accuracy'] = '2'
                # Score thisSuburb
                scoreSuburb(this, thisSuburb, statePid)
                if this.street is not None:
                    this.result['score'] |= 256
                if this.houseNo is not None:
                    this.result['score'] |= 2048
                return True
    if this.street is not None:
        this.result['score'] |= 256
    if this.houseNo is not None:
        this.result['score'] |= 2048
    return False


def Rules1and2(this):
    '''
Business Rules 1 and 2
    '''

    # Business Rule 1
    if region:
        if this.validState is None:
            if len(this.validSuburbs) > 0:
                NTsuburb = list(this.validSuburbs)[0]
                soundCode = jellyfish.soundex(NTsuburb)
                if len(suburbs[soundCode][NTsuburb]) == 1:
                    this.validState = list(suburbs[soundCode][NTsuburb])[0]
                    this.logger.info('Trying state (%s)', this.validState)
    if (this.validState is None) and (this.validPostcode is None):
        this.logger.debug('Rules1and2 - no valid state or postcode')
        this.result['messages'].append('no valid state or postcode')
        return False
    if this.validState is None:
        this.logger.debug('Rules1and2 - no valid state')
    if this.validPostcode is None:
        this.logger.debug('Rules1and2 - no valid postcode')

    this.bestSuburb = None

    if len(this.validSuburbs) > 0:
        this.logger.debug('Rules1and2 - have valid suburb(s)')
        bestSuburb(this)        # Compute the best suburbs
        # Has a chance of passing V1, V2 or V3
        if this.validPostcode is not None:
            # Passed "Have postcode"
            this.logger.debug('Rules1and2 - have valid postcode(%s)', this.validPostcode)
            if this.validState is not None:
                # Passed "Have state"
                this.logger.debug('Rules1and2 - have valid state(%s)', states[this.validState][0])
                if this.validState in postcodes[this.validPostcode]['states']:
                    # Passed "postcode/state comb'n defined"
                    this.logger.debug('Rules1and2 - postcode(%s) is in state(%s)', this.validPostcode, states[this.validState][0])
                    if (len(this.suburbInPostcode) > 0) and (len(this.suburbInState) > 0):
                        # Passed V1 - but we need to check for multiple suburbs
                        # Caller could have provided one in 'suburb' and another as extraText after the streetName
                        # One could be in the state, and the other in the suburb
                        this.logger.debug('Rules1and2 - passed V1')
                        if this.bestSuburb is not None:        # Use the best suburb
                            thisSuburb = this.bestSuburb
                            this.logger.debug('Rules1and2 - best suburb (in both state and postcode) is (%s)', thisSuburb)
                        else:        # Oops - no common suburb - pick first one that is in the postcode
                            thisSuburb = list(sorted(this.suburbInPostcode))[0]
                            this.logger.debug('Rules1and2 - best suburb (in just postcode) is (%s)', thisSuburb)
                        if not accuracy2(this, thisSuburb, this.validState):
                            this.logger.debug('Rules1and2 - no geocoding for this suburb')
                        return True
                    else:
                        this.logger.debug('Rules1and2 - but suburb(s) not in postcode(%s), or not in state(%s)', this.validPostcode, this.validState)
            if len(this.suburbInPostcode) > 0:
                # Passed V2 - bad state
                # Geocode the suburb, so long as it doesn't cross a state boundary
                this.logger.debug('Rules1and2 - passed V2 - suburb in postcode (bad state)')
                if len(postcodes[this.validPostcode]['states']) == 1:       # Postcode exists in only one state
                    statePid = list(postcodes[this.validPostcode]['states'])[0]
                    this.logger.debug('Rules1and2 - and postcode(%s) occurs only in one state(%s)', this.validPostcode, states[statePid][0])
                    this.result['state'] = states[statePid][0]
                    this.result['score'] &= ~3
                    if this.validState is not None:
                        if this.validState == statePid:
                            if this.isAPIstate:
                                this.result['score'] |= 3
                            else:
                                this.result['score'] |= 2
                        else:
                            this.result['score'] |= 1
                    if this.bestSuburb is not None:        # Use the best suburb
                        thisSuburb = this.bestSuburb
                    else:
                        thisSuburb = list(sorted(this.suburbInPostcode))[0]
                    this.logger.debug('Rules1and2 - searching geocoding for this suburb(%s) in state(%s)', thisSuburb, states[statePid][0])
                    if not accuracy2(this, thisSuburb, statePid):
                        this.logger.debug('Rules1and2 - no geocoding for this suburb in this postcode')
                        this.result['messages'].append('no geocode data for suburb in postcode')
                        return False
                    return True
                # Postcode exits in multiple states, but the suburb is within the postcode
                # If the suburb exist within only one state then that's our state
                statePid = None
                for suburb in this.suburbInPostcode:
                    soundCode = jellyfish.soundex(suburb)
                    if (soundCode not in suburbs) or (suburb not in suburbs[soundCode]):
                        break
                    if (len(suburbs[soundCode][suburb]) > 1) or (statePid is not None):
                        statePid = None
                        break
                    statePid = list(suburbs[soundCode][suburb])[0]
                if statePid is None:
                    this.logger.debug('Rules1and2 - postcode(%s) is in multiple states', this.validPostcode)
                    this.result['messages'].append('postcode in multiple states')
                    return False
                this.logger.debug('Rules1and2 - and postcode(%s)/suburb(%s) occurs only in one state(%s)', this.validPostcode, repr(sorted(this.suburbInPostcode)), statePid)
                this.result['state'] = statePid
                this.result['score'] &= ~3
                if this.bestSuburb is not None:        # Use the best suburb
                    thisSuburb = this.bestSuburb
                else:
                    thisSuburb = list(sorted(this.suburbInPostcode))[0]
                this.logger.debug('Rules1and2 - searching geocoding for this suburb(%s) in state(%s)', thisSuburb, states[statePid][0])
                if not accuracy2(this, thisSuburb, statePid):
                    this.logger.debug('Rules1and2 - no geocoding for this suburb in this postcode')
                    this.result['messages'].append('no geocode data for suburb in postcode')
                    return False
                return True
        if (this.validState is not None) and (len(this.suburbInState) > 0):
            # Passed V3 - bad postcode
            this.logger.debug('Rules1and2 - passed V3 - suburb in state (bad postcode)')
            this.result['postcode'] = ''
            this.result['score'] &= ~12
            thisSuburb = list(sorted(this.suburbInState))[0]        # Pick the first suburb found in this state
            for suburb in this.suburbInState:        # Then look for a better one
                soundCode = jellyfish.soundex(suburb)
                if (soundCode not in suburbs) or (suburb not in suburbs[soundCode]):
                    continue
                if this.validState in suburbs[soundCode][suburb]:
                    if 'A' in suburbs[soundCode][suburb][this.validState]:
                        if len(suburbs[soundCode][suburb][this.validState]['A']) == 1:      # Only one postcode for this suburb in this state
                            thisPostcode = list(suburbs[soundCode][suburb][this.validState]['A'])[0]
                            this.result['postcode'] = thisPostcode
                            if this.validPostcode is not None:
                                if this.validPostcode == thisPostcode:
                                    if this.isAPIpostcode:
                                        this.result['score'] |= 12
                                    else:
                                        this.result['score'] |= 8
                                else:
                                    this.result['score'] |= 4
                            thisSuburb = suburb
                            break
            if this.result['postcode'] == '':
                this.validPostcode = None
            this.logger.debug('Rules1and2 - best suburb is (%s)', thisSuburb)
            if not accuracy2(this, thisSuburb, this.validState):
                this.logger.debug('Rules1and2 - no geocoding for this suburb in this state')
                this.result['messages'].append('no geocode data for suburb in state')
                return False
            return True

    # Failed V1, V2 and V3 - try V4/V5
    if (this.validState is None) or (this.validPostcode is None):
        # Failed V4/V5 on "Have postcode" and "Have state"
        this.logger.debug('Rules1and2 - no valid state or no valid postcode')
        this.result['messages'].append('bad suburb and no valid state or no valid postcode')
        return False
    # We have a postcode and a state. Is the postcode in the state?
    if this.validState not in postcodes[this.validPostcode]:
        # Failed V4/V5 on "postcode/state comb'n defined"
        this.logger.debug('Rules1and2 - valid postcode not in valid state')
        this.result['messages'].append('bad suburb and valid postcode not in valid state')
        return False
    if len(postcodes[this.validPostcode][this.validState]) == 1:        # All the suburbs, in this postcode, are in this state
        # There is only one suburb with this postcode, in this state
        # Passed V4 - bad suburb
        thisSuburb = list(postcodes[this.validPostcode][this.validState])[0]
        this.logger.debug('Rules1and2 - passed V4 - only suburb in postcode (%s), in state(%s) is (%s)', this.validPostcode, this.validState, thisSuburb)
        if accuracy2(this, thisSuburb, this.validState):
            this.logger.debug('Rules1and2 - postcode is (%s), suburb is (%s)', this.validPostcode, thisSuburb)
            return True
        else:
            this.logger.debug('Rules1and2 - no geocoding for this suburb')
            this.result['messages'].append('no geocode data for only suburb in postcode')
            return False
    else:
        # Passed V5 - bad suburb
        this.logger.debug('Rules1and2 - passed V5')
        this.result['suburb'] = ''
        this.result['score'] &= ~240
        this.result['SA1'] = postcodes[this.validPostcode][''][0]
        this.result['LGA'] = postcodes[this.validPostcode][''][1]
        this.result['latitude'] = postcodes[this.validPostcode][''][2]
        this.result['longitude'] = postcodes[this.validPostcode][''][3]
        this.result['G-NAF ID'] = this.validPostcode
        this.result['status'] = 'Postcode found'
        this.result['accuracy'] = '1'
        if this.street is not None:
            this.result['score'] |= 256
        if this.houseNo is not None:
            this.result['score'] |= 2048
        return True


def addSources(this, streetKey, srcs):
    '''
Add sources to this.validStreet for streets in this.validState and this.validPostcode
    '''

    this.logger.debug('addSources - adding sources(%s) for street(%s)', repr(list(srcs)), streetKey)

    for src in srcs:
        if src in ['SK', 'regex']:
            continue
        places = copy.deepcopy(srcs[src])
        if this.validState is not None:
            # Check if every street in these places is in this state
            for streetPid in list(places):            # Check each street (that has this soundCode, streetKey, source)
                if streetPid not in stateStreets[this.validState]:        # This street is not in this state - park this streetPid
                    if this.fuzzLevel not in this.parkedWrongState:
                        this.parkedWrongState[this.fuzzLevel] = {}
                    if streetKey not in this.parkedWrongState[this.fuzzLevel]:
                        this.parkedWrongState[this.fuzzLevel][streetKey] = {}
                    if src not in this.parkedWrongState[this.fuzzLevel][streetKey]:
                        this.parkedWrongState[this.fuzzLevel][streetKey][src] = {}
                    '''
                    this.logger.debug('addSources - parking (wrong state) street(%s), source(%s), streetPid(%s), place(%s)',
                                      streetKey, src, streetPid, repr(places[streetPid]))
                    '''
                    this.parkedWrongState[this.fuzzLevel][streetKey][src][streetPid] = places[streetPid]
                    del places[streetPid]        # And remove it from places
        if this.validPostcode is not None:
            # Check if every street in these places is in this postcode
            # To do that we need to find the locality containing this streetPid
            # and then check all the postcodes associates with that locality
            for streetPid in list(places):            # Check each street (that has this soundCode, streetKey, source)
                foundPostcode = False
                for streetData in streetNames[streetPid]:        # Check every locality that has an instance of this street
                    localityPid = streetData[3]            # Check if the valid postcode is associatied with this locality
                    if (localityPid in localityPostcodes) and (this.validPostcode in localityPostcodes[localityPid]):
                        foundPostcode = True            # This street, in this locality, is in this postcode
                        break
                if not foundPostcode:        # There is no locality, associated with this street, in this postcode - park this streetPid
                    if this.fuzzLevel not in this.parkedWrongPostcode:
                        this.parkedWrongPostcode[this.fuzzLevel] = {}
                    if streetKey not in this.parkedWrongPostcode[this.fuzzLevel]:
                        this.parkedWrongPostcode[this.fuzzLevel][streetKey] = {}
                    if src not in this.parkedWrongPostcode[this.fuzzLevel][streetKey]:
                        this.parkedWrongPostcode[this.fuzzLevel][streetKey][src] = {}
                    '''
                    this.logger.debug('addSources - parking (wrong postcode) street(%s), source(%s), streetPid(%s), place(%s)',
                                     streetKey, src, streetPid, repr(places[streetPid]))
                    '''
                    this.parkedWrongPostcode[this.fuzzLevel][streetKey][src][streetPid] = places[streetPid]
                    del places[streetPid]        # And remove it from places
        # Check if any places left
        if len(places) > 0:
            if streetKey not in this.validStreets:
                this.validStreets[streetKey] = {}
            if src not in this.validStreets[streetKey]:
                this.validStreets[streetKey][src] = {}
            this.validStreets[streetKey][src].update(places)
    return


def createValidStreets(this):
    '''
Create the set of validStreets using streetName, streetType and streetSuffix
    '''

    if this.streetName is None:
        this.logger.debug('createValidStreets - no street')
        return
    this.logger.debug('createValidStreets - for street(%s)', this.street)
    this.logger.debug('createValidStreets - on entry, validStreets(%s)', repr(this.validStreets))
    soundCode = jellyfish.soundex(this.streetName)
    if soundCode not in streets:
        this.logger.debug('createValidStreets - no street named %s, soundCode %s', this.streetName, soundCode)
        return
    streetType = this.streetType
    streetSuffix = this.streetSuffix
    if this.streetType is None:
        streetType = ''
        if this.streetSuffix is None:
            streetSuffix = ''
            streetKey = this.streetName + '~~'
            shortKey = this.streetName
        else:
            streetKey = '~'.join([this.streetName, '', this.streetSuffix])
            shortKey = ' '.join([this.streetName, this.streetSuffix]).strip()
        if shortKey not in shortStreets:
            if this.streetSuffix is None:
                this.logger.debug('createValidStreets - no short street named %s, soundCode %s, streetKey %s', this.streetName, soundCode, streetKey)
            else:
                this.logger.debug('createValidStreets - no short street named %s %s, soundCode %s, streetKey %s', this.streetName, this.streetSuffix, soundCode, streetKey)
            return
        srcs = shortStreets[shortKey]
    elif this.streetSuffix is None:
        streetSuffix = ''
        streetKey = '~'.join([this.streetName, this.streetType, ''])
        if streetKey not in streets[soundCode]:
            this.logger.debug('createValidStreets - no street named %s %s, soundCode %s, streetKey %s', this.streetName, this.streetType, soundCode, streetKey)
            return
        srcs = streets[soundCode][streetKey]
    else:
        streetKey = '~'.join([this.streetName, this.streetType, this.streetSuffix])
        if streetKey not in streets[soundCode]:
            this.logger.debug('createValidStreets - no street named %s %s %s, soundCode %s, streetKey %s', this.streetName, this.streetType, this.streetSuffix, soundCode, streetKey)
            return
        srcs = streets[soundCode][streetKey]
    this.logger.debug('createValidStreets - streetKey(%s)', streetKey)
    this.logger.debug('createValidStreets - sources(%s)', list(srcs))
    if streetKey not in this.validStreets:
        this.validStreets[streetKey] = {}
        this.validStreets[streetKey]['SX'] = [soundCode, this.streetName, streetType, streetSuffix]
    addSources(this, streetKey, srcs)
    this.logger.debug('createValidStreets - on exit, validStreets(%s)', repr(this.validStreets))
    return


def validateStreets(this):
    '''
Check to see if any of the valid streets are in any of the valid suburbs
    '''

    # Initially, there are no valid streets in the valid suburbs
    this.subsetValidStreets = set()
    this.neighbourhoodSuburbs = {}

    # Check that we have some valid streets
    if len(this.validStreets) == 0:
        this.logger.debug('validateStreets - no valid streets in this.validStreets')
        return False
    this.logger.debug('validateStreets - have (%d) valid streets - %s', len(this.validStreets), repr(sorted(this.validStreets)))

    # Check if we have any suitable suburbs
    if len(this.validSuburbs) == 0:
        this.logger.debug('validateStreets - no valid suburbs in this.validSuburbs')
        return False
    this.logger.debug('validateStreets - have (%d) valid suburbs - %s', len(this.validSuburbs), repr(sorted(this.validSuburbs)))

    haveSuburbs = False
    for suburb in sorted(this.validSuburbs):
        for statePid in this.validSuburbs[suburb]:
            if statePid == 'SX':
                continue
            for src in this.validSuburbs[suburb][statePid]:
                if src in ['G', 'GA', 'GS', 'GL', 'GN']:
                    haveSuburbs = True
                    break
            if haveSuburbs:
                break
        if haveSuburbs:
            break
    if not haveSuburbs:
        this.logger.debug('validateStreets - no G-NAF suburbs')
        return False
    this.logger.debug('validateStreets - have suburbs(%s)', repr(list(sorted(this.validSuburbs))))

    # Create the set of all valid streets (streetPids) - all the streetPids from all the sources across all states and postcodes
    allStreets = set()                  # The set of all valid streets (street pids)
    this.allStreetSources = {}          # The 'street src' ~ 'suburb src' for each street pid
    for streetKey in this.validStreets:
        for src in ['G', 'GA', 'GS', 'GAS', 'GL', 'GAL', 'GN']:
            if src in this.validStreets[streetKey]:
                theseStreets = set(this.validStreets[streetKey][src])        # A set of streetPids
                for streetPid in theseStreets:
                    if streetPid not in this.allStreetSources:
                        this.allStreetSources[streetPid] = src      # the best street source for this street
                allStreets = allStreets.union(theseStreets)
    this.logger.debug('validateStreets - have streets(%s)', repr(sorted(allStreets)))

    # Assemble all the streets in the suburbs valid suburb - regardless of state/territory
    suburbStreets = set()           # The set of all valid streets in suburbs (street pids)
    for suburb in sorted(this.validSuburbs):        # All the valid suburbs
        this.logger.debug('validateStreets - checking suburb(%s)', suburb)
        for statePid in this.validSuburbs[suburb]:        # In every state
            if statePid == 'SX':
                continue
            # this.logger.debug('validateStreets - checking statePid(%s) for suburb(%s)', statePid, suburb)
            for src in ['G', 'GA', 'GS', 'GAS', 'GL', 'GAL', 'GN']:            # That is a G-NAF source
                # this.logger.debug('validateStreets - checking source(%s)', src)
                if src in this.validSuburbs[suburb][statePid]:        # From every source
                    # this.logger.debug('validateStreets - checking source(%s) for suburb(%s)', src, suburb)
                    for localityPid in this.validSuburbs[suburb][statePid][src]:
                        # this.logger.debug('validateStreets - checking locality(%s) for source(%s) for suburb(%s)', localityPid, src, suburb)
                        if localityPid in localityStreets:            # Does this locality have any streets
                            this.logger.debug('validateStreets - suburb (%s) [locality (%s)], in state (%s) has streets(%s)',
                                              suburb, list(localities[localityPid])[0][1], states[statePid][0], repr(sorted(localityStreets[localityPid])))
                            # Select streets from this set that match streets colleted above (the named streets in this named suburb)
                            theseStreets = allStreets.intersection(localityStreets[localityPid])
                            suburbStreets = suburbStreets.union(theseStreets)
                            for streetPid in theseStreets:
                                if streetPid not in this.allStreetSources:
                                    this.allStreetSources[streetPid] = ''                # Adding a street that is not in validStreets
                                this.allStreetSources[streetPid] += '~' + src
                            if (src == 'GN') and (localityPid in localities):
                                for thisStatePid, thisSuburb, thisAlias in localities[localityPid]:
                                    neighbour = thisSuburb
                                    if neighbour not in this.neighbourhoodSuburbs:
                                        this.neighbourhoodSuburbs[neighbour] = set()
                                    this.neighbourhoodSuburbs[neighbour].add(suburb)
                                    if suburb not in this.neighbourhoodSuburbs:
                                        this.neighbourhoodSuburbs[suburb] = set()
                                    this.neighbourhoodSuburbs[suburb].add(neighbour)
    this.subsetValidStreets = suburbStreets

    # If there's no intersection (no street in the valid suburbs) then we have failed
    if len(this.subsetValidStreets) == 0:
        this.logger.debug('validateStreets - no streets in suburbs')
        return False
    return True


def checkHouseNo(this):
    '''
Check if this.houseNo is in any of the streets in this.subsetValidStreets
    '''

    # Check each street
    this.logger.info('checkHouseNo - streetSourceWeight(%s)', repr(streetSourceWeight))
    this.logger.info('checkHouseNo - suburbSourceWeight(%s)', repr(suburbSourceWeight))
    foundStreetPid = None
    foundHouseNo = None
    foundWeight = None
    for streetPid in sorted(this.subsetValidStreets):
        if streetPid not in streetNos:        # an alias street
            continue
        this.logger.debug('checkHouseNo - check street(%s) for house(%d)', streetPid, this.houseNo)
        this.logger.debug('checkHouseNo - (%s)', repr(sorted(streetNos[streetPid])))
        if this.houseNo in streetNos[streetPid]:
            if this.isLot:        # We are looking for LOT numbers
                if not streetNos[streetPid][this.houseNo][3]:
                    continue
            if streetPid not in this.allStreetSources:
                this.logger.critical('checkHouseNo - street(%s) not in allStreetSources', streetPid)
                continue
            srcs = this.allStreetSources[streetPid].split('~')
            this.logger.info('checkHouseNo - house number found - Sources(%s)', repr(srcs))
            if srcs[1] not in suburbSourceWeight:
                continue
            if srcs[0] not in streetSourceWeight:
                continue
            weight = 4 * suburbSourceWeight[srcs[1]] + 6 * streetSourceWeight[srcs[0]]
            if (foundWeight is None) or (foundWeight < weight):
                foundWeight = weight
                foundStreetPid = streetPid
                foundHouseNo = this.houseNo
    exactWeight = foundWeight

    # look for a better nearby match
    houseNum = this.houseNo
    minHouse = houseNum - 6
    maxHouse = houseNum + 6
    houseStep = 2
    foundWeight = None
    for streetPid in sorted(this.subsetValidStreets):
        if streetPid not in streetNos:        # an alias street
            continue
        streetType = streetNames[streetPid][0][1]
        if streetType in ['CLOSE', 'COURT', 'PLACE', 'CUL-DE-SAC']:
            minHouse = houseNum - 3
            maxHouse = houseNum + 3
            houseStep = 1
        # this.logger.debug('checkHouseNo - checking street(%s) from (%d) to (%d) in steps of (%d)', streetPid, minHouse, maxHouse, houseStep)
        # this.logger.debug('checkHouseNo - (%s)', repr(sorted(streetNos[streetPid])))
        for thisHouse in range(minHouse, maxHouse, houseStep):
            if thisHouse in streetNos[streetPid]:
                if this.isLot:        # We are looking for LOT numbers
                    if not streetNos[streetPid][thisHouse][3]:
                        continue
                if streetPid not in this.allStreetSources:
                    this.logger.critical('checkHouseNo - street(%s) not in allStreetSources', streetPid)
                    continue
                srcs = this.allStreetSources[streetPid].split('~')
                if srcs[1] not in suburbSourceWeight:
                    continue
                if srcs[0] not in streetSourceWeight:
                    continue
                weight = 5 * suburbSourceWeight[srcs[1]] + 10 * streetSourceWeight[srcs[0]]
                if (foundWeight is None) or (foundWeight < weight):        # We found a better nearby number
                    foundWeight = weight
                    if exactWeight is not None:                            # Check if it is significantly better than the exact match
                        if weight > exactWeight * 2:
                            foundStreetPid = streetPid
                            foundHouseNo = thisHouse
                    else:
                        foundStreetPid = streetPid
                        foundHouseNo = thisHouse

    if foundStreetPid is not None:
        this.houseNo = foundHouseNo
        this.result['houseNo'] = str(this.houseNo)
        returnHouse(this, foundStreetPid, False)
        return True

    this.logger.debug('checkHouseNo - house not found')
    return False


def expandSuburbsAndStreets(this):
    '''
Add more suburbs to validSuburb or more streets to validStreets
based upon this.fuzzLevel
    '''

    this.logger.debug('expandSuburbsAndStreets - fuzzLevel(%d)', this.fuzzLevel)

    if this.fuzzLevel == 2:
        # Add soundex sounds like streets to this.validStreets for streets already in this.validStreets
        this.logger.info('expandSuburbAndStreets - adding soundex like streets (same postcode and state)')
        for streetKey in list(this.validStreets):
            parts = streetKey.split('~')
            soundCode = this.validStreets[streetKey]['SX'][0]
            for otherKey in streets[soundCode]:        # All the similar sounding streets
                if otherKey == streetKey:        # This street sound like itself, but don't add it as it is already there
                    continue
                otherParts = otherKey.split('~')
                # Same sounding street name, but it has to have the same street type and street suffix
                if this.streetType is None:
                    if otherParts[1] != '':
                        continue
                elif this.streetType != otherParts[1]:
                    continue
                if this.streetSuffix is None:
                    if otherParts[2] != '':
                        continue
                elif this.streetSuffix != otherParts[2]:
                    continue
                # Only add something if it is not too different to this street
                streetLength = len(parts[0])
                maxDist = int((streetLength + 6) / 4)
                dist = jellyfish.levenshtein_distance(parts[0], otherParts[0])
                if dist >= maxDist:
                    continue
                this.logger.debug('expandSuburbsAndStreets - adding street(%s), distance(%d) from (%s)', otherParts[0], dist, parts[0])
                if otherKey not in this.validStreets:
                    this.validStreets[otherKey] = {}
                    this.validStreets[otherKey]['SX'] = [soundCode, otherParts[0], otherParts[1], otherParts[2]]
                newSources = {}
                for src in streets[soundCode][otherKey]:
                    if src not in ['G', 'GA']:            # Only use a G-NAF primary source
                        continue
                    newSources[src + 'S'] = streets[soundCode][otherKey][src]
                addSources(this, otherKey, newSources)
        # Add soundex streets to this.validStreets for this.streetName, this.streetType, this.streetSuffix if not already in this.validStreets
        if this.streetName is not None:
            soundCode = jellyfish.soundex(this.streetName)
            if soundCode in streets:            # Does any street sound like this
                for otherKey in streets[soundCode]:
                    otherParts = otherKey.split('~')
                    # Same sounding street name, but it has to have the same street type and street suffix
                    if this.streetType is None:
                        if otherParts[1] != '':
                            continue
                    elif this.streetType != otherParts[1]:
                        continue
                    if this.streetSuffix is None:
                        if otherParts[2] != '':
                            continue
                    elif this.streetSuffix != otherParts[2]:
                        continue
                    # Only add something if it is not too different to this street
                    streetLength = len(this.streetName)
                    maxDist = int((streetLength + 6) / 4)
                    dist = jellyfish.levenshtein_distance(this.streetName, otherParts[0])
                    if dist >= maxDist:
                        continue
                    this.logger.debug('expandSuburbsAndStreets - adding street(%s), distance(%d) from (%s)', otherParts[0], dist, this.streetName)
                    if otherKey not in this.validStreets:
                        this.validStreets[otherKey] = {}
                        this.validStreets[otherKey]['SX'] = [soundCode, otherParts[0], otherParts[1], otherParts[2]]
                    newSources = {}
                    for src in streets[soundCode][otherKey]:
                        if src not in ['G', 'GA']:            # Only use a G-NAF primary source
                            continue
                        newSources[src + 'S'] = streets[soundCode][otherKey][src]
                    addSources(this, otherKey, newSources)
    elif this.fuzzLevel == 3:
        # Add Levenshtein Distance streets to this.validStreets for streets already in this.validStreets
        this.logger.info('expandSuburbAndStreets - adding Levenshtein Distance like streets (same postcode and state)')
        GAset = set(['G', 'GA'])        # Don't expand on sounds like streets
        for streetKey in list(this.validStreets):
            srcs = set(this.validStreets[streetKey])
            if srcs.isdisjoint(GAset):
                continue
            streetName = this.validStreets[streetKey]['SX'][1]
            streetLength = len(streetName)
            maxDist = int((streetLength + 2) / 4)
            minLen = max(0, streetLength - 2)
            maxLen = streetLength + 2
            processed = set()
            for thisLen in range(minLen, maxLen):
                if thisLen in streetLen:
                    for streetInfo in streetLen[thisLen]:
                        if streetInfo[2] == streetKey:
                            continue
                        soundCode = streetInfo[0]
                        otherKey = streetInfo[2]
                        if otherKey in processed:
                            continue
                        dist = jellyfish.levenshtein_distance(streetName, streetInfo[1])
                        if dist <= maxDist:
                            parts = otherKey.split('~')
                            if this.streetType is None:
                                if parts[1] != '':
                                    continue
                            elif this.streetType != parts[1]:
                                continue
                            if this.streetSuffix is None:
                                if parts[2] != '':
                                    continue
                            elif this.streetSuffix != parts[2]:
                                continue
                            if otherKey not in this.validStreets:
                                this.validStreets[otherKey] = {}
                                this.validStreets[otherKey]['SX'] = [soundCode, parts[0], parts[1], parts[2]]
                            newSources = {}
                            for src in streets[soundCode][otherKey]:
                                if src not in ['G', 'GA']:            # Only use a G-NAF primary source
                                    continue
                                newSources[src + 'L'] = streets[soundCode][otherKey][src]
                            addSources(this, otherKey, newSources)
                            processed.add(otherKey)
        # Add Levenshtein Distance streets to this.validStreets for this.streetName, this.streetType, this.streetSuffix if not already in this.validStreets
        if this.streetName is not None:
            soundCode = jellyfish.soundex(this.streetName)
            if this.streetType is None:
                if this.streetSuffix is None:
                    streetKey = this.streetName + '~~'
                else:
                    streetKey = '~'.join([this.streetName, '', this.streetSuffix])
            elif this.streetSuffix is None:
                streetKey = '~'.join([this.streetName, this.streetType, ''])
            else:
                streetKey = '~'.join([this.streetName, this.streetType, this.streetSuffix])
            if streetKey not in list(this.validStreets):
                streetLength = len(this.streetName)
                maxDist = int((streetLength + 2) / 4)
                minLen = max(0, streetLength - 2)
                maxLen = streetLength + 2
                processed = set()
                for thisLen in range(minLen, maxLen):
                    if thisLen in streetLen:
                        for streetInfo in streetLen[thisLen]:
                            soundCode = streetInfo[0]
                            otherKey = streetInfo[2]
                            if otherKey in processed:
                                continue
                            dist = jellyfish.levenshtein_distance(this.streetName, streetInfo[1])
                            if dist <= maxDist:
                                parts = otherKey.split('~')
                                if this.streetType is None:
                                    if parts[1] != '':
                                        continue
                                elif this.streetType != parts[1]:
                                    continue
                                if this.streetSuffix is None:
                                    if parts[2] != '':
                                        continue
                                elif this.streetSuffix != parts[2]:
                                    continue
                                if otherKey not in this.validStreets:
                                    this.validStreets[otherKey] = {}
                                    this.validStreets[otherKey]['SX'] = [soundCode, parts[0], parts[1], parts[2]]
                                newSources = {}
                                for src in streets[soundCode][otherKey]:
                                    if src not in ['G', 'GA']:            # Only use a G-NAF primary source
                                        continue
                                    newSources[src + 'L'] = streets[soundCode][otherKey][src]
                                addSources(this, otherKey, newSources)
                                processed.add(otherKey)

    elif this.fuzzLevel == 4:
        # Add soundex suburbs to this.validSuburbs for suburbs already in this.validSuburbs
        this.logger.info('expandSuburbAndStreets - adding soundex like suburbs (same postcode and state)')
        for suburb in sorted(list(this.validSuburbs)):
            soundCode = this.validSuburbs[suburb]['SX'][0]
            isAPI = this.validSuburbs[suburb]['SX'][1]
            if soundCode in suburbs:                # Does any suburb sound like this
                for otherSuburb in suburbs[soundCode]:
                    if otherSuburb == suburb:
                        continue
                    # Only add something if it is not too different to one of the foundSuburbTexts
                    suburbLength = len(otherSuburb)
                    maxDist = int((suburbLength + 6) / 4)
                    for foundSuburb, isAPI in this.foundSuburbText:
                        dist = jellyfish.levenshtein_distance(foundSuburb, otherSuburb)
                        if dist <= maxDist:
                            this.logger.debug('expandSuburbsAndStreets - adding suburb(%s), distance(%d) from (%s)', otherSuburb, dist, foundSuburb)
                            break
                    else:
                        continue
                    if otherSuburb not in this.validSuburbs:
                        this.validSuburbs[otherSuburb] = {}
                        this.validSuburbs[otherSuburb]['SX'] = [soundCode, isAPI]
                    for statePid in suburbs[soundCode][otherSuburb]:
                        if statePid == 'SX':
                            continue
                        if statePid not in this.validSuburbs[otherSuburb]:
                            this.validSuburbs[otherSuburb][statePid] = {}
                        for src in suburbs[soundCode][otherSuburb][statePid]:
                            if src not in this.validSuburbs[otherSuburb][statePid]:
                                if src not in ['G', 'GA']:            # Only use a G-NAF primary source
                                    continue
                                this.validSuburbs[otherSuburb][statePid][src + 'S'] = suburbs[soundCode][otherSuburb][statePid][src]
        # Add soundex suburbs to this.validSuburbs for all foundSuburbText, if not already in this.validSuburbs
        for suburb, isAPI in sorted(this.foundSuburbText):
            if suburb in this.validSuburbs:
                continue
            soundCode = jellyfish.soundex(suburb)
            if soundCode in suburbs:            # Does any suburb sound like this
                for otherSuburb in suburbs[soundCode]:
                    if otherSuburb == suburb:
                        continue
                    # Only add something if it is not too different to one of the foundSuburbTexts
                    suburbLength = len(otherSuburb)
                    maxDist = int((suburbLength + 6) / 4)
                    for foundSuburb, isAPI in this.foundSuburbText:
                        dist = jellyfish.levenshtein_distance(foundSuburb, otherSuburb)
                        if dist <= maxDist:
                            this.logger.debug('expandSuburbsAndStreets - adding suburb(%s), distance(%d) from (%s)', otherSuburb, dist, foundSuburb)
                            break
                    else:
                        continue
                    if otherSuburb not in this.validSuburbs:
                        this.validSuburbs[otherSuburb] = {}
                        this.validSuburbs[otherSuburb]['SX'] = [soundCode, isAPI]
                    for statePid in suburbs[soundCode][otherSuburb]:
                        if statePid == 'SX':
                            continue
                        if statePid not in this.validSuburbs[otherSuburb]:
                            this.validSuburbs[otherSuburb][statePid] = {}
                        for src in suburbs[soundCode][otherSuburb][statePid]:
                            if src not in ['G', 'GA']:            # Only use a G-NAF primary source
                                continue
                            if src not in this.validSuburbs[otherSuburb][statePid]:
                                this.validSuburbs[otherSuburb][statePid][src + 'S'] = suburbs[soundCode][otherSuburb][statePid][src]
        bestSuburb(this)        # Compute the best suburbs
    elif this.fuzzLevel == 5:
        # Add Levenshtein Distance suburbs to this.validSuburbs for all suburbs already in this.validSuburb
        this.logger.info('expandSuburbAndStreets - adding Levenshtein Distance like suburbs (same postcode and state)')
        for suburb in sorted(list(this.validSuburbs)):
            toCheck = False         # Don't expand on sounds like suburbs
            for statePid in this.validSuburbs[suburb]:
                if statePid == 'SX':
                    continue
                for src in this.validSuburbs[suburb][statePid]:
                    if src in ['G', 'GA']:
                        toCheck = True
                        break
            if not toCheck:
                continue
            suburbLength = len(suburb)
            maxDist = int((suburbLength + 2) / 4)
            minLen = max(0, suburbLength - 2)
            maxLen = min(maxSuburbLen, suburbLength + 2)
            processed = set()
            # this.logger.debug('expandSuburbAndStreets - checking from %d to %d', minLen, maxLen - 1)
            for thisLen in range(minLen, maxLen):
                if thisLen in suburbLen:
                    for soundCode in suburbLen[thisLen]:
                        for otherSuburb in suburbLen[thisLen][soundCode]:
                            # this.logger.debug('expandSuburbAndStreets - checking %s with %s', suburb, otherSuburb)
                            if otherSuburb == suburb:
                                continue
                            if otherSuburb in processed:
                                continue
                            processed.add(otherSuburb)
                            dist = jellyfish.levenshtein_distance(suburb, otherSuburb)
                            if dist <= maxDist:
                                if otherSuburb not in this.validSuburbs:
                                    this.validSuburbs[otherSuburb] = {}
                                    isAPI = this.validSuburbs[suburb]['SX'][1]
                                    this.validSuburbs[otherSuburb]['SX'] = [soundCode, isAPI]
                                for statePid in suburbs[soundCode][otherSuburb]:
                                    if statePid == 'SX':
                                        continue
                                    if statePid not in this.validSuburbs[otherSuburb]:
                                        this.validSuburbs[otherSuburb][statePid] = {}
                                    for src in suburbs[soundCode][otherSuburb][statePid]:
                                        if src not in ['G', 'GA']:            # Only use a G-NAF primary source
                                            continue
                                        if src not in this.validSuburbs[otherSuburb][statePid]:
                                            this.validSuburbs[otherSuburb][statePid][src + 'L'] = suburbs[soundCode][otherSuburb][statePid][src]
        # Add Levenshtein Distance suburbs to this.validSuburbs for all foundTextSuburbs, if not already in this.validSuburb
        this.logger.debug('exandSuburbAndStreets - checking %s', this.foundSuburbText)
        for suburb, isAPI in sorted(this.foundSuburbText):
            if suburb in this.validSuburbs:
                continue
            suburbLength = len(suburb)
            maxDist = int((suburbLength + 2) / 4)
            minLen = max(0, suburbLength - 2)
            maxLen = min(maxSuburbLen, suburbLength + 2)
            processed = set()
            # this.logger.debug('expandSuburbAndStreets - checking from %d to %d', minLen, maxLen - 1)
            for thisLen in range(minLen, maxLen):
                if thisLen in suburbLen:
                    for soundCode in suburbLen[thisLen]:
                        for otherSuburb in suburbLen[thisLen][soundCode]:
                            # this.logger.debug('expandSuburbAndStreets - checking %s with %s', suburb, otherSuburb)
                            if otherSuburb == suburb:
                                continue
                            if otherSuburb in processed:
                                continue
                            processed.add(otherSuburb)
                            dist = jellyfish.levenshtein_distance(suburb, otherSuburb)
                            if dist <= maxDist:
                                if otherSuburb not in this.validSuburbs:
                                    this.validSuburbs[otherSuburb] = {}
                                    this.validSuburbs[otherSuburb]['SX'] = [soundCode, isAPI]
                                for statePid in suburbs[soundCode][otherSuburb]:
                                    if statePid == 'SX':
                                        continue
                                    if statePid not in this.validSuburbs[otherSuburb]:
                                        this.validSuburbs[otherSuburb][statePid] = {}
                                    for src in suburbs[soundCode][otherSuburb][statePid]:
                                        if src not in ['G', 'GA']:            # Only use a G-NAF primary source
                                            continue
                                        if src not in this.validSuburbs[otherSuburb][statePid]:
                                            this.validSuburbs[otherSuburb][statePid][src + 'L'] = suburbs[soundCode][otherSuburb][statePid][src]
        bestSuburb(this)        # Compute the best suburbs
    elif this.fuzzLevel == 6:
        # Add the streets in neighbouring suburbs to the streets in this.validSuburbs for this state
        this.logger.info('expandSuburbAndStreets - adding neighbouring suburbs')
        if this.validState is not None:
            for suburb in sorted(this.validSuburbs):
                this.logger.debug('fuzzLevel 6 - looking for neighbours for suburb(%s), in state(%s)', suburb, states[this.validState][0])
                soundCode = this.validSuburbs[suburb]['SX'][0]
                if (this.validState in suburbs[soundCode][suburb]) and ('GN' in suburbs[soundCode][suburb][this.validState]):
                    this.logger.debug('fuzzLevel 6 - adding source(GN), for state(%s) for suburb(%s) to validSuburbs',
                                      states[this.validState][0], suburb)
                    this.logger.debug('%s', repr(sorted(suburbs[soundCode][suburb][this.validState]['GN'])))
                    this.validSuburbs[suburb][this.validState]['GN'] = suburbs[soundCode][suburb][this.validState]['GN']
        bestSuburb(this)        # Compute the best suburbs
    elif this.fuzzLevel == 7:
        # Add back the soundex and levenshtein streets for this state
        this.logger.info('expandSuburbAndStreets - adding soundex suburbs (same state)')
        for thisLevel in [2, 3]:
            if thisLevel not in this.parkedWrongPostcode:
                continue
            if len(this.parkedWrongPostcode[thisLevel]) == 0:
                continue
            for streetKey in this.parkedWrongPostcode[thisLevel]:
                if streetKey not in this.validStreets:
                    this.validStreets[streetKey] = {}
                for src in this.parkedWrongPostcode[thisLevel][streetKey]:
                    if src not in this.validStreets[streetKey]:
                        this.validStreets[streetKey][src] = {}
                    this.logger.debug('fuzzLevel 7 - adding back (wrong postcode) street(%s), source(%s), places(%s)',
                                     streetKey, src, repr(this.parkedWrongPostcode[thisLevel][streetKey][src]))
                    this.validStreets[streetKey][src].update(this.parkedWrongPostcode[thisLevel][streetKey][src])
    elif this.fuzzLevel == 8:
        # Add back the soundex and levenshtein streets for this state, from soundex and levenshtein suburbs
        this.logger.info('expandSuburbAndStreets - adding soundex and Levenshtein streets (same state)')
        for thisLevel in [4, 5]:
            if thisLevel not in this.parkedWrongPostcode:
                continue
            if len(this.parkedWrongPostcode[thisLevel]) == 0:
                continue
            for streetKey in this.parkedWrongPostcode[thisLevel]:
                if streetKey not in this.validStreets:
                    this.validStreets[streetKey] = {}
                for src in this.parkedWrongPostcode[thisLevel][streetKey]:
                    if src not in this.validStreets[streetKey]:
                        this.validStreets[streetKey][src] = {}
                    this.logger.debug('fuzzLevel 7 - adding back (wrong postcode) street(%s), source(%s), places(%s)',
                                     streetKey, src, repr(this.parkedWrongPostcode[thisLevel][streetKey][src]))
                    this.validStreets[streetKey][src].update(this.parkedWrongPostcode[thisLevel][streetKey][src])
        bestSuburb(this)        # Compute the best suburbs
    elif this.fuzzLevel == 9:
        # Add streets with different street types to this.validStreets
        this.logger.info('expandSuburbAndStreets - adding streets with different street types')
        for streetKey in list(this.validStreets):
            soundCode = this.validStreets[streetKey]['SX'][0]
            streetName = this.validStreets[streetKey]['SX'][1]
            streetType = this.validStreets[streetKey]['SX'][2]
            streetSuffix = this.validStreets[streetKey]['SX'][3]
            for otherType in list(streetTypes) + ['']:            # All the street type, plus no street type
                if otherType == streetType:
                    continue
                if streetSuffix is None:
                    otherKey = '~'.join([streetName, otherType, ''])
                else:
                    otherKey = '~'.join([streetName, otherType, streetSuffix])
                if otherKey in streets[soundCode]:
                    if otherKey not in this.validStreets:
                        this.validStreets[otherKey] = {}
                        this.validStreets[otherKey]['SX'] = [soundCode, streetName, streetType, streetSuffix]
                    srcs = streets[soundCode][otherKey]
                    addSources(this, otherKey, srcs)
        # Add streets with different street types to this.streetName, this.streetType for this.streetName
        if this.streetName is not None:
            for otherType in list(streetTypes) + ['']:            # All the street type, plus no street type
                if (this.streetType is not None) and (otherType == this.streetType):
                    continue
                if this.streetSuffix is None:
                    otherKey = '~'.join([this.streetName, otherType, ''])
                else:
                    otherKey = '~'.join([this.streetName, otherType, this.streetSuffix])
                soundCode = jellyfish.soundex(this.streetName)
                if soundCode in streets:
                    if otherKey in streets[soundCode]:
                        if otherKey not in this.validStreets:
                            this.validStreets[otherKey] = {}
                            this.validStreets[otherKey]['SX'] = [soundCode, this.streetName, this.streetType, this.streetSuffix]
                        srcs = streets[soundCode][otherKey]
                        addSources(this, otherKey, srcs)
    elif this.fuzzLevel == 10:
        # Add streets from other states/postcodes (with the same soundex code)
        this.logger.info('expandSuburbAndStreets - adding soundex streets (other state)')
        if this.streetName is not None:
            soundCode = jellyfish.soundex(this.streetName)
            for streetKey in this.validStreets:
                if this.validStreets[streetKey]['SX'][0] != soundCode:
                    continue
                for src in streets[soundCode][streetKey]:
                    if src == 'SX':
                        continue
                    this.validStreets[streetKey][src] = streets[soundCode][streetKey][src]
    return


def scoreStreet(this, streetPid):
    '''
Score this street
    '''

    streetName = None
    bestStreet = None
    for ii, streetInfo in enumerate(streetNames[streetPid]):
        if (streetName is None) or (streetInfo[3] == 'P'):
            streetName = streetInfo[0]
            streetType = streetInfo[1]
            streetSuffix = streetInfo[2]
            bestStreet = ii
    streetInfo = streetNames[streetPid][bestStreet]
    this.street = streetName
    this.abbrevStreet = streetName
    if streetType != '':
        this.street += ' ' + streetType
        this.abbrevStreet += ' ' + streetTypes[streetType][0]
    if streetSuffix != '':
        this.street += ' ' + streetSuffix
        this.abbrevStreet += ' ' + streetSuffix
    soundCode = jellyfish.soundex(streetName)
    if streetType == '':
        if streetSuffix == '':
            streetKey = streetName + '~~'
        else:
            streetKey = '~'.join([streetName, '', streetSuffix])
    elif streetSuffix == '':
        streetKey = '~'.join([streetName, streetType, ''])
    else:
        streetKey = '~'.join([streetName, streetType, streetSuffix])
    this.result['street'] = this.street
    # Find the best 'source' for this streetPid
    this.result['score'] &= ~1792
    bestSource = None
    for key in this.validStreets:
        for src in ['G', 'GA', 'GS', 'GL', 'GAS', 'GAL']:
            if src not in this.validStreets[key]:
                continue
            if streetPid not in this.validStreets[key][src]:
                continue
            if src == 'G':
                bestSource = src
                this.result['score'] |= 1792
            elif src == 'GA':
                bestSource = src
                this.result['score'] |= 1536
            elif src in ['GS', 'GAS']:
                bestSource = src
                this.result['score'] |= 1024
            else:
                this.result['score'] |= 768
            break
    return streetKey, soundCode, streetInfo, bestSource


def returnHouse(this, streetPid, exactHouse):
    '''
Set up the return data with the geocoding for this.houseNo in this street
And score this returned data
    '''

    streetKey, soundCode, streetInfo, bestSource = scoreStreet(this, streetPid)
    if streetKey == '':        # Deal with 'Unused variable' error in Visual Code
        pass
    if exactHouse:
        this.result['score'] |= 6144
    else:
        this.result['score'] |= 4096
    meshBlock = streetNos[streetPid][this.houseNo][0]
    sa1 = SA1map[meshBlock]
    lga = LGAmap[meshBlock]
    latitude = streetNos[streetPid][this.houseNo][1]
    longitude = streetNos[streetPid][this.houseNo][2]
    gnafid = streetNos[streetPid][this.houseNo][4]
    this.result['Mesh Block'] = meshBlock
    this.result['SA1'] = sa1
    this.result['LGA'] = lga
    this.result['latitude'] = latitude
    this.result['longitude'] = longitude
    this.result['G-NAF ID'] = gnafid
    this.result['status'] = 'Address found'
    this.result['accuracy'] = '4'
    if streetPid not in streetLocalities:
        this.logger.critical('returnHouse - configuration error - streetPid not in streetLocalities')
    else:
        locality = streetLocalities[streetPid]
        if locality not in localities:
            this.logger.critical('returnHouse - configuration error - localityPid not in localities')
        else:
            this.logger.debug('returnHouse - locality for street(%s) is (%s)', streetPid, locality)
            suburb = None
            postcode = None
            statePid = None
            if locality in localityPostcodes:
                if (this.validPostcode is not None) and (this.validPostcode in localityPostcodes[locality]):
                    postcode = this.validPostcode
                else:
                    postcode = list(localityPostcodes[locality])[0]
                this.logger.debug('returnHouse - postcode [from localityPostcodes] for locality(%s) is (%s)', locality, postcode)
            this.logger.debug('returnHouse - there are %d options for locality(%s)', len(localities[locality]), locality)
            for thisStatePid, thisSuburb, thisAlias in localities[locality]:
                if thisAlias == 'P':        # Go for the first primary locality
                    statePid = thisStatePid
                    suburb = thisSuburb
                    break
            if suburb is None:
                thisStatePid, thisSuburb, thisAlias = list(localities[locality])[0]
                statePid = thisStatePid
                suburb = thisSuburb
            this.result['state'] = states[statePid][0]
            this.result['score'] &= ~3
            this.logger.debug('returnHouse - suburb(%s)', suburb)
            # Score suburb
            if suburb not in sorted(this.validSuburbs):
                # If not a valid suburb, then make it a valid suburb as it must be a primary for a passed suburb
                isAPI = False
                if len(this.foundSuburbText) > 0:
                    thisFoundSuburb, isAPI = this.foundSuburbText[0]
                else:
                    isAPI = False
                soundCode = jellyfish.soundex(suburb)
                # Only add exact matches for this suburb
                if (soundCode in suburbs) and (suburb in suburbs[soundCode]):
                    this.logger.debug('returnHouse - adding suburb(%s) to validSuburbs', suburb)
                    if suburb not in this.validSuburbs:
                        this.validSuburbs[suburb] = {}
                        this.validSuburbs[suburb]['SX'] = [soundCode, isAPI]
                        if statePid in suburbs[soundCode][suburb]:
                            if statePid not in this.validSuburbs[suburb]:
                                this.validSuburbs[suburb][statePid] = {}
                            for src in ['G', 'GA', 'A']:            # Only add primary sources
                                if (src in suburbs[soundCode][suburb][statePid]) and (src not in this.validSuburbs[suburb][statePid]):
                                    this.logger.debug('returnHouse - adding source(%s), for state(%s) for suburb(%s) to validSuburbs',
                                                      src, states[statePid][0], suburb)
                                    this.logger.debug('returnHouse - (%s)', repr(sorted(suburbs[soundCode][suburb][statePid][src])))
                                    this.validSuburbs[suburb][statePid][src] = suburbs[soundCode][suburb][statePid][src]
            scoreSuburb(this, suburb, statePid)
            this.result['score'] &= ~12
            if postcode is not None:
                this.logger.debug('returnHouse - setting postcode to (%s)', postcode)
                this.result['postcode'] = postcode
                if this.validPostcode is not None:
                    if this.validPostcode == postcode:
                        if this.isAPIpostcode:
                            this.result['score'] |= 12
                        else:
                            this.result['score'] |= 8
                    else:
                        this.result['score'] |= 4
            else:
                this.result['postcode'] = ''
            this.logger.debug('returnHouse - setting state to (%s)', states[statePid][0])
            this.result['state'] = states[statePid][0]
            if this.validState is not None:
                if this.validState == statePid:
                    if this.isAPIstate:
                        this.result['score'] |= 3
                    else:
                        this.result['score'] |= 2
                else:
                    this.result['score'] |= 1
    setupAddress1Address2(this, None)
    return


def returnStreetPid(this, streetPid):
    '''
Set up the return data with the geocoding for this street
And score the returned data
    '''

    this.logger.debug('returnStreetPid - streetPid(%s)', streetPid)

    streetKey, soundCode, streetInfo, bestSource = scoreStreet(this, streetPid)
    if (soundCode in streets) and (streetKey in streets[soundCode]) and (bestSource in streets[soundCode][streetKey]) and (streetPid in streets[soundCode][streetKey][bestSource]):
        thisSA1, thisLGA, latitude, longitude = streets[soundCode][streetKey][bestSource][streetPid]
    else:
        this.logger.critical('returnStreetPid - configuration error - streetPid not in streets')
        return
    streetName = streetInfo[0]
    streetType = streetInfo[1]
    streetSuffix = streetInfo[2]
    this.street = streetName
    this.abbrevStreet = streetName
    if streetType != '':
        this.street += ' ' + streetType
        this.abbrevStreet += ' ' + streetTypes[streetType][0]
    if streetSuffix != '':
        this.street += ' ' + streetSuffix
        this.abbrevStreet += ' ' + streetSuffix
    if this.result['isPostalService']:
        if this.postalServiceText3 is not None:
            this.postalServiceText2 = this.street  + this.postalServiceText3
        else:
            this.postalServiceText2 = this.street
    this.result['street'] = this.street
    this.result['SA1'] = thisSA1
    this.result['LGA'] = thisLGA
    this.result['latitude'] = latitude
    this.result['longitude'] = longitude
    this.result['G-NAF ID'] = 'S-' + str(streetPid)
    this.result['status'] = 'Street address found'
    this.result['accuracy'] = '3'
    if streetPid not in streetLocalities:
        this.logger.critical('returnStreetPid - configuration error - streetPid not in streetLocalities')
    else:
        locality = streetLocalities[streetPid]
        if locality not in localities:
            this.logger.critical('returnStreetPid - configuration error - localityPid not in localities')
        else:
            this.logger.critical('returnStreetPid - locality (%s)', locality)
            postcode = None
            if locality in localityPostcodes:
                if (this.validPostcode is None) or (this.validPostcode not in localityPostcodes[locality]):
                    postcode = list(sorted(localityPostcodes[locality]))[0]
                    this.logger.critical('returnStreetPid - locality postcode (%s)', postcode)
                else:
                    postCode = this.validPostcode
            # Pick primary locality from localities[localityPid]
            this.logger.critical('returnStreetPid - choosing statePid (and suburb if None) from (%s)', localities[locality])
            suburb = None
            statePid = None
            for thisStatePid, thisSuburb, thisAlias in localities[locality]:
                if thisAlias == 'P':        # Go for the first primary locality
                    statePid = thisStatePid
                    suburb = thisSuburb
                    break
            if (suburb is None) or (statePid is None):
                this.logger.critical('returnStreetPid - missing suburb (%s) or statePid (%s) - choosing from (%s)', suburb, statePid, localities[locality])
                statePid, suburb, thisAlias = list(localities[locality])[0]
            this.suburb = suburb
            this.result['suburb'] = suburb
            this.result['state'] = states[statePid][0]
            this.result['score'] &= ~3
            if this.validState is not None:
                if (statePid is not None) and (this.validState == statePid):
                    if this.isAPIstate:
                        this.result['score'] |= 3
                    else:
                        this.result['score'] |= 2
                else:
                    this.result['score'] |= 1
            if suburb not in this.validSuburbs:
                # If not a valid suburb, then make it a valid suburb (before scoreSuburb())
                soundCode = jellyfish.soundex(suburb)
                # Only add exact matches for this suburb
                if (soundCode in suburbs) and (suburb in suburbs[soundCode]):
                    this.logger.debug('returnStreetPid - adding suburb(%s) to validSuburbs', suburb)
                    if suburb not in this.validSuburbs:
                        this.validSuburbs[suburb] = {}
                        this.validSuburbs[suburb]['SX'] = [soundCode, False]
                        if statePid in suburbs[soundCode][suburb]:
                            if statePid not in this.validSuburbs[suburb]:
                                this.validSuburbs[suburb][statePid] = {}
                            for src in ['G', 'GA', 'A']:            # Only add primary sources
                                if (src in suburbs[soundCode][suburb][statePid]) and (src not in this.validSuburbs[suburb][statePid]):
                                    this.logger.debug('returnStreetPid - adding source(%s), for state(%s) for suburb(%s) to validSuburbs',
                                                      src, states[statePid][0], suburb)
                                    this.logger.debug('returnStreetPid - (%s)', repr(sorted(suburbs[soundCode][suburb][statePid][src])))
                                    this.validSuburbs[suburb][statePid][src] = suburbs[soundCode][suburb][statePid][src]
            # Score suburb
            scoreSuburb(this, suburb, statePid)
            this.result['score'] &= ~12
            if postcode is not None:
                this.result['postcode'] = postcode
                if this.validPostcode is not None:
                    if this.validPostcode == postcode:
                        if this.isAPIpostcode:
                            this.result['score'] |= 12
                        else:
                            this.result['score'] |= 8
                    else:
                        this.result['score'] |= 4
            else:
                this.result['postcode'] = ''
    return


def verifyAddress(this):
    '''
Verify an address
this.Address is a dictionary. It must contain an array of 'addressLines' even if there is only one line.
It may contain a 'suburb' although this will be found in the 'addressLines' if there is no 'suburb' in the dictionary.
It may contain a 'postcode' although this will be found in the 'addressLines' if there is no 'postcode' in the dictionary.
It may contain a 'state' although this will be found in the 'addressLines' if there is no 'state' in the dictionary.

verifyAddress is passed a dictionary containing
addressLines - one or more lines of address data, which get concatenated for analysis
state [optional] - the state which, if present, must not be in any of the addressLines
suburb [optional] - the suburb which, if present, must not be in any of the addressLines
postcode [optional] - the postcode which, if present, must not be in any of the addressLines

verifyAddress returns a dictionary of results containing a status, a score and a normalized address of suburb,
state, suburb and address lines (addressLine1 and addressLine2).
addressLine1 is the trim (Unit/Flat etc) from the start of the concatenated addressLines.
addressLine2 contains the remainder of the concatenated addressLines, less suburb, state and postcode.
If there is no trim then addressLine1 contains the concatenated addressLines, less suburb, state and postcode,
and addressLine2 is blank.

The accuracy is
1 - if only the Postcode is valid - (G-NAF ID is a postcode from postcodeSA1LGA.psv [Australia Post])
2 - if only the Suburb and Postcode valid - (G-NAF ID is a 'suburb name' + '~' + postcode from postcodeSA1LGA.psv [Australia Post])
3 - if only the street is valid - (G-NAF ID is a G-NAF locality_pid)
4 - if the property is valid - (G-NAF ID is a G-NAF address_detail_pid)

    '''

    # Initialize the returned data
    this.result = {}
    this.result['id'] = ''
    this.result['isPostalService'] = False
    this.result['buildingName'] = ''
    this.result['houseNo'] = ''
    this.result['street'] = ''
    this.result['addressLine1'] = ''
    this.result['addressLine2'] = ''
    if returnBoth:
        this.result['addressLine1Abbrev'] = ''
        this.result['addressLine2Abbrev'] = ''
    this.result['state'] = ''
    this.result['suburb'] = ''
    this.result['postcode'] = ''
    this.result['SA1'] = ''
    this.result['LGA'] = ''
    this.result['Mesh Block'] = ''
    this.result['G-NAF ID'] = ''
    this.result['longitude'] = ''
    this.result['latitude'] = ''
    this.result['score'] = 0
    this.result['status'] = 'Address not found'
    this.result['accuracy'] = '0'
    this.result['messages'] = []
    this.subsetValidStreets = set()
    this.neighbourhoodSuburbs = {}
    this.foundSuburbText = []
    this.foundBuildings = []
    this.isPostalService = False
    this.street = None
    this.abbrevStreet = None
    this.streetName = None
    this.streetType = None
    this.streetSuffix = None
    if not isinstance(this.Address, dict):
        this.result['status'] = 'Invalid address'
        this.result['messages'].append('Bad data - no Address Data')
        return
    if 'addressLines' not in this.Address:
        this.result['status'] = 'Invalid address'
        this.result['messages'].append('Insufficient Address data - no addressLines')
        return
    if not isinstance(this.Address['addressLines'], list):      # addressLines must be an array
        this.result['status'] = 'Invalid address'
        this.result['messages'].append('no array of addressLines')
        return

    # Remember id if one was provided
    if ('id' in this.Address) and (this.Address['id'] != ''):            # Check if id supplied
        this.result['id'] = this.Address['id']

    '''
    Parse API data
    '''
    this.validState = None
    this.validPostcode = None
    this.returnState = ''
    this.returnPostcode = ''
    this.returnSuburb = ''
    this.validSuburbs = {}
    this.validStreets = {}
    this.houseNo = None
    this.houseTrim = None
    this.isAPIpostcode = False
    if ('postcode' in this.Address) and (this.Address['postcode'] != ''):            # Check if postcode supplied
        postcode = cleanText(this.Address['postcode'], True)
        if NTpostcodes:
            if (len(postcode) == 3) and (postcode[0] == '8'):
                postcode = '0' + postcode
        if postcode in postcodes:
            this.logger.info('Postcode(%s) is a valid postcode', postcode)
            this.validPostcode = postcode
            this.result['postcode'] = postcode
            this.result['score'] |= 4
            this.isAPIpostcode = True
        else:
            this.logger.info('Postcode(%s) is not a valid postcode', postcode)
            this.result['messages'].append(f'Bad postcode({postcode})')
    this.isAPIstate = False
    if ('state' in this.Address) and (this.Address['state'] != ''):                    # Check if state supplied
        '''
        Cleans state
        '''
        state = cleanText(this.Address['state'], True)
        for statePid, stateInfo in states.items():                                    # Look for an exact match for this state
            for pattern in stateInfo[1:]:
                match = pattern.match(state)
                if (match is not None) and (match.start() == 0) and (match.end() == len(state)):
                    # Perfect match - state found
                    this.logger.info('state(%s) is a valid state', state)
                    this.validState = statePid
                    this.result['state'] = stateInfo[0]
                    this.result['score'] |= 1
                    this.isAPIstate = True
                    break
            else:
                continue
            break
        else:
            this.logger.info('state(%s) is not a valid state', state)
            this.result['messages'].append('Bad state({state})')

    # Building up a single string (address) from the addressLine(s)
    addressLine = ''
    for thisLine in this.Address['addressLines']:           # Ignore blank addressLine(s)
        if (thisLine is not None) and (thisLine != ''):
            thisLine = thisLine.strip()
            # Put the first line in addressLine1. Concatentate the reset into addressLine2
            # We'll replace these with G-NAF data if the address gets verified
            if addressLine != '':
                if this.result['addressLine2'] == '':
                    this.result['addressLine2'] = thisLine
                else:
                    this.result['addressLine2'] += ', ' + thisLine
                addressLine += ', '             # Join the lines with ', ' as the lines my imply structure
            else:
                this.result['addressLine1'] = thisLine
            addressLine += thisLine
    '''
    Cleans addressLine
    '''
    addressLine = cleanText(addressLine, False)       # Clean up the address line
    this.logger.info('addressLine:%s', addressLine)

    # Check the state and/or postcode is in the addressLine (i.e. not passed as atomic data)
    if (this.validState is None) or (this.validPostcode is None):
        this.logger.debug('Looking for state and/or postcode in Address line (%s)', addressLine)
        '''
        Scan addressLine backward for state/postcode
        '''
        parts = addressLine.split(',')        # Split a the commas and treat each bit as independant
        parts = list(map(str.strip, parts))
        partNo = len(parts) - 1                # Start with the last part (the end of the addressLine)
        while ((this.validState is None) or (this.validPostcode is None)) and (partNo >= 0):
            subParts = parts[partNo].split(' ')            # Split this into words - look for the longest matching string of words
            subParts = list(map(str.strip, subParts))
            firstSubPart = 0                    # Start with all the words
            endSubPart = len(subParts)
            while firstSubPart < endSubPart:    # Keep checking till we run out of words
                thisPart = ' '.join(subParts[firstSubPart:endSubPart])        # Create a phrase of words
                found = False
                if this.validPostcode is None:                # Check if we need a postcode
                    if thisPart in postcodes:                    # Check if this is a postcode
                        # postcode found
                        this.logger.info('Postcode(%s) is a valid postcode', thisPart)
                        this.validPostcode = thisPart
                        this.result['postcode'] = thisPart
                        this.result['score'] |= 4
                        found = True
                    elif NTpostcodes and (len(thisPart) == 3) and (thisPart[0] == '8') and ('0' + thisPart in postcodes):                    # Check if this is a postcode
                        # postcode found
                        this.logger.info('Postcode(%s) is a valid postcode', '0' + thisPart)
                        this.validPostcode = '0' + thisPart
                        this.result['postcode'] = '0' + thisPart
                        this.result['score'] |= 4
                        found = True
                if (this.validState is None) and not found:    # Check if we need a state and this is a candidate
                    for state, stateInfo in states.items():                        # Check if this is a state or abbrevated state
                        for pattern in stateInfo[1:]:
                            match = pattern.match(thisPart)
                            if (match is not None) and (match.start() == 0) and (match.end() == len(thisPart)):
                                # Perfect match - state found
                                this.logger.info('state(%s) is a valid state', thisPart)
                                this.validState = state
                                this.result['state'] = states[state][0]
                                this.result['score'] |= 1
                                found = True
                                break
                        else:
                            continue
                        break
                if found:                    # If this phrase matched a postcode or state then remove it from the addressLine
                    for ii in range(endSubPart - 1, firstSubPart - 1, -1):
                        this.logger.debug('Scan address backwards: removing subparts(%s)', subParts[ii])
                        del subParts[ii]
                    endSubPart = firstSubPart        # And check what's left of this words in this part
                    firstSubPart = 0
                else:
                    firstSubPart += 1        # No match, make the phrase one word shorts (lop one of the front)
            endSubPart -= 1            # Ran out of words ending here, so try phrases that end one word earlier
            firstSubPart = 0
            parts[partNo] = ' '.join(subParts)    # And restore what's left of this part
            partNo -= 1                            # And try the next part (one further from the end)

        for ii in range(len(parts) -1, -1, -1):        # Clean up any parts that became empty (removed as they were matched)
            if parts[ii] == '':
                this.logger.debug('Scan address backwards: removing parts[%d]', ii)
                del parts[ii]
        addressLine = ' '.join(parts)        # Restore addressLine
    else:
        addressLine = addressLine.replace(',', ' ')
    if this.validState is None:
        this.logger.info('No state found in address line')
    if this.validPostcode is None:
        this.logger.info('No postcode found in address line')
    this.logger.debug('Address line is now (%s)', addressLine)

    if ('suburb' in this.Address) and (this.Address['suburb'] != ''):            # Check if suburb supplied
        this.logger.debug('Checking passed suburb (%s)', this.Address['suburb'])
        suburb = cleanText(this.Address['suburb'], False)
        leftOvers = scanForSuburb(this, suburb, 'forwards', True)            # Find all the suburbs in the 'suburb'
        if len(this.validSuburbs) == 0:
            this.result['messages'].append(f'Bad suburb({suburb})')
        else:
            this.result['suburb'] = sorted(list(this.validSuburbs))[0]
        if leftOvers != '':
            addressLine += ' ' + leftOvers
            this.logger.debug('Addres line is now (%s)', addressLine)

    '''
    Strip Trim
    '''
    # Find all the buildings - their geocode information may be more accurate than a just a 'suburb'
    # And small communities often share a building name for all houses in the community
    buildingAt = None
    this.logger.debug('Checking for building names')
    for building in sorted(buildings.keys(), key=len, reverse=True):
        matched = buildingPatterns[building].search(addressLine)
        if matched is not None:
            buildingAt = matched.start()
            this.logger.debug('Strip Trim: building(%s) found ', building)
            for buildingInfo in buildings[building]:
                this.foundBuildings.insert(0, [building] + buildingInfo + [buildingAt])
    if len(this.foundBuildings) > 0:
        this.logger.debug('Buildings: (%s)', this.foundBuildings)
    else:
        this.logger.debug('None found')

    # Look for the last digits
    houseEnd = None
    trimEnd = None
    lastDigits = None
    this.isLot = False
    this.isRange = False
    this.houseTrim = None
    this.logger.debug('Checking for a house number')
    for digits in lastDigit.finditer(addressLine):
        if digits.end() != len(addressLine):    # Don't let a bad postcode look like house digits
            lastDigits = digits
    if lastDigits is None:                # No digits in the address
        trimEnd = len(addressLine)
    else:
        trimEnd = lastDigits.start()    # The start of the number or number range. There is no trim after this.
        houseEnd = len(lastDigits.group())        # The length of the house number
        this.houseEnd = houseEnd
        this.houseTrim = lastDigits.group()
        if lastDigits.group(8) is not None:        # Was there a number in the second half of a number range
            this.isRange = True
            this.houseNo = int(lastDigits.group(8))        # If so, then it's the house number
            this.result['houseNo'] = str(this.houseNo)
        else:            # Just a number, not a  number range
            this.houseNo = int(lastDigits.group(2))        # So the first number is the house number
            this.result['houseNo'] = str(this.houseNo)

            # Check if there was 'LOT ' before the first number
            LOTmatch = LOTpattern.search(addressLine)
            if LOTmatch is not None:        # There was a LOT number somewhere
                if LOTmatch.start(3) == lastDigits.start(2):        # Check if the LOT number was the houseNo just found
                    this.isLot = True
                    trimEnd = LOTmatch.start()                # The start of the LOT number. There is no trim after this.
                    houseEnd = len(LOTmatch.group())        # The length of the house number
                    this.houseTrim = LOTmatch.group()
                    this.houseEnd = houseEnd

    # Now remove the trim
    # If there's a house number, then trim stops at the house number
    this.trim = None
    if not removePostalService(this, addressLine, trimEnd, houseEnd):       # Postal addresses can't have flat, levels, extra trim
        this.logger.debug('Removing flats, levels and trims')
        # Find the longest trim
        removeFlats(this, addressLine, trimEnd)
        removeLevels(this, addressLine, trimEnd)
        removeExtraTrims(this, addressLine, trimEnd)

    # Remove any left over garbage trim - if we can do so safely
    if lastDigits is not None:      # We have a boundary between trim and address
        if not this.isPostalService:
            this.trim = addressLine[:trimEnd].strip()
            if this.trim == '':
                this.trim = None
            addressLine = addressLine[trimEnd + houseEnd:].strip()
        else:
            this.trim = this.postalServiceText1
            trimEnd = len(this.postalServiceText1)
            addressLine = addressLine[trimEnd:].strip()
    else:
        if this.trim is not None:       # We have 'trim', but it might wipe out the street and/or suburb
            trimEnd = len(this.trim)
        else:
            trimEnd = 0
        # Set Trim end to the end of any buildings
        for thisBuildingInfo in this.foundBuildings:
            buildingName = thisBuildingInfo[0]
            buildingEnd = thisBuildingInfo[4] + len(buildingName)
            if (buildingEnd > trimEnd) and (buildingEnd < len(addressLine)):
                trimEnd = buildingEnd
        this.trim = addressLine[:trimEnd].strip()
        addressLine = addressLine[trimEnd:].strip()

    this.logger.debug('Trim (%s) found, trimEnd(%d), addressLine(%s)', this.trim, trimEnd, addressLine)


    '''
    Search for street type in addressLine
    '''
    streetTypeAt = None
    streetTypeEnd = None
    streetAt = None
    streetEnd = None
    streetSuffixEnd = None
    extraText = ''
    skipped = []
    if addressLine != '':
        this.logger.debug('Searching for street type in addressLine (%s)', addressLine)
        for streetType, streetTypeInfo in streetTypes.items():
            for streetTypePattern in streetTypeInfo[1:]:
                match = streetTypePattern.search(addressLine)
                if match is not None:
                    if streetType in streetTypeSuburbs:
                        skipIt = False
                        for thisSuburb in streetTypeSuburbs[streetType]:
                            isSuburb = thisSuburb.search(addressLine)
                            if (isSuburb is not None) and (isSuburb.start() < match.start()) and (isSuburb.end() == match.end()):
                                this.logger.debug('Skipping street type (%s) in favor of suburb (%s)', streetType, isSuburb.group())
                                skipped.append((streetType, match.start(), match.end(), isSuburb.group()))
                                skipIt = True
                                break
                        if skipIt:
                            continue
                    this.logger.debug('Street type (%s) found in addressLine (%s)', streetType, addressLine)
                    if (this.streetType is None) or (this.streetType not in streetTypeCount) or (streetType not in streetTypeCount):
                        this.streetType = streetType
                        streetTypeAt = match.start()
                        streetTypeEnd = match.end()
                    elif streetTypeCount[streetType] >= streetTypeCount[this.streetType]:
                        this.streetType = streetType
                        streetTypeAt = match.start()
                        streetTypeEnd = match.end()


        if streetTypeAt is None:    # No streetType in address
            this.logger.debug('No street type found - scanning for streets with no street type')
            '''
            Scan for street with no street type
            '''
            streetAt = None
            streetEnd = None
            for shortStreet in reversed(sorted(shortStreets)):
                found = shortStreets[shortStreet]['regex'].search(addressLine)
                if found is not None:
                    this.logger.debug('Short street (%s) found in addressLine (%s)', shortStreet, addressLine)
                    foundEnd = found.end() + len(shortStreet)
                    if (streetEnd is None) or (foundEnd > streetEnd):
                        streetAt = found.start()
                        streetEnd = foundEnd
            if streetAt is not None:
                this.streetName = addressLine[streetAt:streetEnd].strip()
                if this.streetName == '':
                    this.streetName = None
                elif (lastDigits is not None) and (this.trim == this.streetName):       # A street who's name is a number!!!
                    this.trim = None
                    trimEnd = 0
                else:
                    this.trim = addressLine[:streetAt]
                    trimEnd = len(this.trim)
                extraText = addressLine[streetEnd:].strip()
            else:
                this.streetName = None
                extraText = addressLine
        else:
            this.streetName = addressLine[:streetTypeAt].strip()        # Includes trim
            if this.streetName == '':
                this.streetName = None
            elif (lastDigits is None) and (trimEnd > streetTypeAt):
                this.trim = None
                trimEnd = 0
            extraText = addressLine[streetTypeEnd:].strip()
    if streetTypeAt is not None:
        this.logger.info('Trim (%s), Street name (%s %s), extraText (%s)', this.trim, this.streetName, this.streetType, extraText)
    elif streetAt is not None:
        this.logger.info('Trim (%s), Street name (%s), extraText (%s)', this.trim, this.streetName, extraText)
    else:               # Scan for a word that sounds like a street type
        this.logger.debug('No street type/street name found - scanning for sounds like street types')
        words = addressLine.split(' ')
        if len(words) > 2:
            at = 0
            for ii in range(1, len(words) -1 ):
                at += len(words[ii - 1]) + 1
                soundCode = jellyfish.soundex(words[ii])
                if soundCode in streetTypeSound:
                    streetType = streetTypeSound[soundCode][0]
                    maxDist = int(len(streetType) / 2)
                    dist = jellyfish.levenshtein_distance(streetType, words[ii])
                    if dist >= maxDist:
                        continue
                    this.logger.debug('Street type (%s) with sound (%s) found for word (%s)', streetType, soundCode, words[ii])
                    if (this.streetType is None) or (this.streetType not in streetTypeCount) or (streetType not in streetTypeCount):
                        this.streetType = streetType
                        streetTypeAt = at
                        streetTypeEnd = at + len(words[ii])
                    elif streetTypeCount[streetType] >= streetTypeCount[this.streetType]:
                        this.streetType = streetType
                        streetTypeAt = at
                        streetTypeEnd = at + len(words[ii])
        if streetTypeAt is not None:
            this.streetName = addressLine[:streetTypeAt].strip()        # Includes trim
            if this.streetName == '':
                this.streetName = None
            extraText = addressLine[streetTypeEnd:].strip()
            this.logger.info('Trim (%s), Street name (%s %s), extraText (%s)', this.trim, this.streetName, this.streetType, extraText)
        elif len(skipped) > 0:       # Skipped a street type, but no other street type found
            this.logger.info('No street type found - restoring last skipped street type')
            minAt = None
            for thisStreetType, thisStart, thisEnd, thisSuburb in skipped:
                if (minAt is None) or (minAt > thisStart):
                    this.streetType = thisStreetType
                    streetTypeAt = thisStart
                    streetTypeEnd = thisEnd
            this.streetName = addressLine[:streetTypeAt].strip()        # Includes trim
            if this.streetName == '':
                this.streetName = None
            elif (lastDigits is None) and (trimEnd > streetTypeAt):
                this.trim = None
                trimEnd = 0
            extraText = addressLine[streetTypeEnd - len(thisSuburb):].strip()       # Leave the skipped suburb in the address line so we can find it as a street
        else:
            this.logger.info('No street type/street name found')

    # Check extraText for streetSuffix
    if extraText != '':
        if streetTypeAt is not None:
            this.logger.debug('Street Type found (%s), checking for street type suffix in (%s)', this.streetType, extraText)
            for suffix in reversed(sorted(streetSuffixes)):
                for streetSuffixPattern in streetSuffixes[suffix]:
                    matched = streetSuffixPattern.search(extraText)
                    if matched is not None:
                        streetSuffixEnd = matched.end()
                        this.streetSuffix = matched.group()
                        extraText = extraText[streetSuffixEnd:].strip()
                        break
                else:
                    continue
                break
        # Scan for suburbs in extraText
        if extraText != '':
            leftOvers = scanForSuburb(this, extraText, 'backwards', False)
            if (leftOvers != extraText) and this.isPostalService:
                # OUCH - we have a postal delivery service, followed by some address stuff, which includes a suburb!
                # this.postalServiceText2 was addressLine before we started looking for a street
                # We need to fix it up
                if leftOvers != '':
                    leftOvers = ' ' + leftOvers
                if streetSuffixEnd is not None:        # We have a street name, street type and street suffix
                    this.postalServiceText2 = addressLine[:streetTypeAt] + this.streetType + ' ' + this.streetSuffix  + leftOvers
                elif streetTypeAt is not None:
                    this.postalServiceText2 = addressLine[:streetTypeAt] + this.streetType + leftOvers
                elif streetAt is not None:
                    this.postalServiceText2 = addressLine[:streetEnd] + leftOvers
                else:
                    this.postalServiceText2 = leftOvers
                this.postalServiceText3 = leftOvers

    # Set up street and abbrevStreet
    if this.streetName is not None:
        if (lastDigits is None) and (this.trim is not None) and (this.streetName.startswith(this.trim)):
            if this.trim == this.streetName:
                this.street = this.streetName
                this.trim = None
            else:
                this.street = this.streetName[trimEnd:].strip()
        else:
            this.street = this.streetName       # May included trim (but trim will be None)
        this.abbrevStreet = this.streetName
        if this.streetType is not None:
            this.street += ' ' + this.streetType
            this.abbrevStreet += ' ' + streetTypes[this.streetType][0]
        if this.streetSuffix is not None:
            this.street += ' ' + this.streetSuffix
            this.abbrevStreet += ' ' + this.streetSuffix

    '''
    Rules 1 and 2
    '''
    this.fuzzLevel = 0      # No fuzzy logic for Rules 1 and 2 tests
    if not Rules1and2(this):
        # We have very little address data
        this.logger.debug('Failed Rules1and2')
        this.result['status'] = 'Address not found'
        this.result['accuracy'] = '0'
        setupAddress1Address2(this, None)
        return

    '''
    Create Valid Streets
    '''
    this.parkedWrongState = {}
    this.parkedWrongPostcode = {}
    createValidStreets(this)

    streetFound = None
    bestStreetPid = None
    for thisFuzz in fuzzLevels:
        this.fuzzLevel = thisFuzz
        this.result['fuzzLevel'] = thisFuzz
        this.logger.debug('fuzzLevel(%d)', this.fuzzLevel)
        if this.fuzzLevel > 1:
            expandSuburbsAndStreets(this)

        '''
        Check for valid street
        '''
        streetFound = validateStreets(this)

        if streetFound:
            # Pick the best street from this.allStreetSources
            bestStreetPid = None
            bestWeight = None
            for streetPid in this.subsetValidStreets:
                srcs = this.allStreetSources[streetPid].split('~')
                if srcs[1] not in suburbSourceWeight:
                    continue
                if srcs[0] not in streetSourceWeight:
                    continue
                weight = 5 * suburbSourceWeight[srcs[1]] + 10 * streetSourceWeight[srcs[0]]
                if (bestWeight is None) or (bestWeight < weight):        # We found a better street
                    bestWeight = weight
                    bestStreetPid = streetPid
            if bestStreetPid is not None:
                streetPid = bestStreetPid
                returnStreetPid(this, streetPid)
                this.logger.debug('street found: %s', streetPid)
                # If we don't have a house number, then this is as good as it is going to get
                # We may have a building, but it certainly won't be in this street, in this suburb
                if this.houseNo is None:
                    setupAddress1Address2(this, None)
                    return

            '''
            Check house number - as we have some candidate streets
            '''
            if checkHouseNo(this):
                this.logger.debug('house found')
                return

        # For postal delivery services, with no street, we only need a valid suburb
        this.logger.debug('isPostalService(%s), street(%s), bestSurburb(%s)', this.isPostalService, this.street, this.bestSuburb)
        if this.isPostalService and (this.street is None) and (this.bestSuburb is not None):
            break

        ### NT SPECIFIC HACK FOR SMALL COMMUNITIES - COMMUNITY === SUBURB === STREET (HOUSES HAVE COMMUNITY NUMBERS)
        if thisFuzz == 8:       # Don't try streets with other street types if we have a valid street
            if streetFound:
                break
        ### END NT SPECIFIC HACK FOR SMALL COMMUNITIES - COMMUNITY === SUBURB === STREET (HOUSES HAVE COMMUNITY NUMBERS)

    # No more fuzz levels - return a street answer if we ever had any valid streets
    if this.isPostalService and (this.street is None) and (this.bestSuburb is not None):
        # We have a best suburb - a suburb that's in the valid state and in the valid postcode
        this.result['score'] &= ~3
        this.result['score'] |= 3
        if this.isAPIpostcode:
            this.result['score'] |= 12        # An API postcode was supplied and matched the supplied suburb
        else:
            this.result['score'] |= 8        # An address line postcode was supplied and matched the supplied suburb
        isPost = accuracy2(this, this.bestSuburb, this.validState)
        if isPost:        # Deal with 'Unused variable' error in Visual Code
            pass
        setupAddress1Address2(this, None)
        return
    if streetFound and bestStreetPid is not None:
        if scoreBuilding(this, None, None):            # See if we can do better with a building name that matches one of these suburbs, with a house that has this house number
            this.logger.debug('building found')
            return
        setupAddress1Address2(this, None)
        return
    if not streetFound:
        # We have no streets within suburbs but, if we have suburbs, we may be able to return suburb level geocode data
        if len(this.validSuburbs) > 0:            # We have suburbs
            if scoreBuilding(this, None, None):            # See if we can do better with a building name that matches one of these suburbs, with a house that has this house number
                this.logger.debug('building found')
                return
            thisSuburb = None
            thisState = None
            thisPostcode = None
            if this.bestSuburb is not None:            # We have a best suburb, in the validState, in the validPostcode
                thisSuburb = this.bestSuburb
                thisState = this.validState
                thisPostcode = this.validPostcode
            elif len(this.suburbInState) > 0:        # We have suburbs in the validState
                thisSuburb = sorted(list(this.suburbInState))[0]
                thisState = this.validState
            elif len(this.suburbInPostcode) > 0:    # We have suburbs in the validPostcode
                thisSuburb = sorted(list(this.suburbInPostcode))[0]
                thisPostcode = this.validPostcode
            else:
                thisSuburb = sorted(list(this.validSuburbs))[0]            # Pick the first one and try and work out the state and postcode
            if thisPostcode is not None:            # We have a postcode in postcodes to work with
                if len(postcodes[thisPostcode]['states']) > 1:
                    # The specified postcode crosses a state boundary - so state cannot be determined
                    this.result['suburb'] = thisSuburb
                    this.result['score'] = 0
                    this.result['score'] |= 16            # A suburb was supplied
                    this.result['state'] = ''
                    if this.validState is not None:
                        this.result['score'] |= 1        # A state was supplied
                    this.result['postcode'] = thisPostcode
                    if this.validPostcode is not None:
                        if this.validPostcode == thisPostcode:
                            if this.isAPIpostcode:
                                this.result['score'] |= 12        # An API postcode was supplied and matched the supplied suburb
                            else:
                                this.result['score'] |= 8        # An address line postcode was supplied and matched the supplied suburb
                        else:
                            this.result['score'] |= 4            # A postcode was supplied
                    if this.street is not None:
                        this.result['score'] |= 256
                    if this.houseNo is not None:
                        this.result['score'] |= 2048
                    this.result['status'] = 'Address not found'
                    this.result['accuracy'] = '0'
                    return
                elif thisState is None:                # And it's unique within one state
                    thisState = list(postcodes[thisPostcode]['states'])[0]
            if (thisState is not None) and (thisPostcode is not None):        # Have to have state and postcode in order to find geocode data
                if scoreBuilding(this, thisState, thisPostcode):            # See if we can do better with a building name within this state or postcode
                    this.logger.debug('building found')
                    return
                # Score thisSuburb
                scoreSuburb(this, thisSuburb, thisState)        # Score this suburb
                this.result['state'] = states[thisState][0]
                this.result['score'] &= ~3
                if this.validState is not None:
                    if this.validState == thisState:
                        if this.isAPIstate:
                            this.result['score'] |= 3
                        else:
                            this.result['score'] |= 2
                    else:
                        this.result['score'] |= 1
                this.result['postcode'] = thisPostcode
                this.result['score'] &= ~12
                if this.validPostcode is not None:
                    if this.validPostcode == thisPostcode:
                        if this.isAPIpostcode:
                            this.result['score'] |= 12
                        else:
                            this.result['score'] |= 8
                    else:
                        this.result['score'] |= 4
                if this.street is not None:
                    this.result['score'] |= 256
                if this.houseNo is not None:
                    this.result['score'] |= 2048
                soundCode = jellyfish.soundex(thisSuburb)
                found = False
                # Try and find a locality (in this postcode) for this suburb in this state
                if (soundCode in suburbs) and (thisSuburb in suburbs[soundCode]) and (thisState in suburbs[soundCode][thisSuburb]):
                    for src in ['G', 'GA', 'A', 'GS', 'AS', 'GL', 'AL', 'GN']:            # Select best geocode data
                        if src in suburbs[soundCode][thisSuburb][thisState]:
                            if src in ['A', 'AS', 'AL']:
                                # Australia Post
                                if thisPostcode in suburbs[soundCode][thisSuburb][thisState][src]:
                                    SA1, LGA, latitude, longitude = suburbs[soundCode][thisSuburb][thisState][src[:1]][thisPostcode]
                                    gnafId = str(thisSuburb) + '~' + str(thisPostcode)
                                    break
                            else:
                                # For G-NAF we need a localityPid match between suburb and localityPostcodes
                                for localityPid in suburbs[soundCode][thisSuburb][thisState][src]:
                                    if localityPid in localityPostcodes:
                                        if thisPostcode in localityPostcodes[localityPid]:
                                            SA1, LGA, latitude, longitude = suburbs[soundCode][thisSuburb][thisState][src][localityPid]
                                            gnafId = 'L-' + str(localityPid)
                                            found = True
                                            break
                if not found:
                    this.result['status'] = 'Address not found'
                    this.result['accuracy'] = '0'
                    return
                this.result['G-NAF ID'] = gnafId
                this.result['SA1'] = SA1
                this.result['LGA'] = LGA
                this.result['latitude'] = latitude
                this.result['longitude'] = longitude
                this.result['status'] = 'Suburb found'
                this.result['accuracy'] = '2'
                return
            # We have no state or no postcode
            this.result['suburb'] = thisSuburb
            this.result['score'] &= ~240
            if thisState is not None:
                scoreSuburb(this, thisSuburb, thisState)
                this.result['state'] = states[thisState][0]
                this.result['score'] &= ~3
                if this.validState is not None:
                    if this.validState == thisState:
                        if this.isAPIstate:
                            this.result['score'] |= 3
                        else:
                            this.result['score'] |= 2
                    else:
                        this.result['score'] |= 1
            if thisPostcode is not None:
                this.result['postcode'] = thisPostcode
                this.result['score'] &= ~12
                return
        else:    # No valid suburbs
            this.result['score'] = 0
            this.result['state'] = ''
            if this.validState is not None:
                this.result['score'] |= 1
            this.result['score'] &= ~12
            if this.validPostcode is not None:
                this.result['score'] |= 4
            if this.street is not None:
                this.result['score'] |= 256
            if this.houseNo is not None:
                this.result['score'] |= 2048
            this.result['status'] = 'Address not found'
            this.result['accuracy'] = '0'
            return
    else:
        # We have streets within suburbs - return the first one - it's a guess
        if scoreBuilding(this, None, None):            # See if we can do better with a building name that matches one of these suburbs, with a house that has this house number
            this.logger.debug('building found')
            return
        streetPid = list(this.subsetValidStreets)[0]
        returnStreetPid(this, streetPid)
        setupAddress1Address2(this, None)
    return



# The main code
if __name__ == '__main__':
    '''
Validate an address against some known Australian concept (postcode, known Australian State, suburbs, streets)
    '''

    # Get the script name (without the '.py' extension)
    progName = os.path.basename(sys.argv[0])
    progName = progName[0:-3]        # Strip off the .py ending

    # Define the command line options
    parser = argparse.ArgumentParser(prog=progName)
    parser.add_argument('-I', '--inputDir', dest='inputDir', default='.', help='The name of the input directory')
    parser.add_argument('-O', '--outputDir', dest='outputDir', default='.', help='The name of the output directory')
    parser.add_argument('-C', '--configDir', dest='configDir', default='.',
                        help='The name of the configuration directory (default .)')
    parser.add_argument('-c', '--configFile', dest='configFile', default='verifyAddress.json',
                        help='The name of the configuration file (default verifyAddress.json)')
    parser.add_argument('-H', '--hasHeading', dest='hasHeading', action='store_true',
                        help='Files of address is CSV and has a heading line')
    parser.add_argument('-m', '--headingsMappingFile', dest='headingsMappingFile', default='headings.json',
                        help='The name of headings mapping file in the configuration directory(default headings.json)')
    parser.add_argument('-S', '--verifyAddressService', dest='verifyAddressService', action='store_true',
                        help='Run verifyAddress as a service')
    parser.add_argument('-P', '--verifyAddressPort', dest='verifyAddressPort', type=int, default=8086,
                        help='The port for the verifyAddress service (default=8086)')
    parser.add_argument('-G', '--GNAFdir', dest='GNAFdir', help='Use the standard G-NAF psv files from this folder')
    parser.add_argument('-A', '--ABSdir', dest='ABSdir', default='./ABS',
                        help='The directory where the standard ABS csv files will be found (default=./ABS)')
    parser.add_argument('-F', '--DataFilesDirectory', dest='DataDir', default='./data',
                        help='The name of the directory containing the compact files(default ./data)')
    parser.add_argument('-N', '--NTpostcodes', dest='NTpostcodes', action='store_true', help='Asuume 8xx is NT postcode 08xx')
    parser.add_argument('-R', '--region', dest='region', action='store_true',
                        help='Assume Australian region (State/Territory) if no state/territory supplied, but unique suburb found')
    parser.add_argument('-D', '--DatabaseType', dest='DatabaseType', choices=['MSSQL', 'MySQL'],
                        help='The Database Type [choices: MSSQL/MySQL]')
    parser.add_argument('-s', '--server', dest='server', help='The address of the database server')
    parser.add_argument('-u', '--username', dest='username', help='The user required to access the database')
    parser.add_argument('-p', '--password', dest='password', help='The user password required to access the database')
    parser.add_argument('-d', '--databaseName', dest='databaseName', help='The name of the database')
    parser.add_argument('-x', '--addExtras', dest='addExtras', action='store_true', help='Use additional solution specific trims')
    parser.add_argument('-W', '--configWeights', dest='configWeights', action='store_true',
                        help='Use suburb/state weights and fuzz levels from the config file')
    parser.add_argument('-a', '--abbreviate', dest='abbreviate', action='store_true', help='Output abbreviated street types')
    parser.add_argument('-b', '--returnBoth', dest='returnBoth', action='store_true', help='Output both full and abbreviated street types')
    parser.add_argument('-v', '--verbose', dest='verbose', type=int, choices=list(range(0, 5)),
                        help='The level of logging\n\t0=CRITICAL,1=ERROR,2=WARNING,3=INFO,4=DEBUG')
    parser.add_argument('-L', '--logDir', dest='logDir', default='.', help='The name of a logging directory')
    parser.add_argument('-l', '--logFile', dest='logFile', default=None, help='The name of the logging file')
    parser.add_argument('args', nargs=argparse.REMAINDER)

    # Parse the command line options
    args = parser.parse_args()
    inputDir = args.inputDir
    outputDir = args.outputDir
    configDir = args.configDir
    configFile = args.configFile
    hasHeading = args.hasHeading
    headingsMappingFile = args.headingsMappingFile
    verifyAddressService = args.verifyAddressService
    verifyAddressPort = args.verifyAddressPort
    GNAFdir = args.GNAFdir
    ABSdir = args.ABSdir
    NTpostcodes = args.NTpostcodes
    region = args.region
    DataDir = args.DataDir
    DatabaseType = args.DatabaseType
    server = args.server
    username = args.username
    password = args.password
    databaseName = args.databaseName
    addExtras = args.addExtras
    configWeights = args.configWeights
    abbreviate = args.abbreviate
    returnBoth = args.returnBoth
    logDir = args.logDir
    logFile = args.logFile
    loggingLevel = args.verbose
    if returnBoth:
        abbreviate = True

    # Check the consistency of the provided arguements
    if (GNAFdir is not None) and (DatabaseType is not None):
        sys.stderr.write('Cannot use G-NAF files and a database({DatabaseType})\n')
        parser.print_usage(sys.stderr)
        sys.stderr.flush()
        sys.exit(EX_USAGE)

    # Set up the VerifyData - just in case we are not running as a service
    verifydata = VerifyData(progName)

    # Set up logging - unless we are running as a service
    if loggingLevel and (loggingLevel not in logging_levels):
        sys.stderr.write(f'Error - invalid logging verbosity ({loggingLevel})\n')
        parser.print_usage(sys.stderr)
        sys.stderr.flush()
        sys.exit(EX_USAGE)
    verifydata.logger = logging.getLogger()
    verifydata.logger.setLevel(logging.NOTSET)
    if logFile is not None:        # If sending to a file then check if the log directory exists
        # Check that the logDir exists
        if not os.path.isdir(logDir):
            sys.stderr.write(f'Error - logDir ({logDir}) does not exits\n')
            parser.print_usage(sys.stderr)
            sys.stderr.flush()
            sys.exit(EX_USAGE)
        with open(os.path.join(logDir,logFile), 'wt', newline='', encoding='utf-8') as logfile:
            pass
        fh = logging.FileHandler(filename=os.path.join(logDir, logFile))
        fh.setFormatter(verifydata.formatter)
        if loggingLevel    :    # Change the logging level from "WARN" if the -v vebose option is specified
            fh.setLevel(logging_levels[loggingLevel])
        else:
            fh.setLevel(logging.WARNING)
        verifydata.logger.addHandler(fh)
        print(f'Now logging to {os.path.join(logDir, logFile)}')
        sys.stdout.flush()
    else:
        sh = logging.StreamHandler(sys.stderr)
        sh.setFormatter(verifydata.formatter)
        if loggingLevel    :    # Change the logging level from "WARN" if the -v vebose option is specified
            sh.setLevel(logging_levels[loggingLevel])
        else:
            sh.setLevel(logging.WARNING)
        verifydata.logger.addHandler(sh)
        print('Now logging to sys.stderr')
        sys.stdout.flush()

    # Read in the configuration file - which must exist if required
    configRequired = False
    if (DatabaseType is not None) or configWeights:
        configRequired = True

    config = {}                 # The configuration data
    if configRequired:
        configfile = os.path.join(configDir, configFile)
        try:
            with open(configfile, 'rt', newline='', encoding='utf-8') as configfile:
                config = json.load(configfile, object_pairs_hook=collections.OrderedDict)
        except IOError:
            verifydata.logger.critical('configFile (%s/%s) failed to load', configDir, configFile)
            logging.shutdown()
            sys.exit(EX_CONFIG)

        # Check that we have a databaseName if we have a databaseType
        if DatabaseType is not None:
            if DatabaseType not in config:
                verifydata.logger.critical('DatabaseType(%s) not found in configuraton file(%s)', DatabaseType, configfile)
                logging.shutdown()
                sys.exit(EX_USAGE)
            if 'connectionString' not in config[DatabaseType]:
                logging.critical('No %s connectionString defined in configuration file(SQLAlchemyDB.json)', DatabaseType)
                logging.shutdown()
                sys.exit(EX_CONFIG)
            connectionString = config[DatabaseType]['connectionString']
            if ('username' in config[DatabaseType]) and (username is None):
                username = config[DatabaseType]['username']
            if ('password' in config[DatabaseType]) and (password is None):
                password = config[DatabaseType]['password']
            if ('server' in config[DatabaseType]) and (server is None):
                server = config[DatabaseType]['server']
            if ('databaseName' in config[DatabaseType]) and (databaseName is None):
                databaseName = config[DatabaseType]['databaseName']

            # Check that we have all the required paramaters
            if username is None:
                verifydata.logging.critical('Missing definition for "username"')
                logging.shutdown()
                sys.exit(EX_USAGE)
            if password is None:
                verifydata.logging.critical('Missing definition for "password"')
                logging.shutdown()
                sys.exit(EX_USAGE)
            if server is None:
                verifydata.logging.critical('Missing definition for "server"')
                logging.shutdown()
                sys.exit(EX_USAGE)
            if databaseName is None:
                verifydata.logging.critical('Missing definition for "databaseName"')
                logging.shutdown()
                sys.exit(EX_USAGE)
            connectionString = connectionString.format(username=username, password=password, server=server, databaseName=databaseName)

            # Create the engine
            if DatabaseType == 'MSSQL':
                engine = create_engine(connectionString, use_setinputsizes=False, echo=True)
            else:
                engine = create_engine(connectionString, echo=True)

            # Check if the database exists
            if not database_exists(engine.url):
                verifydata.logging.critical('Database %s does not exist', databaseName)
                logging.shutdown()
                sys.exit(EX_CONFIG)

            # Connect to the database
            try:
                conn = engine.connect()
            except OperationalError:
                verifydata.logging.critical('Connection error for database %s', databaseName)
                logging.shutdown()
                sys.exit(EX_UNAVAILABLE)
            except Exception as e:
                verifydata.logging.critical('Connection error for database %s', databaseName)
                logging.shutdown()
                sys.exit(EX_UNAVAILABLE)
            conn.close()
            Session = sessionmaker(bind=engine)
            # logging.getLogger('sqlalchemy.engine').setLevel(logging.DEBUG)

        if configWeights:
            if 'weights' not in config:
                verifydata.logger.critical('weights not found in configuraton file(%s)', configfile)
                logging.shutdown()
                sys.exit(EX_USAGE)
            if 'suburbSourceWeight' in config['weights']:
                suburbSourceWeight = config['weights']['suburbSourceWeight']
                if not isinstance(suburbSourceWeight, dict):
                    verifydata.logger.critical('suburbSourceWeight not a dictionary in configuraton file(%s)', configfile)
                    logging.shutdown()
                    sys.exit(EX_USAGE)
                for source in suburbSourceWeight:
                    if source not in suburbSourceWeight:
                        if source not in ['G', 'GA', 'GN', 'GS', 'GL', 'GAS', 'GAL', '']:
                            del suburbSourceWeight[source]
            if 'streetSourceWeight' in config['weights']:
                streetSourceWeight = config['weights']['streetSourceWeight']
                if not isinstance(streetSourceWeight, dict):
                    verifydata.logger.critical('streetSourceWeight not a dictionary in configuraton file(%s)', configfile)
                    logging.shutdown()
                    sys.exit(EX_USAGE)
                for source in streetSourceWeight:
                    if source not in streetSourceWeight:
                        if source not in ['G', 'GA', 'GS', 'GL', 'GAS', 'GAL', '']:
                            del streetSourceWeight[source]
            if 'fuzzLevels' in config['weights']:
                fuzzLevels = config['weights']['fuzzLevels']
                if not isinstance(fuzzLevels, list):
                    verifydata.logger.critical('fuzzLevels not a list in configuraton file(%s)', configfile)
                    logging.shutdown()
                    sys.exit(EX_USAGE)
                for fuzz in range(len(fuzzLevels) - 1, -1, -1):
                    if not isinstance(fuzzLevels[fuzz], int):
                        verifydata.logger.warning(' Invalid fuzzLevel (%s) in configuraton file(%s) - ignoring', fuzz, configfile)
                        del fuzzLevels[fuzz]
                        continue
                    if (fuzzLevels[fuzz] < 1) or (fuzzLevels[fuzz][0] > 10):
                        verifydata.logger.warning(' Invalid fuzzLevel (%d) in configuraton file(%s) - ignoring', fuzz, configfile)
                        del fuzzLevels[fuzz]


    # Check if files have headings and if so read in the mapping
    fileHas = {}
    if hasHeading:
        mappingfile = os.path.join(inputDir, headingsMappingFile)
        try:
            with open(mappingfile, 'rt', newline='', encoding='utf-8') as mapfile:
                fileHas = json.load(mapfile, object_pairs_hook=collections.OrderedDict)
        except IOError:
            verifydata.logger.critical('headingsMappingFile (%s/%s) failed to load', inputDir, headingsMappingFile)
            logging.shutdown()
            sys.exit(EX_CONFIG)
        if 'addressLines' not in fileHas:
            logging.fatal('Headings mapping file(%s) does not define column(s) for addressLines', os.path.join(inputDir, headingsMappingFile))
            sys.exit(EX_CONFIG)

    # Check if we are going to need inputDir and outputDir
    isStdIO = True
    for fileName in args.args:
        if fileName != '-':
            isStdIO = False
    if not isStdIO:
        # Check that the inputDir exists
        if not os.path.isdir(inputDir):
            verifydata.logger.critical('Usage error - inputDir (%s) does not exits', inputDir)
            logging.shutdown()
            sys.exit(EX_USAGE)
        # Check that the outputDir exists
        if not os.path.isdir(outputDir):
            verifydata.logger.critical('Usage error - outputDir (%s) does not exits', outputDir)
            logging.shutdown()
            sys.exit(EX_USAGE)

    # Read in the G-NAF data and build the data structures for verifying addresses
    initData(verifydata)

    # Now process every input arguement
    if verifyAddressService:
        print('Starting verifyAddress Service', file=sys.stdout)
        sys.stdout.flush()
        try:
            httpd = ThreadedHTTPServer(('', verifyAddressPort), verifyAddressHandler)
# fork() NOT AVAILABLE ON WINDOWS
#             httpd = ForkingHTTPServer(('', autocodePort), autocodingHandler)
            print ('Started httpserver on port', verifyAddressPort, file=sys.stdout)
            sys.stdout.flush()
            httpd.serve_forever()
        except KeyboardInterrupt:
            print ('Stopped httpserver on port', verifyAddressPort, file=sys.stdout)
            sys.stdout.flush()
            for hdlr in verifydata.logger.handlers:
                hdlr.flush()

        # Wrap it up
        logging.shutdown()
        sys.stderr.flush()
        sys.exit(EX_OK)

    elif len(args.args) == 0:
        # Read addresses from standard input
        lines = sys.stdin.readlines()
        for line in lines:
            line = line.strip()
            verifydata.Address = {'addressLines': [line]}
            verifyAddress(verifydata)
            print('Original text:', line, file=sys.stdout)
            print('Structured address:', file=sys.stdout)
            print('Address line 1:', verifydata.result['addressLine1'], file=sys.stdout)
            print('Address line 2:', verifydata.result['addressLine2'], file=sys.stdout)
            print('Suburb:', verifydata.result['suburb'], file=sys.stdout)
            print('Postcode:', verifydata.result['postcode'], file=sys.stdout)
            print('State:', verifydata.result['state'], file=sys.stdout)
            print('SA1:', verifydata.result['SA1'], file=sys.stdout)
            print('LGA:', verifydata.result['LGA'], file=sys.stdout)
            print('Mesh Block:', verifydata.result['Mesh Block'], file=sys.stdout)
            print('Longitude:', verifydata.result['longitude'], file=sys.stdout)
            print('Latitude:', verifydata.result['latitude'], file=sys.stdout)
            print('G-NAF ID:', verifydata.result['G-NAF ID'], file=sys.stdout)
            print('Accuracy:', verifydata.result['accuracy'], file=sys.stdout)
            print('Fuzz Level:', verifydata.result['fuzzLevel'], file=sys.stdout)
            print('Score:', verifydata.result['score'], file=sys.stdout)
            print('Status:', verifydata.result['status'], file=sys.stdout)
            if 'messages' in verifydata.result['messages']:
                for i in range(len(verifydata.result['messages'])):
                    print('Message:', verifydata.result['messages'][i], file=sys.stdout)

    else:   # Process one or more file. Each file must contain one address per line
        for fileName in args.args:
            # Check for stdin
            inDialect = csv.excel
            if fileName == '-':
                fpIn = sys.stdin
                fpOut = sys.stdout
            else:       # Check if the input file exists
                if not os.path.isfile(os.path.join(inputDir, fileName)):
                    verifydata.logger.critical('Usage error - input file (%s) is missing', os.path.join(inputDir, fileName))
                    logging.shutdown()
                    sys.exit(EX_USAGE)

                # Open the input, output and logging files
                try:
                    fpIn = open(os.path.join(inputDir, fileName), 'rt', newline='', encoding='utf-8')
                except IOError:
                    verifydata.logger.critical('Usage error - input file (%s) cannot be read', os.path.join(inputDir, fileName))
                    logging.shutdown()
                    sys.exit(EX_USAGE)

                # If CSV then sniff the CSV file structure
                if hasHeading:
                    inDialect = csv.Sniffer().sniff(fpIn.read(4096))
                    fpIn.seek(0)

                # Give the output file the same name as the input file - unless they are in the same directory
                outFileName = 'verifyAddress_' + os.path.basename(fileName)

                # Try creating the output file
                try:
                    fpOut = open(os.path.join(outputDir, outFileName), 'wt', newline='', encoding='utf-8')
                except IOError:
                    verifydata.logger.critical('Usage error - cannot create output file (%s)', os.path.join(outputDir, outFileName))
                    # Close the input file and try the next argument
                    fpIn.close()
                    logging.shutdown()
                    sys.exit(EX_USAGE)
                if hasHeading:
                    outDialect = csv.excel
                    outDialect.delimiter = str(',')
                    outWriter = csv.writer(fpOut, outDialect)

                # Now try swapping the logging over
                # Craft the log filename from the root of the output filename, but with '.log' as the extension
                (logRoot, outExt) = os.path.splitext(outFileName)
                logFileName = logRoot + '.log'

                # Check for a name clash - input file ends in '.log' !!!!
                if (logDir == outputDir) and (outFileName == logFileName):
                    logFileName = 'autocoded_log_' + logFileName
                elif os.path.abspath(os.path.join(inputDir, fileName)) == os.path.abspath(os.path.join(logDir, logFileName)):
                    logFileName = 'autocoded_log_' + logFileName

                # Start by closing all the existing handlers
                fhFound = False
                for hdlr in verifydata.logger.handlers:
                    hdlr.close()
                    if hdlr == fh:
                        fhFound = True
                if fhFound:
                    verifydata.logger.removeHandler(fh)

                # Now set up logging again
                with open(os.path.join(logDir,logFileName), 'wt', newline='', encoding='utf-8') as logfile:
                    pass
                fh = logging.FileHandler(filename=os.path.join(logDir, logFileName))
                fh.setFormatter(verifydata.formatter)
                if loggingLevel    :    # Change the logging level from "WARN" if the -v vebose option is specified
                    fh.setLevel(logging_levels[loggingLevel])
                else:
                    fh.setLevel(logging.WARNING)
                verifydata.logger.addHandler(fh)
                verifydata.logger.debug('csv dialect: delimiter(%s), doublequote(%s), escapechar(%s), lineterminator(%s), quotechar(%s), quoting(%s), skipinitialspace(%s)',
                                        inDialect.delimiter, inDialect.doublequote, inDialect.escapechar, inDialect.lineterminator, inDialect.quotechar, inDialect.quoting, inDialect.skipinitialspace)

            # Now check each line in the file - every line must be an address
            lines = fpIn.readlines()
            header = True
            inFileHas = {}
            count = 0
            if returnBoth:
                headingParts = ['isPostalService', 'Building Name', 'House No.', 'Street', 'AddressLine1', 'AddressLine2', 'AddressLine1Abbrev', 'AddressLine2Abbrev', 'Suburb', 'State', 'Postcode', 'SA1', 'LGA', 'Mesh Block', 'Longitude', 'Latitude', 'G-NAF ID', 'Accuracy', 'Fuzz Level', 'Score', 'Status', 'Message', 'Changed']
                addressParts = ['isPostalService', 'buildingName', 'houseNo', 'street', 'addressLine1', 'addressLine2', 'addressLine1Abbrev', 'addressLine2Abbrev', 'suburb', 'state', 'postcode', 'SA1', 'LGA', 'Mesh Block', 'latitude', 'longitude', 'G-NAF ID', 'accuracy', 'fuzzLevel', 'score', 'status', 'messages']
            else:
                headingParts = ['isPostalService', 'Building Name', 'House No.', 'Street', 'AddressLine1', 'AddressLine2', 'Suburb', 'State', 'Postcode', 'SA1', 'LGA', 'Mesh Block', 'Longitude', 'Latitude', 'G-NAF ID', 'Accuracy', 'Fuzz Level', 'Score', 'Status', 'Message', 'Changed']
                addressParts = ['isPostalService', 'buildingName', 'houseNo', 'street', 'addressLine1', 'addressLine2', 'suburb', 'state', 'postcode', 'SA1', 'LGA', 'Mesh Block', 'latitude', 'longitude', 'G-NAF ID', 'accuracy', 'fuzzLevel', 'score', 'status', 'messages']
            for line in lines:
                line = line.strip()
                if hasHeading:
                    # file must be a CSV file
                    row = list(csv.reader([line], inDialect))[0]
                    verifydata.logger.debug('csv line(%s)', repr(row))

                    # Check for end of file
                    if (row[0] == 'End of File') and (len(row) == 2):
                        outRow = []
                        outRow.append('End of File')
                        outRow.append(count)
                        outWriter.writerow(outRow)
                        break

                    if header:
                        columns = 0
                        for i, col in enumerate(row):
                            columns += 1
                            if col in inFileHas:
                                if col in fileHas:
                                    logging.fatal('Ambiguous column heading. Require column(%s) defined more than once', col)
                                    sys.exit(EX_CONFIG)
                                else:
                                    logging.warning('Ambiguous column heading. Column(%s) defined more than once', col)
                            inFileHas[col] = i
                        verifydata.logger.debug('header line(%s)', repr(inFileHas))
                        verifydata.logger.debug('mapping line(%s)', repr(fileHas))
                        for addressPart in fileHas:
                            if addressPart == '/* comment */':
                                continue
                            if isinstance(fileHas[addressPart], list):
                                for i in range(len(fileHas[addressPart])):
                                    if fileHas[addressPart][i] not in inFileHas:
                                        logging.critical('Input file (%s) is missing column(%s)', os.path.join(inputDir, fileName), fileHas[addressPart][i])
                                        sys.exit(EX_CONFIG)
                            else:
                                if fileHas[addressPart] not in inFileHas:
                                    logging.critical('Input file (%s) is missing column(%s)', os.path.join(inputDir, fileName), fileHas[addressPart])
                                    sys.exit(EX_CONFIG)
                        outRow = row[:]
                        for addressPart in headingParts:
                            outRow.append(addressPart)
                        outWriter.writerow(outRow)
                        header = False
                        continue
                    else:
                        # Process a data row
                        if len(row) != columns:
                            logging.critical('Input record has wrong number of columns - line columns(%d), heading columns(%d)\n%s\n%s',
                                             len(row), columns, str(line), repr(row))
                            continue

                        outRow = row[:]
                        verifydata.Address = {}
                        for addressPart in fileHas:
                            if addressPart == '/* comment */':
                                continue
                            if isinstance(fileHas[addressPart], list):
                                verifydata.Address[addressPart] = []
                                for i in range(len(fileHas[addressPart])):
                                    verifydata.logger.debug('saving (%s) as (%s)', repr(row[inFileHas[fileHas[addressPart][i]]]), repr(addressPart))
                                    verifydata.Address[addressPart].append(row[inFileHas[fileHas[addressPart][i]]])
                            else:
                                verifydata.logger.debug('saving (%s) as (%s)', repr(row[inFileHas[fileHas[addressPart]]]), repr(addressPart))
                                verifydata.Address[addressPart] = row[inFileHas[fileHas[addressPart]]]
                else:
                    # A line from a file with no headings
                    verifydata.Address = {'addressLines': [line]}

                verifyAddress(verifydata)

                if hasHeading:
                    # Save the returned address
                    for addressPart in addressParts:
                        if addressPart in verifydata.result:
                            if isinstance(verifydata.result[addressPart], str) and (verifydata.result[addressPart] != '') and (verifydata.result[addressPart][-1] == ','):
                                verifydata.result[addressPart] = verifydata.result[addressPart][:-1]
                                outRow.append(verifydata.result[addressPart])
                            elif isinstance(verifydata.result[addressPart], list):
                                first = True
                                part = ''
                                for i in range(len(verifydata.result[addressPart])):
                                    if first:
                                        first = False
                                    else:
                                        part += ', '
                                    part += verifydata.result[addressPart][i]
                                outRow.append(part)
                            else:
                                outRow.append(verifydata.result[addressPart])
                        else:
                            outRow.append('')
                    # Now check the address
                    changed = ''
                    for addressPart in fileHas:
                        if addressPart == '/* comment */':
                            continue
                        if isinstance(fileHas[addressPart], list):
                            if (len(fileHas[addressPart]) > 0) and (row[inFileHas[fileHas[addressPart][0]]] != verifydata.result['addressLine1']):
                                if changed != '':
                                    changed += ', '
                                changed += 'addressLine1'
                            if (len(fileHas[addressPart]) > 1) and (row[inFileHas[fileHas[addressPart][1]]] != verifydata.result['addressLine2']):
                                if changed != '':
                                    changed += ', '
                                changed += 'addressLine2'
                        elif row[inFileHas[fileHas[addressPart]]] != verifydata.result[addressPart]:
                            if changed != '':
                                changed += ', '
                            changed += addressPart
                    outRow.append(changed)
                    outWriter.writerow(outRow)
                    count += 1
                else:
                    print('Original text:', line, file=fpOut)
                    print('Structured address:', file=fpOut)
                    print('Postal Delivery Service address:', verifydata.result['isPostalService'], file=fpOut)
                    print('House No.:', verifydata.result['houseNo'], file=fpOut)
                    if (verifydata.result['isPostalService']) and (verifydata.result['buildingName'] != ''):
                        print('Building Name:', verifydata.result['buildingName'], file=fpOut)
                    print('Street:', verifydata.result['street'], file=fpOut)
                    if (verifydata.result['addressLine1'] != '') and (verifydata.result['addressLine1'][-1] == ','):
                        verifydata.result['addressLine1'] = verifydata.result['addressLine1'][:-1]
                    print('Address line 1:', verifydata.result['addressLine1'], file=fpOut)
                    if (verifydata.result['addressLine2'] != '') and (verifydata.result['addressLine2'][-1] == ','):
                        verifydata.result['addressLine2'] = verifydata.result['addressLine2'][:-1]
                    print('Address line 2:', verifydata.result['addressLine2'], file=fpOut)
                    if returnBoth:
                        if (verifydata.result['addressLine1Abbrev'] != '') and (verifydata.result['addressLine1Abbrev'][-1] == ','):
                            verifydata.result['addressLine1Abbrev'] = verifydata.result['addressLine1Abbrev'][:-1]
                        print('Abbreviated Address line 1:', verifydata.result['addressLine1Abbrev'], file=fpOut)
                        if (verifydata.result['addressLine2Abbrev'] != '') and (verifydata.result['addressLine2Abbrev'][-1] == ','):
                            verifydata.result['addressLine2Abbrev'] = verifydata.result['addressLine2Abbrev'][:-1]
                        print('Abbreviated Address line 2:', verifydata.result['addressLine2Abbrev'], file=fpOut)
                    if (not verifydata.result['isPostalService']) and (verifydata.result['buildingName'] != ''):
                        print('Building Name:', verifydata.result['buildingName'], file=fpOut)
                    print('Suburb:', verifydata.result['suburb'], file=fpOut)
                    print('Postcode:', verifydata.result['postcode'], file=fpOut)
                    print('State:', verifydata.result['state'], file=fpOut)
                    print('SA1:', verifydata.result['SA1'], file=fpOut)
                    print('LGA:', verifydata.result['LGA'], file=fpOut)
                    print('Mesh Block:', verifydata.result['Mesh Block'], file=fpOut)
                    print('Latitude:', verifydata.result['latitude'], file=fpOut)
                    print('Longitude:', verifydata.result['longitude'], file=fpOut)
                    print('G-NAF ID:', verifydata.result['G-NAF ID'], file=fpOut)
                    print('Accuracy:', verifydata.result['accuracy'], file=fpOut)
                    print('Fuzz Level:', verifydata.result['fuzzLevel'], file=fpOut)
                    print('Score:', verifydata.result['score'], file=fpOut)
                    print('Status:', verifydata.result['status'], file=fpOut)
                    if ('messages' in verifydata.result) and (len(verifydata.result['messages']) > 0):
                        for i in range(len(verifydata.result['messages'])):
                            print('Message:', verifydata.result['messages'][i], file=fpOut)
                    print(file=fpOut)

            # And close the files
            if fileName != '-':
                fpIn.close()
                fpOut.close()

    # Wrap it up
    logging.shutdown()
    sys.stdout.flush()
    sys.stderr.flush()
    sys.exit(EX_OK)
