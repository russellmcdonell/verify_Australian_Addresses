#!/usr/bin/env python

'''
A script to verify addresses by sening them to the verifyAddress service.
The address are read from a CSV file(s) which must have a heading row.
The headings for those columns must be defined in the sendAddress.json file.

Results will be written to 'sendAddress_' + filename and will consist of the address columns from the input file
plus additional columns, being the result returned from the verifyAddress service.


SYNOPSIS
$ python sendAddress.py [-I inputDir|--inputDir=inputDir] [-O outputDir|--outputDir=outputDir]
                         [-S verifyAddressServer|--verifyAddressServer=verifyAddressServer]
                         [-P verifyAddressPort|--verifyAddressPort=verifyAddressPort]
                         [-U verifyAddressURL|--verifyAddressURL=verifyAddressURL]
                         [-v loggingLevel|--verbose=logingLevel] [-L logDir|--logDir=logDir] [-l logfile|--logfile=logfile]
                         filename

REQUIRED


OPTIONS
-I inputDir|--inputDir=inputDir
The directory where the CSV file(s) will be found (default='input')
sendAddress.json will be read from this directory (default='output').

-O outputDir|--outputDir=outputDir
The directory where the output file will be written.

-S|--verifyAddressServer
The verifyAddress server name (default=localhost)

-P verifyAddressPort|--verifyAddressPort=verifyAddressPort
The port for the verifyAddress service (default=8086)

-U verifyAddressURL|--verifyAddressURL=verifyAddressURL
The URL on the verifyAddress server for the verifyAddress service

-v loggingLevel|--verbose=loggingLevel
Set the level of logging that you want (defaut INFO).

-L logDir
The directory where the log file will be written (default='logs')

-l logfile|--logfile=logfile
The name of a logging file where you want all log messages written (default=None)

'''

# Import all the modules that make life easy
import sys
import os
import io
import argparse
import logging
import json
import collections
import csv
from http import client
from urllib.parse import urlencode, parse_qs


# This next section is plagurised from /usr/include/sysexits.h
EX_OK = 0        # successful termination
EX_WARN = 1        # non-fatal termination with warnings

EX_USAGE = 64        # command line usage error
EX_DATAERR = 65        # data format error
EX_NOINPUT = 66        # cannot open input
EX_NOUSER = 67        # addressee unknown
EX_NOHOST = 68        # host name unknown
EX_UNAVAILABLE = 69    # service unavailable
EX_SOFTWARE = 70    # internal software error
EX_OSERR = 71        # system error (e.g., can't fork)
EX_OSFILE = 72        # critical OS file missing
EX_CANTCREAT = 73    # can't create (user) output file
EX_IOERR = 74        # input/output error
EX_TEMPFAIL = 75    # temp failure; user is invited to retry
EX_PROTOCOL = 76    # remote error in protocol
EX_NOPERM = 77        # permission denied
EX_CONFIG = 78        # configuration error


# The command line arguments and their related globals
inputDir = 'input'                # The directory where the input files will be found
outputDir = 'output'                # The directory where the output files will be written
verifyAddressServer = 'localhost'              # The verifyAddress server
verifyAddressPort = 8086                # The verifyAddress service port
veryfiAddressURL = '/'            # The URL on the verifyAddress server for sending reports to
logDir = 'logs'                # The directory where the log files will be written
logging_levels = {0:logging.CRITICAL, 1:logging.ERROR, 2:logging.WARNING, 3:logging.INFO, 4:logging.DEBUG}
loggingLevel = logging.NOTSET        # The default logging level
logFile = None                # The name of the logfile (output to stderr if None)


# The main code
if __name__ == '__main__':
    '''
Query addresses to see if they are valid Australian addresses.
Addresses are read from CSV file(s)
    '''

    # Get the script name (without the '.py' extension)
    progName = os.path.basename(sys.argv[0])
    progName = progName[0:-3]        # Strip off the .py ending

    # Define the command line options
    parser = argparse.ArgumentParser(prog=progName)
    parser.add_argument('-I', '--inputDir', dest='inputDir', default='.',
                        help='The name of the input directory. sendAddress.json will be read from this directory')
    parser.add_argument('-O', '--outputDir', dest='outputDir', default='.', help='The name of the output directory')
    parser.add_argument('-S', '--verifyAddressServer', dest='verifyAddressServer', default='localhost',
                        help='The verifyAddress server')
    parser.add_argument('-P', '--verifyAddressPort', dest='verifyAddressPort', type=int, default=8086,
                        help='The port for the verifyAddress service (default 8086')
    parser.add_argument ('-U', '--verifyAddressURL', dest='verifyAddressURL', default='/', help='The verifyAddress service URL')
    parser.add_argument('-v', '--verbose', dest='verbose', type=int, choices=range(0, 5),
                        help='The level of logging\n\t0=CRITICAL,1=ERROR,2=WARNING,3=INFO,4=DEBUG')
    parser.add_argument('-L', '--logDir', dest='logDir', default='.', help='The name of a logging directory')
    parser.add_argument('-l', '--logFile', dest='logFile', default=None, help='The name of the logging file')
    parser.add_argument('args', nargs=argparse.REMAINDER)

    # Parse the command line options
    args = parser.parse_args()
    inputDir = args.inputDir
    outputDir = args.outputDir
    verifyAddressServer = args.verifyAddressServer
    verifyAddressPort = args.verifyAddressPort
    verifyAddressURL = args.verifyAddressURL
    loggingLevel = args.verbose
    logDir = args.logDir
    logFile = args.logFile

    # Check if we have one input file to process
    if len(args.args) == 0:
        sys.stderr.write('No filename specified!')
        parser.print_usage(sys.stderr)
        sys.stderr.flush()
        sys.exit(EX_USAGE)
    if len(args.args) > 1:
        sys.stderr.write('Too many filenames specified!')
        parser.print_usage(sys.stderr)
        sys.stderr.flush()
        sys.exit(EX_USAGE)
    fileName = args.args[0]

    # Set up logging
    logging_levels = {0:logging.CRITICAL, 1:logging.ERROR, 2:logging.WARNING, 3:logging.INFO, 4:logging.DEBUG}
    logfmt = progName + ' [%(asctime)s]: %(message)s'
    if loggingLevel and (loggingLevel not in logging_levels) :
        sys.stderr.write('Error - invalid logging verbosity (%d)\n' % (loggingLevel))
        parser.print_usage(sys.stderr)
        sys.stderr.flush()
        sys.exit(EX_USAGE)
    if logFile :        # If sending to a file then check if the log directory exists
        # Check that the logDir exists
        if not os.path.isdir(logDir) :
            sys.stderr.write('Error - logDir (%s) does not exits\n' % (logDir))
            parser.print_usage(sys.stderr)
            sys.stderr.flush()
            sys.exit(EX_USAGE)
        with open(os.path.join(logDir,logFile), 'w') as logfile :
            pass
        if loggingLevel :
            logging.basicConfig(format=logfmt, datefmt='%d/%m/%y %H:%M:%S %p', level=logging_levels[loggingLevel], filename=os.path.join(logDir, logFile))
        else :
            logging.basicConfig(format=logfmt, datefmt='%d/%m/%y %H:%M:%S %p', filename=os.path.join(logDir, logFile))
        print('Now logging to %s' % (os.path.join(logDir, logFile)))
        sys.stdout.flush()
    else :
        if loggingLevel :
            logging.basicConfig(format=logfmt, datefmt='%d/%m/%y %H:%M:%S %p', level=logging_levels[loggingLevel])
        else :
            logging.basicConfig(format=logfmt, datefmt='%d/%m/%y %H:%M:%S %p')
        print('Now logging to sys.stderr')
        sys.stdout.flush()

    # Check that the inputDir exists
    if not os.path.isdir(inputDir) :
        logging.critical('Input directory(%s) does not exist!', inputDir)
        sys.exit(EX_CONFIG)

    # Check that the input file exists
    if not os.path.isfile(os.path.join(inputDir, fileName)):
        logging.critical('Usage error - input file (%s) is missing', os.path.join(inputDir, fileName))
        logging.shutdown()
        sys.exit(EX_USAGE)

    # Check that the configuration file exists
    if not os.path.isfile(os.path.join(inputDir, 'sendAddress.json')):
        logging.critical('Configuration file(%s) does not exist!', os.path.join(inputDir, 'sendAddress.json'))
        sys.exit(EX_CONFIG)

    # Read in the configuration file - which must exist
    csvHas = {}
    configfile = os.path.join(inputDir, 'sendAddress.json')
    try:
        with open(configfile, 'r', encoding='utf-8', newline='') as configfile:
            csvHas = json.load(configfile, object_pairs_hook=collections.OrderedDict)
    except(IOError):
        this.logger.critical('configFile (%s/%s) failed to load', inputDir, 'sendAddress.json')
        logging.shutdown()
        sys.exit(EX_CONFIG)
    if 'addressLines' not in csvHas:
        logging.fatal('Config file(%s) does not define column(s) for addressLines', os.path.join(inputDir, 'sendAddress.json'))
        sys.exit(EX_CONFIG)

    # Make sure we have connectivity to verifyAddress service
    verifyAddressHeaders = {'Content-type':'application/json', 'Accept':'application/JSON'}
    try :
        verifyAddressConnection = client.HTTPConnection(verifyAddressServer, verifyAddressPort)
        verifyAddressConnection.close()
    except (client.NotConnected, client.InvalidURL, client.UnknownProtocol, client.UnknownTransferEncoding,
            client.UnimplementedFileMode, client.IncompleteRead, client.ImproperConnectionState,
            client.CannotSendRequest, client.CannotSendHeader, client.ResponseNotReady, client.BadStatusLine) as e:
        logging.critical('Cannot connect to the verifyAddress Service on host (%s) and port (%d). Error:%s', verifyAddressServer, verifyAddressPort, str(e))
        logging.shutdown()
        sys.stdout.flush()
        sys.exit(EX_UNAVAILABLE)
    logging.info('Connected to %s:%d', verifyAddressServer, verifyAddressPort)

    # Open the input, output and logging files
    try:
        fpIn = open(os.path.join(inputDir, fileName))
    except(IOError):
        logging.critical('Usage error - input file (%s) cannot be read', os.path.join(inputDir, fileName))
        logging.shutdown()
        sys.exit(EX_USAGE)
    inDialect = csv.Sniffer().sniff(fpIn.read(2048))
    fpIn.seek(0)
    inReader = csv.reader(fpIn, inDialect)

    # Try creating the output file
    outFileName = 'sendAddress_' + fileName
    try:
        fpOut = open(os.path.join(outputDir, outFileName), 'wt', encoding='utf-8', newline='')
    except(IOError):
        logging.critical('Usage error - cannot create output file (%s)', os.path.join(outputDir, outFileName))
        # Close the input file and try the next argument
        fpIn.close()
        logging.shutdown()
        sys.exit(EX_USAGE)
    outDialect = csv.excel
    outWriter = csv.writer(fpOut, outDialect)

    # Now process the input file`
    # verify each line in the file
    header = True
    inFileHas = {}
    count = 0
    addressHeadings = ['isPostal', 'isCommunity', 'AddressLine1', 'AddressLine2', 'Suburb', 'State', 'Postcode', 'SA1', 'LGA', 'Longitude', 'Latitude', 'FuzzLevel', 'Score', 'Status', 'Message','Changed']
    addressParts = ['isPostal', 'isCommunity', 'addressLine1', 'addressLine2', 'suburb', 'state', 'postcode', 'SA1', 'LGA', 'longitude', 'latitude', 'fuzzLevel', 'score', 'status', 'message']
    for row in inReader:
        if header:
            for i, heading in enumerate(row):
                inFileHas[heading] = i
            outRow = []
            for addressPart in csvHas:
                if addressPart == '/* comment */':
                    continue
                if isinstance(csvHas[addressPart], list):
                    for i in range(len(csvHas[addressPart])):
                        if csvHas[addressPart][i] not in inFileHas:
                            logging.critical('Input file (%s) is missing column(%s)', os.path.join(inputDir, fileName), csvHas[addressPart][i])
                            fpIn.close()
                            logging.shutdown()
                            sys.exit(EX_CONFIG)
                        else:
                            outRow.append(row[inFileHas[csvHas[addressPart][i]]])
                else:
                    if csvHas[addressPart] not in inFileHas:
                        logging.critical('Input file (%s) is missing column(%s)', os.path.join(inputDir, fileName), csvHas[addressPart])
                        fpIn.close()
                        logging.shutdown()
                        sys.exit(EX_CONFIG)
                    else:
                        outRow.append(row[inFileHas[csvHas[addressPart]]])
            for addressPart in addressHeadings:
                outRow.append(addressPart)
            outWriter.writerow(outRow)
            header = False
            continue
        
        # Check for end of file
        if (row[0] == 'End of File') and (len(row) == 2):
            outRow = []
            outRow.append('End of File')
            outRow.append(count)
            outWriter.writerow(outRow)
            break

        Address = {}
        outRow = []
        for addressPart in csvHas:
            if addressPart == '/* comment */':
                continue
            if isinstance(csvHas[addressPart], list):
                Address[str(addressPart)] = []
                for i in range(len(csvHas[addressPart])):
                    outRow.append(row[inFileHas[csvHas[addressPart][i]]])
                    Address[str(addressPart)].append(row[inFileHas[csvHas[addressPart][i]]])
            else:
                outRow.append(row[inFileHas[csvHas[addressPart]]])
                Address[str(addressPart)] = row[inFileHas[csvHas[addressPart]]]

        # Verify this address - send Address as the query string
        params = json.dumps(Address)
        try :
            verifyAddressConnection = client.HTTPConnection(verifyAddressServer, verifyAddressPort)
            verifyAddressConnection.request('POST', verifyAddressURL, params, verifyAddressHeaders)
            response = verifyAddressConnection.getresponse()
            if response.status != 200 :
                logging.critical('Invalid response from verifyAddress Service:error %s', response.status)
                fpIn.close()
                logging.shutdown()
                sys.exit(EX_SOFTWARE)
            responseData = response.read()
            verifyAddressConnection.close()
        except (client.NotConnected, client.InvalidURL, client.UnknownProtocol, client.UnknownTransferEncoding,
                client.UnimplementedFileMode, client.IncompleteRead, client.ImproperConnectionState,
                client.CannotSendRequest, client.CannotSendHeader, client.ResponseNotReady, client.BadStatusLine) as e:
            logging.critical('verifyAddress Service error:(%s)', repr(e))
            fpIn.close()
            logging.shutdown()
            sys.exit(EX_SOFTWARE)

        # Get back the results structure
        try :
            result = json.loads(responseData)
        except ValueError as e :
            logging.critical('Invalid data from verifyAddress Service:error(%s)', repr(e))
            fpIn.close()
            logging.shutdown()
            sys.exit(EX_SOFTWARE)

        # urllib.parse_qs returns everything as list, even if only one value was sent
        for addressPart in addressParts:
            if addressPart in result:
                outRow.append(result[addressPart])
            else:
                outRow.append('')
        # Now check the address
        changed = ''
        for part in Address:
            if isinstance(Address[part], list):
                if (len(Address[part]) > 0) and (Address[part] != result['addressLine1']):
                    if changed != '':
                        changed += ', '
                    changed += 'addressLine1'
                if (len(Address[part]) > 1) and (Address[part] != result['addressLine2']):
                    if changed != '':
                        changed += ', '
                    changed += 'addressLine2'
            elif Address[part] != result[part]:
                if changed != '':
                    changed += ', '
                changed += part
        outRow.append(changed)
        outWriter.writerow(outRow)
        count += 1

    # And close the input and ouput files
    fpIn.close()
    fpOut.close()

    # Wrap it up
    logging.shutdown()
    sys.stdout.flush()
    sys.stderr.flush()
    sys.exit(EX_OK)
