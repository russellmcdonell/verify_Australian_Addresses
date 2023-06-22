#!/usr/bin/env python

# pylint: disable=line-too-long, invalid-name, pointless-string-statement

'''
Script to extract a subset of columns from a csv file


SYNOPSIS
$ python csvExtract.py
         [-f|--fileName] [-d|--delimiter=delmiter] [-N|--noHeader]
         [-u|--uniqueRows] [-s|--suppressHeaderFooter]
         [-c configSection|--configSection=configSection]
         [-v loggingLevel|--verbose=logingLevel] [-o logfile|--logfile=logfile]
         [csvFile [extractFile]]

REQUIRED
csvFile
The CSV csv file to be read. Required if an extractFile is specified.

extractFile
The output CSV extract file to be created


OPTIONS
-f|--fileName
Prepend the csvFilename to the extracted columns

-d|--delimiter=delimiter
Use the delimiter as the delimiter character if the input delimter cannot be automatically determined

-N|--noHeader
There is no Header in the file. The header will be defined in the config section.

-u|--uniqueRows
Only output one instance of each row

-s|--suppressHeaderFooter
Do not output header record or footer record (if there is one)

-c configSection|--configSection=configSection
Get wantedColumns from specific config section (default=wanted_columns)

-v loggingLevel|--verbose=loggingLevel
Set the level of logging that you want (defaut INFO).

-o logfile|--logfile=logfile
The name of a logging file where you want all messages captured.
'''


# Import all the modules that make life easy
import sys
import csv
import copy
import argparse
import logging
from configparser import ConfigParser as ConfParser
from configparser import MissingSectionHeaderError, NoSectionError, NoOptionError, ParsingError
import re
import datetime
import random


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


def parseHeader(topRow, wantedCols, paramCols, thisFile, thisFilename):
    '''
Parse the first line of the file and check that all required columns are present
    '''

    thisInputHas = {}
    for ii, thisHeader in enumerate(topRow):
        if thisHeader not in thisInputHas:
            thisInputHas[thisHeader] = ii

    # Compute the header filename
    newFilename = None
    if thisFilename:
        if ('filename' not in inputHas) or ('filename' not in wantedCols):
            newFilename = 'filename'
        else:
            for letter in range(ord('a'), ord('z')):
                newFilename = 'filename_' + chr(letter)
                if (newFilename not in thisInputHas) or (newFilename not in wantedCols):
                    break
            else:
                logging.fatal('Input csv file(%s) already has 25 prepended filenames', thisFile)
                return (None, None, None)

    # Check that every wanted column is in the csv file
    for ii, thisColumn in enumerate(wantedCols):
        if (thisColumn not in thisInputHas) and (thisColumn not in newColumns):
            if (thisFile is None) or (thisFile == '-'):
                logging.fatal('Wanted column(%s) not in input csv file(sys.stdin) and not in newColumns', thisColumn)
            else:
                logging.fatal('Wanted column(%s) not in input csv file(%s) and not in newColumns', thisColumn, thisFile)
            return (None, None, None)
    thisMax = 0
    for ii, thisColumn in enumerate(paramCols):
        if thisColumn not in thisInputHas:
            if (thisFile is None) or (thisFile == '-'):
                logging.fatal('Parameter column(%s) not in input csv file(sys.stdin)', thisColumn)
            else:
                logging.fatal('Parameter column(%s) not in input csv file(%s)', thisColumn, thisFile)
            return (None, None, None)
        if thisInputHas[thisColumn] > thisMax:
            thisMax = thisInputHas[thisColumn]
    return (thisInputHas, thisMax, newFilename)


# The main code
if __name__ == '__main__':
    '''
    The main code
    Parse the command line arguments and then read in the configuration file
    '''

    progName = sys.argv[0]
    progName = progName[0:-3]        # Strip off the .py ending

    parser = argparse.ArgumentParser()
    parser.add_argument('csvFile', metavar='csvFile', nargs='?', default=None, help='The name of the CSV file')
    parser.add_argument('extractFile', metavar='extractFile', nargs='?', default=None, help='The name of the extract CSV file')
    parser.add_argument('-f', '--fileName', dest='fileName', action='store_true', help='Prepend filename to every row')
    parser.add_argument('-d', '--delimiter', dest='delimiter', default=',',
                        help='Use the delimiter as the delimiter character if the input delimter cannot be automatically determined')
    parser.add_argument('-N', '--noHeader', dest='noHeader', action='store_true',
                        help='There is no Header in the file. The header will be defined in the config section.')
    parser.add_argument('-u', '--uniqueRows', dest='uniqueRows', action='store_true', help='Only output one instance of each row')
    parser.add_argument('-s', '--suppressHeaderFooter', dest='suppressHeaderFooter', action='store_true',
                        help='Do not output header and footer records')
    parser.add_argument('-c', '--configSection', dest='configSection', default='wanted_columns',
                        help='Get wantedColumns from specific section (default=wanted_columns)')
    parser.add_argument('-v', '--verbose', dest='verbose', type=int, choices=list(range(0, 5)),
                        help='The level of logging\n\t0=CRITICAL,1=ERROR,2=WARNING,3=INFO,4=DEBUG')
    parser.add_argument('-o', '--logfile', metavar='logfile', dest='logfile', help='The name of a logging file')

    # Parse the command line options
    args = parser.parse_args()
    csvFile = args.csvFile
    extractFile = args.extractFile
    fileName = args.fileName
    delimiter = args.delimiter
    noHeader = args.noHeader
    uniqueRows = args.uniqueRows
    suppressHeaderFooter = args.suppressHeaderFooter
    configSection = args.configSection

    # Set up logging
    logging_levels = {0: logging.CRITICAL, 1: logging.ERROR, 2: logging.WARNING, 3: logging.INFO, 4: logging.DEBUG}
    logfmt = progName + ' [%(asctime)s]: %(message)s'
    if args.verbose:    # Change the logging level from "WARN" if the -v vebose option is specified
        loggingLevel = args.verbose
        if args.logfile:        # and send it to a file if the -o logfile option is specified
            logging.basicConfig(format=logfmt, datefmt='%d/%m/%y %H:%M:%S %p',
                                level=logging_levels[loggingLevel], filemode='w', filename=args.logfile)
        else:
            logging.basicConfig(format=logfmt, datefmt='%d/%m/%y %H:%M:%S %p', level=logging_levels[loggingLevel])
    else:
        if args.logfile:        # send the default(WARN) logging to a file if the -o logfile option is specified
            logging.basicConfig(format=logfmt, datefmt='%d/%m/%y %H:%M:%S %p', filemode='w', filename=args.logfile)
        else:
            logging.basicConfig(format=logfmt, datefmt='%d/%m/%y %H:%M:%S %p')

    # Check we aren't adding stdin as a filename header
    if fileName:        # 'filename' should be in the header
        if (csvFile is None) or (csvFile == '-'):
            logging.fatal('stdin does not have a filename to prepend')
            logging.shutdown()
            for line in sys.stdin:      # Be nice - suck up the input
                pass
            sys.stdout.flush()
            sys.exit(EX_USAGE)

    # Then read in the csvExtract configuration file(csvExtract.cfg)
    config = ConfParser(allow_no_value=True)
    config.optionxform = str
    newColumns = {}
    wantedColumns = []
    paramColumns = []
    haveHeader = False
    headerLine = None
    try:
        config.read('csvExtract.cfg')
        # Now read in the wanted columns
        wanted = config.get(configSection, 'wantedColumns')
        for column in wanted.split(','):
            wantedColumns.append(column)
        if config.has_option(configSection, 'newColumns'):
            newCols = config.get(configSection, 'newColumns')
            theNewColumns = newCols.split('~')
            for i, column in enumerate(theNewColumns):
                columnDetails = column.split('=')
                if len(columnDetails) != 2:
                    raise ParsingError('New column must be title=value/expression')
                newColumns[columnDetails[0]] = columnDetails[1]
                for thisParam in re.finditer(r'\$\{([^}]+)\}', columnDetails[1]):
                    paramColumns.append(thisParam.group(1))
        if noHeader:
            headerLine = config.get(configSection, 'header')
            haveHeader = True            # headerLine from config file
    except(MissingSectionHeaderError, NoSectionError, NoOptionError, ParsingError) as detail:
        logging.critical('%s', detail)
        logging.shutdown()
        if (csvFile is None) or (csvFile == '-'):
            for line in sys.stdin:      # Be nice - suck up the input
                pass
        sys.stdout.flush()
        sys.exit(EX_CONFIG)

    # Check that nobody has specified the input csv file as the extract output file
    if (csvFile is not None) and (csvFile == extractFile):
        logging.fatal('Cannot use the same filename for the input CSV file and the output CSV extract file')
        logging.shutdown()
        sys.stdout.flush()
        sys.exit(EX_USAGE)

    # Check that the input CSV file can be opened and read
    inputFile = None
    inputCSV = None
    if (csvFile is None) or (csvFile == '-'):
        try:
            sys.stdin.reconfigure(encoding='utf-8')
            inputFile = sys.stdin
            headerLine = inputFile.readline()
            haveHeader = True        # headerLine from stdin
            inputDialect = csv.Sniffer().sniff(headerLine, delimiters=",:;|\t")
        except csv.Error:
            inputDialect = csv.excel
            inputDialect.delimiter = delimiter
            inputDialect.doublequote = True
            inputDialect.quoting = csv.QUOTE_MINIMAL
            inputDialect.quotechar = '"'
        except OSError:
            (exc_type, exc_value, exc_traceback) = sys.exc_info()
            logging.fatal('Cannot sniff csv input file(sys.stdin)')
            logging.fatal('Error: %s', exc_value)
            logging.shutdown()
            for line in sys.stdin:      # Be nice - suck up the input
                pass
            sys.stdout.flush()
            sys.exit(EX_DATAERR)
        if inputDialect.quoting == csv.QUOTE_NONE:
            inputDialect.doublequote = True
            inputDialect.quoting = csv.QUOTE_MINIMAL
            inputDialect.quotechar = '"'
    else:
        try:
            inputFile = open(csvFile, 'rt', encoding='utf-8')
        except OSError:
            (exc_type, exc_value, exc_traceback) = sys.exc_info()
            logging.fatal('Cannot open csv input file(%s)', csvFile)
            logging.fatal('Error: %s', exc_value)
            logging.shutdown()
            sys.stdout.flush()
            sys.exit(EX_NOINPUT)
        try:
            inputDialect = csv.Sniffer().sniff(inputFile.read(4096))
            inputFile.seek(0)
        except csv.Error:
            logging.warning('Could not sniff csv input file(%s) - csv.excel assumed', csvFile)
            inputDialect = csv.excel
            inputDialect.delimiter = delimiter
            inputDialect.doublequote = True
            inputDialect.quoting = csv.QUOTE_MINIMAL
            inputDialect.quotechar = '"'
            inputFile.seek(0)
        except OSError:
            (exc_type, exc_value, exc_traceback) = sys.exc_info()
            logging.fatal('Cannot sniff csv input file(%s)', csvFile)
            logging.fatal('Error: %s', exc_value)
            logging.shutdown()
            sys.stdout.flush()
            sys.exit(EX_DATAERR)
    inputCSV = csv.reader(inputFile, inputDialect)

    # Check that the output CSV file can be opened and written
    outputFile = None
    outputCSV = None
    outputDialect = copy.deepcopy(inputDialect)
    outputDialect.doublequote = True
    outputDialect.quoting = csv.QUOTE_MINIMAL
    outputDialect.quotechar = '"'
    if (extractFile is None) or (extractFile == '-'):
        outputDialect.lineterminator = '\n'
        outputFile = sys.stdout
        sys.stdout.reconfigure(encoding='utf-8')
    else:
        try:
            outputFile = open(extractFile, 'wt', encoding='utf-8', newline='')
        except OSError:
            (exc_type, exc_value, exc_traceback) = sys.exc_info()
            logging.fatal('Cannot open extract output file(%s)', extractFile)
            logging.fatal('Error: %s', exc_value)
            logging.shutdown()
            if (csvFile is None) or (csvFile == '-'):
                for line in sys.stdin:      # Be nice - suck up the input
                    pass
            sys.stdout.flush()
            sys.exit(EX_CANTCREAT)
    outputCSV = csv.writer(outputFile, outputDialect)

    # Now read the input file
    rows = 0
    header = True
    inputHas = {}            # The index of the column headings in the input file
    rowKeys = set()
    for inputRow in inputCSV:
        # Process the header line and output the heading
        if header:
            if haveHeader:      # sys.stdin - we have the header
                headerDialect = copy.deepcopy(inputDialect)
                if noHeader:
                    headerDialect = copy.deepcopy(csv.excel)
                for row in csv.reader([headerLine], dialect=headerDialect):
                    headerRow = row
                    break
            else:
                headerRow = inputRow[:]

            # Process the header row
            (inputHas, maxNew, headerFilename) = parseHeader(headerRow, wantedColumns, paramColumns, csvFile, fileName)
            if inputHas is None:        # Configuration failure - we will just suckup the input
                if (csvFile is None) or (csvFile == '-'):
                    header = False
                    continue
                else:
                    logging.shutdown()
                    sys.stdout.flush()
                    sys.exit(EX_CONFIG)

            # Output the heading
            if not suppressHeaderFooter:
                outputColumns = wantedColumns[:]
                try:
                    if fileName:
                        outputCSV.writerow([headerFilename] + outputColumns)
                    else:
                        outputCSV.writerow(outputColumns)
                except (OSError) as e:
                    if (extractFile is not None) and (extractFile != '-'):
                        (exc_type, exc_value, exc_traceback) = sys.exc_info()
                        logging.fatal('Cannot write to output file(%s)', extractFile)
                        logging.fatal('Error: %s', exc_value)
                        logging.shutdown()
                        if (csvFile is None) or (csvFile == '-'):
                            for line in sys.stdin:      # Be nice - suck up the input
                                pass
                    sys.stdout.flush()
                    sys.exit(e.errno)
            header = False

            # If no header in the file then process this row - we just processed a header from some other source
            if not haveHeader:
                continue

        # If we have a configuration error - then we are just sucking up stdin
        if inputHas is None:
            continue

        # Update the footer line if there is a footer
        if inputRow[0].upper() == 'END OF FILE':
            if not suppressHeaderFooter:
                inputRow[1] = rows
                try:
                    outputCSV.writerow(inputRow)
                except (OSError) as e:
                    if (extractFile is not None) and (extractFile != '-'):
                        (exc_type, exc_value, exc_traceback) = sys.exc_info()
                        logging.fatal('Cannot write to output file(%s)', extractFile)
                        logging.fatal('Error: %s', exc_value)
                        logging.shutdown()
                        if (csvFile is None) or (csvFile == '-'):
                            for line in sys.stdin:      # Be nice - suck up the input
                                pass
                        sys.stdout.flush()
                        sys.exit(e.errno)
                    else:
                        logging.shutdown()
                        sys.exit(EX_OK)
            break

        # Extract the required columns
        if fileName:
            extract = [csvFile]
        else:
            extract = []
        for i, column in enumerate(wantedColumns):
            if column in newColumns:
                if maxNew >= len(inputRow):
                    if (csvFile is None) or (csvFile == '-'):
                        logging.fatal('Input data row(%d) in file(sys.stdin) has insufficient columns(%s)',
                                      rows, repr(inputRow))
                    else:
                        logging.fatal('Input data row(%d) in file(%s) has insufficient columns(%s)',
                                      rows, csvFile, repr(inputRow))
                    logging.fatal('Need column(%d) - only found (%d) columns', maxNew, len(inputRow))
                    logging.shutdown()
                    if (csvFile is None) or (csvFile == '-'):
                        for line in sys.stdin:      # Be nice - suck up the input
                            pass
                    sys.stdout.flush()
                    sys.exit(EX_DATAERR)
                newExpression = newColumns[column]
                for param in inputHas:
                    newExpression = re.sub(r'\$\{' + param + r'\}', 'inputRow[inputHas[\'' + param + '\']]', newExpression)
                newValue = eval(newExpression)
                extract.append(newValue)
            else:
                thisCol = inputHas[column]
                if thisCol >= len(inputRow):
                    if (csvFile is None) or (csvFile == '-'):
                        logging.fatal('Input data row(%d) in file(sys.stdin) has insufficient columns(%s)',
                                      rows, repr(inputRow))
                    else:
                        logging.fatal('Input data row(%d) in file(%s) has insufficient columns(%s)',
                                      rows, csvFile, repr(inputRow))
                    logging.fatal('Need column(%d) - only found (%d) columns', maxNew, len(inputRow))
                    logging.shutdown()
                    if (csvFile is None) or (csvFile == '-'):
                        for line in sys.stdin:      # Be nice - suck up the input
                            pass
                    sys.stdout.flush()
                    sys.exit(EX_DATAERR)
                extract.append(inputRow[thisCol])
        if uniqueRows:
            rowKey = '~'.join(extract)
            if rowKey in rowKeys:
                continue
            rowKeys.add(rowKey)
        try:
            outputCSV.writerow(extract)
        except (OSError, BrokenPipeError) as e:
            if (extractFile is not None) and (extractFile != '-'):
                (exc_type, exc_value, exc_traceback) = sys.exc_info()
                logging.fatal('Cannot write to output file(%s)', extractFile)
                logging.fatal('Error: %s', exc_value)
                logging.shutdown()
                if (csvFile is None) or (csvFile == '-'):
                    for line in sys.stdin:      # Be nice - suck up the input
                        pass
                sys.stdout.flush()
                sys.exit(e.errno)
            else:
                logging.shutdown()
                if inputHas is None:
                    sys.exit(EX_CONFIG)
                else:
                    sys.exit(EX_OK)
        rows += 1

    try:
        sys.stdout.flush()
    except (OSError, BrokenPipeError) as e:
        logging.shutdown()
        if inputHas is None:
            sys.exit(EX_CONFIG)
        else:
            sys.exit(EX_OK)

    if (extractFile is not None) and (extractFile != '-'):
        try:
            outputFile.close()
        except (OSError, BrokenPipeError) as e:
            logging.shutdown()
            if inputHas is None:
                sys.exit(EX_CONFIG)
            else:
                sys.exit(EX_OK)

    logging.shutdown()

    # Check for a configuration error
    if inputHas is None:
        sys.exit(EX_CONFIG)
    else:
        sys.exit(EX_OK)
