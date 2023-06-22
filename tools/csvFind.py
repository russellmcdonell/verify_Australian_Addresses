#!/usr/bin/env python

# pylint: disable=line-too-long, invalid-name, pointless-string-statement, unused-variable

'''
Script to find a subset of rows from an csv file


SYNOPSIS
$ python csvFind.py
         [-f|--fileName] [-d|--delimiter=delmiter] [-N|--noHeader]
         [-s|--suppressHeaderFooter]
         [-c configSection|--configSection=configSection]
         [-A|--AND] [-X|--findExcept]
         [-v loggingLevel|--verbose=logingLevel] [-o logfile|--logfile=logfile]
         [csvFile [findFile]]

REQUIRED
csvFile
The input CSV csv file

findFile
The output CSV file to be created


OPTIONS
-f|--fileName
Prepend the filename to the found columns

-d|--delimiter=delimiter
Use the delimiter as the delimiter character if the input delimter cannot be automatically determined

-N|--noHeader
There is no Header in the file. The header will be defined in the config section.

-c configSection|--configSection=configSection
Get findExpression from specific config section (default=Find)

-A|-AND
The final FindExpression is the 'AND' of all of the FindExpressions

-X|-findExcept
Find all records except those matching the FindExpression

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


def parseHeader(thisRow, findCols, thisFile, thisFilename):
    '''
Parse the first line of the file and check that all required columns are present
    '''

    fileHas = {}
    lastCol = 0
    for i, heading in enumerate(thisRow):
        if heading not in fileHas:
            fileHas[heading] = i

    # Compute the header thisFilename
    headerName = None
    if thisFilename:
        if 'thisFilename' not in fileHas:
            headerName = 'thisFilename'
        else:
            for letter in range(ord('a'), ord('z')):
                if 'thisFilename_' + chr(letter) not in fileHas:
                    headerName = 'thisFilename_' + chr(letter)
                    break
            else:
                logging.fatal('Input csv file(%s) already has 25 prepended thisFilenames', thisFile)
                return(None, None, None)

    # Check that every find column is in the csv file
    for ii, col in enumerate(findCols):
        if col not in fileHas:
            if (thisFile is None) or (thisFile == '-'):
                logging.fatal('Find column(%s) not in input csv file(sys.stdin)', col)
            else:
                logging.fatal('Find column(%s) not in input csv file(%s)', col, thisFile)
            return(None, None, None)
        thisColumn = fileHas[col]
        if thisColumn > lastCol:
            lastCol = thisColumn
    return (fileHas, lastCol, headerName)


# The main code
if __name__ == '__main__':
    '''
The main code
Parse the command line arguments and then read in the configuration file
    '''

    progName = sys.argv[0]
    progName = progName[0:-3]        # Strip off the .py ending

    parser = argparse.ArgumentParser()
    parser.add_argument('csvFile', metavar='csvFile', nargs='?', default=None,
                        help='The name of the input CSV file or "-" for stdin [optional: stdin if neither csvFile nor findFile specified]')
    parser.add_argument('findFile', metavar='findFile', nargs='?', default=None,
                        help='The name of the output CSV file or "-" for stdout. [optional: stdout if not specified]')
    parser.add_argument('-f', '--fileName', dest='fileName', action='store_true', help='Prepend filename to every row')
    parser.add_argument('-d', '--delimiter', dest='delimiter', default=',',
                        help='Use the delimiter as the delimiter character if the input delimter cannot be automatically determined')
    parser.add_argument('-N', '--noHeader', dest='noHeader', action='store_true',
                        help='There is no Header in the file. The header will be defined in the config section.')
    parser.add_argument('-c', '--configSection', dest='configSection', default='Find',
                        help='Get findExpression from specific section (default=Find)')
    parser.add_argument('-A', '--AND', dest='findRelationship', action='store_true',
                        help='Join the findExpressions with \'and\' (default=\'or\')')
    parser.add_argument('-x', '--findExcept', dest='findExcept', action='store_true',
                        help='Find all records except those matching the findExpressions')
    parser.add_argument('-s', '--suppressHeaderFooter', dest='suppressHeaderFooter', action='store_true',
                        help='Do not output header and footer records')
    parser.add_argument('-v', '--verbose', dest='verbose', type=int, choices=list(range(0, 5)),
                        help='The level of logging\n\t0=CRITICAL, 1=ERROR,2=WARNING,3=INFO,4=DEBUG')
    parser.add_argument('-o', '--logfile', metavar='logfile', dest='logfile', help='The name of a logging file')

    # Parse the command line options
    args = parser.parse_args()
    csvFile = args.csvFile
    findFile = args.findFile
    fileName = args.fileName
    delimiter = args.delimiter
    noHeader = args.noHeader
    configSection = args.configSection
    findRelationship = args.findRelationship
    findExcept = args.findExcept
    suppressHeaderFooter = args.suppressHeaderFooter

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
    wantedColumns = []
    if fileName:        # 'filename' should be in the header
        if (csvFile is None) or (csvFile == '-'):
            logging.fatal('stdin does not have a filename to prepend')
            logging.shutdown()
            for line in sys.stdin:      # Be nice - suck up the input
                pass
            sys.stdout.flush()
            sys.exit(EX_USAGE)

    # Then read in the csvFind configuration file(csvFind.cfg)
    config = ConfParser(allow_no_value=True)
    config.optionxform = str
    findColumns = []
    haveHeader = False
    headerLine = None
    try:
        config.read('csvFind.cfg')
        # Now read in the FindExpression(s)
        findDepth = 0
        findExpression = ''
        openBracket = False
        for (name, value) in config.items(configSection):
            if noHeader and (name == 'header'):
                continue
            if value in ['endAnd', 'endOr']:
                if findDepth > 0:
                    findDepth -= 1
                    findExpression += ')'
                else:
                    logging.critical('Unexpected FindExpression=endAnd/endOr')
                    logging.shutdown()
                    if (csvFile is None) or (csvFile == '-'):
                        for line in sys.stdin:      # Be nice - suck up the input
                            pass
                    sys.stdout.flush()
                    sys.exit(EX_CONFIG)
                if value == 'endAnd':
                    findRelationship = True
                else:
                    findRelationship = False
                continue
            elif not openBracket:
                if findExpression != '':
                    if findRelationship:
                        findExpression += ' and '
                    else:
                        findExpression += ' or '
            else:
                openBracket = False
            if value in ['and', 'or']:
                findExpression += '('
                findDepth += 1
                if value == 'and':
                    findRelationship = True
                else:
                    findRelationship = False
                openBracket = True
            else:
                findExpression += '(' + value + ')'
        if findDepth > 0:
            logging.critical('Missing FindExpression=endAnd/endOr')
            logging.shutdown()
            if (csvFile is None) or (csvFile == '-'):
                for line in sys.stdin:      # Be nice - suck up the input
                    pass
            sys.stdout.flush()
            sys.exit(EX_CONFIG)
        for thisParam in re.finditer(r'\$\{([^}]+)\}', findExpression):
            findColumns.append(thisParam.group(1))
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
    for param in findColumns:
        findExpression = re.sub(r'\$\{' + param + r'\}', 'inputRow[inputHas[\'' + param + '\']]', findExpression)
    logging.debug('findExpress(%s)', findExpression)

    # Check that nobody has specified the input csv file as the find output file
    if (csvFile is not None) and (csvFile == findFile):
        logging.fatal('Cannot use the same filename for the input CSV file and the output CSV file')
        logging.shutdown()
        sys.stdout.flush()
        sys.exit(EX_CONFIG)

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
    if (findFile is None) or (findFile == '-'):
        outputDialect.lineterminator = '\n'
        outputFile = sys.stdout
        sys.stdout.reconfigure(encoding='utf-8')
    else:
        try:
            outputFile = open(findFile, 'wt', encoding='utf-8')
        except OSError:
            (exc_type, exc_value, exc_traceback) = sys.exc_info()
            logging.fatal('Cannot open find output file(%s)', findFile)
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
    for inputRow in inputCSV:
        # Process the header line and output the heading
        if header:
            if haveHeader:      # header from config file or sys.stdin - we have the header
                headerDialect = copy.deepcopy(inputDialect)
                if noHeader:    # header from config file - excel compliant
                    headerDialect = copy.deepcopy(csv.excel)
                for row in csv.reader([headerLine], dialect=headerDialect):
                    headerRow = row
                    break
            else:
                headerRow = inputRow[:]

            # Process the header
            (inputHas, maxCol, headerFilename) = parseHeader(headerRow, findColumns, csvFile, fileName)
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
                try:
                    if fileName:
                        outputCSV.writerow([headerFilename] + headerRow)
                    else:
                        outputCSV.writerow(headerRow)
                except (OSError) as e:
                    if (findFile is not None) and (findFile != '-'):
                        (exc_type, exc_value, exc_traceback) = sys.exc_info()
                        logging.fatal('Cannot write to output file(%s)', findFile)
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
            header = False

            # If no header in the file then process this row - we just processed a header from some other source
            if not noHeader:
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
                    if (findFile is not None) and (findFile != '-'):
                        (exc_type, exc_value, exc_traceback) = sys.exc_info()
                        logging.fatal('Cannot write to output file(%s)', findFile)
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

        # Find the required records
        if maxCol >= len(inputRow):
            if (csvFile is None) or (csvFile == '-'):
                logging.fatal('Input data row(%d) in file(sys.stdin) has insufficient columns(%s)',
                              rows, repr(inputRow))
            else:
                logging.fatal('Input data row(%d) in file(%s) has insufficient columns(%s)',
                              rows, csvFile, repr(inputRow))
            logging.shutdown()
            sys.stdout.flush()
            sys.exit(EX_DATAERR)
        found = eval(findExpression)
        if found == findExcept:
            continue
        try:
            if fileName:
                outputCSV.writerow([headerFilename] + inputRow)
            else:
                outputCSV.writerow(inputRow)
        except (OSError, BrokenPipeError) as e:
            if (findFile is not None) and (findFile != '-'):
                (exc_type, exc_value, exc_traceback) = sys.exc_info()
                logging.fatal('Cannot write to output file(%s)', findFile)
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

    if (findFile is not None) and (findFile != '-'):
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
