#!/usr/bin/env python

# pylint: disable=line-too-long, pointless-string-statement, invalid-name

'''
A script to extract addresses trims from a set of G-NAF PSV files

SYNOPSIS
$ python getAddressTrim.py [-G GNAFdir|--GNAFdir=GNAFdir]
                           [-v loggingLevel|--loggingLevel=loggingLevel]
                           [-L logDir|--logDir=logDir] [-l logfile|--logfile=logfile]

REQUIRED


OPTIONS
-G GNAFdir|--GNAFdir=GNAFdir
The name of the directory containing the G-NAF psv files - default ../G-NAF

-v loggingLevel|--verbose=loggingLevel
Set the level of logging that you want.

-L logDir
The directory where the log file will be written (default='.')

-l logfile|--logfile=logfile
The name of a logging file where you want all messages captured (default=None)

'''

import sys
import os
import csv
import argparse
import logging

# This next section is plagurised from /usr/include/sysexits.h
EX_OK = 0           # successful termination
EX_WARN = 1         # non-fatal termination with warnings

EX_USAGE = 64           # command line usage error
EX_DATAERR = 65         # data format error
EX_NOINPUT = 66         # cannot open input
EX_NOUSER = 67          # addressee unknown
EX_NOHOST = 68          # host name unknown
EX_UNAVAILABLE = 69     # service unavailable
EX_SOFTWARE = 70        # internal software error
EX_OSERR = 71           # system error (e.g., can't fork)
EX_OSFILE = 72          # critical OS file missing
EX_CANTCREAT = 73       # can't create (user) output file
EX_IOERR = 74           # input/output error
EX_TEMPFAIL = 75        # temp failure; user is invited to retry
EX_PROTOCOL = 76        # remote error in protocol
EX_NOPERM = 77          # permission denied
EX_CONFIG = 78          # configuration error


if __name__ == '__main__':
    '''
    The main code - read in any additional configuration data, then the G-NAF data
    '''

    # Save the program name
    progName = sys.argv[0]
    progName = progName[0:-3]        # Strip off the .py ending

    # Parse the arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('-G', '--GNAFdir', default='../GNAF', dest='GNAFdir',
                        help='The name of the directory containing the G-NAF psv files - default ../G-NAF')
    parser.add_argument ('-v', '--verbose', dest='verbose', type=int, choices=list(range(0,5)),
                         help='The level of logging\n\t0=CRITICAL,1=ERROR,2=WARNING,3=INFO,4=DEBUG')
    parser.add_argument('-L', '--logDir', dest='logDir', default='.', help='The name of a logging directory')
    parser.add_argument('-l', '--logfile', dest='logfile', default=None, help='The name of the logging file')
    args = parser.parse_args()

    # Parse the command line options
    GNAFdir = args.GNAFdir
    logDir = args.logDir
    logfile = args.logfile

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

    # Create setfor the flats and levels. We use a set because it eleminate duplicates
    flats = set()
    levels = set()
    heading = ['code']

    # Get the flat types data - CODE is the primary key
    # CODE|NAME|DESCRIPTION
    flatTypefile = os.path.join(GNAFdir, 'Authority Code', 'Authority_Code_FLAT_TYPE_AUT_psv.psv')
    with open(flatTypefile, 'rt', encoding='utf-8') as flatTypeFile:
        flatTypeReader = csv.DictReader(flatTypeFile, dialect=csv.excel, delimiter='|')
        for row in flatTypeReader :
            for col, flat in row.items():
                if (flat == '') or (flat == 'NULL') or flat.isspace():
                    continue
                flats.add(flat)

    # Output the flats
    csvOutfile = open('address_flat.psv', 'wt', newline='', encoding='utf-8')
    csvwriter = csv.writer(csvOutfile, dialect=csv.excel, delimiter='|')
    csvwriter.writerow(heading)
    for thisFlat in reversed(sorted(list(flats))):
        row = [thisFlat]
        csvwriter.writerow(row)
    csvOutfile.close()

    # Get the level types data - CODE is the primary key
    # CODE|NAME|DESCRIPTION
    levelTypefile = os.path.join(GNAFdir, 'Authority Code', 'Authority_Code_LEVEL_TYPE_AUT_psv.psv')
    with open(levelTypefile, 'rt', encoding='utf-8') as levelTypeFile:
        levelTypeReader = csv.DictReader(levelTypeFile, dialect=csv.excel, delimiter='|')
        for row in levelTypeReader :
            for col, level in row.items() :
                if (level == '') or (level == 'NULL') or level.isspace():
                    continue
                levels.add(level)

    # Output the levels
    csvOutfile = open('address_level.psv', 'wt', newline='', encoding='utf-8')
    csvwriter = csv.writer(csvOutfile, dialect=csv.excel, delimiter='|')
    csvwriter.writerow(heading)
    for thisLevel in reversed(sorted(list(levels))):
        row = [thisLevel]
        csvwriter.writerow(row)
    csvOutfile.close()

    logging.shutdown()
    sys.stdout.flush()
    sys.exit(EX_OK)
