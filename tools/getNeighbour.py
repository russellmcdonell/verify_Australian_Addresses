#!/usr/bin/env python

# pylint: disable=line-too-long, pointless-string-statement, invalid-name


'''
A script to extract Neighbours data from a set of G-NAF PSV files

SYNOPSIS
$ python getNeighbour.py [-G GNAFdir|--GNAFdir=GNAFdir] [NeighbourOutputFile] [-v loggingLevel|--loggingLevel=loggingLevel]
                              [-L logDir|--logDir=logDir] [-l logfile|--logfile=logfile]

REQUIRED


OPTIONS
-G GNAFdir|--GNAFdir=GNAFdir
The directory containing the G-NAF psv files - default ../G-NAF

NeighbourOutputFile
The name of the output file of addresses to be created - default:neighbours.psv

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


if __name__ == '__main__' :
    '''
The main code
    '''

    # Save the program name
    progName = sys.argv[0]
    progName = progName[0:-3]        # Strip off the .py ending

    parser = argparse.ArgumentParser()
    parser.add_argument('-G', '--GNAFdir', dest='GNAFdir', default='../G-NAF', help='The name of the input directory (default ../G-NAF)')
    parser.add_argument ('NeighbourOutputFile', nargs='?', default='neighbours.psv',
                         help='The name of the output file of neighbour data to be created. (default neighbours.csv)')
    parser.add_argument ('-v', '--verbose', dest='verbose', type=int, choices=list(range(0,5)),
                         help='The level of logging\n\t0=CRITICAL,1=ERROR,2=WARNING,3=INFO,4=DEBUG')
    parser.add_argument('-L', '--logDir', dest='logDir', default='.', help='The name of a logging directory')
    parser.add_argument('-l', '--logfile', dest='logfile', default=None, help='The name of the logging file')

    # Parse the command line options
    args = parser.parse_args()
    GNAFdir = args.GNAFdir
    NeighbourOutputFile = args.NeighbourOutputFile
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


    neighbour = {}
    # Get the neighbours data - CODE is the primary key
    # LOCALITY_NEIGHBOUR_PID|DATE_CREATED|DATE_RETIRED|LOCALITY_PID|NEIGHBOUR_LOCALITY_PID
    for SandT in ['ACT', 'NSW', 'NT', 'OT', 'QLD', 'SA', 'TAS', 'VIC', 'WA']:
        neighbourfile = os.path.join(GNAFdir, 'Standard', SandT + '_LOCALITY_NEIGHBOUR_psv.psv')
        with open(neighbourfile, 'rt', newline='', encoding='utf-8') as neighbourFile :
            neighbourReader = csv.DictReader(neighbourFile, dialect=csv.excel, delimiter='|')
            for row in neighbourReader :
                if row['DATE_RETIRED'] != '':        # Skip if retired
                    continue
                if row['LOCALITY_PID'] not in neighbour:
                    neighbour[row['LOCALITY_PID']] = set()
                if row['NEIGHBOUR_LOCALITY_PID'] not in neighbour:
                    neighbour[row['NEIGHBOUR_LOCALITY_PID']] = set()
                neighbour[row['LOCALITY_PID']].add(row['NEIGHBOUR_LOCALITY_PID'])
                neighbour[row['NEIGHBOUR_LOCALITY_PID']].add(row['LOCALITY_PID'])


    # Now output all the neighbour data
    # neighbour_pid,neighbour_name,neighbour_abbreviation
    csvOutfile = open(NeighbourOutputFile, 'wt', newline='', encoding='utf-8')
    csvwriter = csv.writer(csvOutfile, dialect=csv.excel, delimiter='|')
    heading = ['locality_pid', 'neighbour']
    csvwriter.writerow(heading)

    # Output the neighbour
    for thisNeighbour in sorted(neighbour):
        for nextNeighbour in sorted(neighbour[thisNeighbour]):
            csvwriter.writerow([thisNeighbour, nextNeighbour])

    csvOutfile.close()

    logging.shutdown()
    sys.stdout.flush()
    sys.exit(EX_OK)
