#!/usr/bin/env python

# pylint: disable=line-too-long, invalid-name, pointless-string-statement

'''
A script to assign SA1 and LGA codes to every G-NAF street_locality_pid


SYNOPSIS
$ python getStreetSA1LGA_fast.py inputDir [StreetSA1LGAoutputFile] [-v loggingLevel|--verbose=logingLevel]
                                  [-L logDir|--logDir=logDir] [-l logfile|--logfile=logfile]

REQUIRED
inputDir
The directory containing the G-NAF psv files

StreetSA1LGAoutputFile
The name of the output file of street SA1 and LGA values to be created - default:street_SA1LGA.psv


OPTIONS
-v loggingLevel|--verbose=loggingLevel
Set the level of logging that you want (defaut INFO).

-L logDir
The directory where the log file will be written (default='.')

-l logfile|--logfile=logfile
The name of a logging file where you want all messages captured (default=None)

This script read the G-NAF files to determine the Mesh Block code for every propery
and the mappings from Mesh Block code to SA1 and LGA.
It then reads all the streets and determines the most popular Mesh Block for that street.
It then maps that Mesh Block to SA1 and LGA.
It also computes, for all the properties in that Mesh Block, an average longitude and average latitude.
Each G-NAF street_locality_pid is then has an SA1 code, an LGA code, a latitude and a longitude.

Note: Whilst this version is faster it has some limitation.
It creates no data for streets that have no properties.
It can create biased data; a street with a shopping centre will be assigned the SA1/LGA of the shopping centre
because that's the greatest concentration of properties.
'''

# Import all the modules that make life easy
import sys
import os
import argparse
import logging
import csv


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



# The main code
if __name__ == '__main__':
    '''
A script to assign SA1 and LGA codes to every G-NAF street_locality_pid
Start by reading all Mesh Block mapping files.
Then reads all the address details.
Then it reads the street locality file and assigns an SA1 and LGA code to each street.
    '''

    # Get the script name (without the '.py' extension)
    progName = os.path.basename(sys.argv[0])
    progName = progName[0:-3]        # Strip off the .py ending

    # Define the command line options
    parser = argparse.ArgumentParser(prog=progName)
    parser.add_argument('-G', '--GNAFdir', default='../G-NAF', help='The name of the G-NAF data directory (default ../G-NAF)')
    parser.add_argument('-A', '--ABSdir', default='../ABS', help='The name of the ABS data directory (default ../ABS)')
    parser.add_argument ('-o', '--StreetSA1LGAoutputFile', default='street_SA1LGA.psv',
                         help='The name of the output file of street SA1 and LGA data to be created. (default street_SA1LGA.psv)')
    parser.add_argument('-v', '--verbose', dest='verbose', type=int, choices=list(range(0, 5)),
                        help='The level of logging\n\t0=CRITICAL,1=ERROR,2=WARNING,3=INFO,4=DEBUG')
    parser.add_argument('-L', '--logDir', dest='logDir', default='.', help='The name of a logging directory')
    parser.add_argument('-l', '--logfile', dest='logfile', default=None, help='The name of the logging file')

    # Parse the command line options
    args = parser.parse_args()
    GNAFdir = args.GNAFdir
    ABSdir = args.ABSdir
    StreetSA1LGAoutputFile = args.StreetSA1LGAoutputFile
    loggingLevel = args.verbose
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

    # Start by reading in the G-NAF and ABS mapping data
    addressMB = {}
    # ADDRESS_MESH_BLOCK_2016_PID|DATE_CREATED|DATE_RETIRED|ADDRESS_DETAIL_PID|MB_MATCH_CODE|MB_2016_PID
    for SandT in ['ACT', 'NSW', 'NT', 'OT', 'QLD', 'SA', 'TAS', 'VIC', 'WA']:
        with open(os.path.join(GNAFdir, 'Standard', SandT + '_ADDRESS_MESH_BLOCK_2016_psv.psv'), 'rt', encoding='utf-8', newline='') as mbFile:
            mbReader = csv.DictReader(mbFile, dialect=csv.excel, delimiter='|')
            for row in mbReader:
                if row['DATE_RETIRED'] != '':        # Skip if retired
                    continue
                if row['ADDRESS_DETAIL_PID'] =='':
                    continue
                addressMB[row['ADDRESS_DETAIL_PID']] = row['MB_2016_PID']
    logging.info('%d Mesh block pids read in', len(addressMB))

    MB = {}
    # MB_2016_PID|DATE_CREATED|DATE_RETIRED|MB_2016_CODE
    for SandT in ['ACT', 'NSW', 'NT', 'OT', 'QLD', 'SA', 'TAS', 'VIC', 'WA']:
        with open(os.path.join(GNAFdir, 'Standard',  SandT + '_MB_2016_psv.psv'), 'rt', encoding='utf-8', newline='') as mbFile:
            mbReader = csv.DictReader(mbFile, dialect=csv.excel, delimiter='|')
            for row in mbReader:
                if row['DATE_RETIRED'] != '':        # Skip if retired
                    continue
                if row['MB_2016_PID'] == '':
                    continue
                MB[row['MB_2016_PID']] = row['MB_2016_CODE']
    logging.info('%d Mesh blocks read in', len(MB))

    # Now the SA1 and LGA data
    SA1 = {}
    # MB_CODE_2016,MB_CATEGORY_NAME_2016,SA1_MAINCODE_2016,SA1_7DIGITCODE_2016,SA2_MAINCODE_2016,SA2_5DIGITCODE_2016,SA2_NAME_2016,SA3_CODE_2016,SA3_NAME_2016,SA4_CODE_2016,SA4_NAME_2016,GCCSA_CODE_2016,GCCSA_NAME_2016,STATE_CODE_2016,STATE_NAME_2016,AREA_ALBERS_SQKM
    for SandT in ['ACT', 'NSW', 'NT', 'OT', 'QLD', 'SA', 'TAS', 'VIC', 'WA']:
        with open(os.path.join(ABSdir, 'MB', 'MB_2016_' + SandT + '.csv'), 'rt', encoding='utf-8', newline='') as mbFile:
            mbReader = csv.DictReader(mbFile, dialect=csv.excel, delimiter=',')
            for row in mbReader:
                if row['MB_CODE_2016'] == '':
                    continue
                SA1[row['MB_CODE_2016']] = row['SA1_MAINCODE_2016']
    logging.info('%d SA1 codes read in', len(SA1))

    LGA = {}
    # MB_CODE_2016,LGA_CODE_2020,LGA_NAME_2020,STATE_CODE_2016,STATE_NAME_2016,AREA_ALBERS_SQKM
    for SandT in ['ACT', 'NSW', 'NT', 'OT', 'QLD', 'SA', 'TAS', 'VIC', 'WA']:
        with open(os.path.join(ABSdir, 'LGA', 'LGA_2020_' + SandT + '.csv'), 'rt', encoding='utf-8', newline='') as lgaFile:
            lgaReader = csv.DictReader(lgaFile, dialect=csv.excel, delimiter=',')
            for row in lgaReader:
                if row['MB_CODE_2016'] == '':
                    continue
                LGA[row['MB_CODE_2016']] = row['LGA_CODE_2020']
    logging.info('%d LGA codes read in', len(LGA))

    # Then the G-NAF Address details file
    streetMB = {}
    # ADDRESS_DETAIL_PID|DATE_CREATED|DATE_LAST_MODIFIED|DATE_RETIRED|BUILDING_NAME|LOT_NUMBER_PREFIX|LOT_NUMBER|LOT_NUMBER_SUFFIX|FLAT_TYPE_CODE|FLAT_NUMBER_PREFIX|FLAT_NUMBER|FLAT_NUMBER_SUFFIX|LEVEL_TYPE_CODE|LEVEL_NUMBER_PREFIX|LEVEL_NUMBER|LEVEL_NUMBER_SUFFIX|NUMBER_FIRST_PREFIX|NUMBER_FIRST|NUMBER_FIRST_SUFFIX|NUMBER_LAST_PREFIX|NUMBER_LAST|NUMBER_LAST_SUFFIX|STREET_LOCALITY_PID|LOCATION_DESCRIPTION|LOCALITY_PID|ALIAS_PRINCIPAL|POSTCODE|PRIVATE_STREET|LEGAL_PARCEL_ID|CONFIDENCE|ADDRESS_SITE_PID|LEVEL_GEOCODED_CODE|PROPERTY_PID|GNAF_PROPERTY_PID|PRIMARY_SECONDARY
    for SandT in ['ACT', 'NSW', 'NT', 'OT', 'QLD', 'SA', 'TAS', 'VIC', 'WA']:
        with open(os.path.join(GNAFdir, 'Standard', SandT + '_ADDRESS_DETAIL_psv.psv'), 'rt', encoding='utf-8', newline='') as addressFile:
            addressReader = csv.DictReader(addressFile, dialect=csv.excel, delimiter='|')
            for row in addressReader:
                if row['DATE_RETIRED'] != '':        # Skip if retired
                    continue
                confidence = row['CONFIDENCE']
                try:
                    confidence = int(confidence)
                except (ValueError, TypeError):
                    confidence = 0
                if confidence < 1:
                    continue
                if row['ADDRESS_DETAIL_PID'] == '':
                    continue
                address_pid = row['ADDRESS_DETAIL_PID']
                if row['STREET_LOCALITY_PID'] == '':
                    continue
                if address_pid not in addressMB:
                    continue
                if addressMB[address_pid] not in MB:
                    continue
                meshBlock = MB[addressMB[address_pid]]
                street_pid = row['STREET_LOCALITY_PID']
                if street_pid not in streetMB:
                    streetMB[street_pid] = {}
                if meshBlock not in streetMB[street_pid]:
                    streetMB[street_pid][meshBlock] = 1
                else:
                    streetMB[street_pid][meshBlock] += 1
    logging.info('%d streets with mesh blocks read in', len(streetMB))

    # Open the output file and write the heading
    streetSA1LGAfile = open(StreetSA1LGAoutputFile, 'wt', newline='', encoding='utf-8')
    streetSA1LGAwriter = csv.writer(streetSA1LGAfile, dialect=csv.excel, delimiter='|')
    outRow = ['street_locality_pid', 'SA1_MAINCODE_2016', 'LGA_CODE_2020', 'longitude', 'latitude']
    streetSA1LGAwriter.writerow(outRow)

    # Next read in all the street locality GPS details
    # STREET_LOCALITY_POINT_PID|DATE_CREATED|DATE_RETIRED|STREET_LOCALITY_PID|BOUNDARY_EXTENT|PLANIMETRIC_ACCURACY|LONGITUDE|LATITUDE
    for SandT in ['ACT', 'NSW', 'NT', 'OT', 'QLD', 'SA', 'TAS', 'VIC', 'WA']:
        streetLocalityFile = os.path.join(GNAFdir, 'Standard', SandT + '_STREET_LOCALITY_POINT_psv.psv')
        with open(streetLocalityFile, 'rt', encoding='utf-8', newline='') as streetFile:
            streetReader = csv.DictReader(streetFile, dialect=csv.excel, delimiter='|')
            for row in streetReader:
                if row['DATE_RETIRED'] != '':        # Skip if retired
                    logging.info('Retired street')
                    continue
                street_pid = row['STREET_LOCALITY_PID']
                if street_pid not in streetMB:
                    logging.info('street_pid %s not in streetMB', street_pid)
                    continue
                longCode = row['LONGITUDE']
                latCode = row['LATITUDE']
                logging.debug('Checking street_locality_pid(%s:%s,%s)', street_pid, longCode, latCode)
                try:
                    longitude = float(longCode)
                except ValueError:
                    logging.info('invalid longitude(%s)', longCode)
                    continue
                try:
                    latitude = float(latCode)
                except ValueError:
                    logging.info('invalid latitude(%s)', latCode)
                    continue

                # Find the most popular Mesh Block for this street
                meshBlock = max(streetMB[street_pid], key=streetMB[street_pid].get)
                if meshBlock not in SA1:
                    logging.info('Mesh Block %s not in SA1', meshBlock)
                    continue
                if meshBlock not in LGA:
                    logging.info('Mesh Block %s not in LGA', meshBlock)
                    continue
                thisSA1 = SA1[meshBlock]
                thisLGA = LGA[meshBlock]

                logging.debug('Found street_pid(%s)[%s,%s], SA1(%s), LGA(%s)', street_pid, longCode, latCode, thisSA1, thisLGA)
                outRow = [street_pid, thisSA1, thisLGA, longitude, latitude]
                streetSA1LGAwriter.writerow(outRow)

    streetSA1LGAfile.close()

    logging.shutdown()
    sys.stdout.flush()
    sys.exit(1)
