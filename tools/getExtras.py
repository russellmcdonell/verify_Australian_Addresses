#!/usr/bin/env python

# pylint: disable=line-too-long, pointless-string-statement, invalid-name, missing-function-docstring

'''
A script to create extraFlats.psv, extraLevels.psv, extraTrims.psv,
extraStates.psv, extraStreetTypes.psv, extraStreetSuffixes.psv
extraPostcodeSA1LGA.psv and extraLocality.psv from getConfig.json


SYNOPSIS
$ python getExtras.py [-A ABSdir|--ABSdir=ABSdir]
                      [-v loggingLevel|--verbose=logingLevel]
                      [-L logDir|--logDir=logDir] [-l logfile|--logfile=logfile]

REQUIRED


OPTIONS
-A ABSdir
The directory containing the ABS SA1 and LGA directories (.shp, .dbf and .shx files)
(default "../ABS)

-v loggingLevel|--verbose=loggingLevel
Set the level of logging that you want (defaut INFO).

-L logDir
The directory where the log file will be written (default='.')

-l logfile|--logfile=logfile
The name of a logging file where you want all messages captured (default=None)

'''

# Import all the modules that make life easy
import sys
import os
import argparse
import logging
import csv
import re
import json
import shapefile


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


def checkCrossing(geoLat, geoLong, segLat1, segLong1, segLat2, segLong2, isInflection):
    '''
    Check if an imaginary line going East (increasing longitude) from a point (geoLong, geoLat)
    crosses the line segment, defined by (segLong1, segLat1) at one end and (segLong2, segLat2) at the other end
    '''

    logging.debug('checkCrossing(%f,%f - ([%f,%f],[%f,%f], %s)',
                 geoLat, geoLong, segLat1, segLong1, segLat2, segLong2, str(isInflection))

    # Check the line's bounding box to see if a crossing is possible.
    if(geoLong > segLong1) and (geoLong > segLong2):    # The point is East of the line
        logging.debug('Point is [%.7f,%.7f] is East of line [%.7f,%.7f] to [%.7f,%.7f]',
            geoLat, geoLong, segLat1, segLong1, segLat2, segLong2)
        return (False, False)                            # so going East from the point won't reach the line segment
    if(geoLat > segLat1) and (geoLat > segLat2):        # The point is North of the line
        logging.debug('Point is [%.7f,%.7f] is North of line [%.7f,%.7f] to [%.7f,%.7f]',
            geoLat, geoLong, segLat1, segLong1, segLat2, segLong2)
        return (False, False)                            # so going East from the point won't reach the line segment
    if(geoLat < segLat1) and (geoLat < segLat2):        # The point South of the line segment
        logging.debug('Point is [%.7f,%.7f] is South of line [%.7f,%.7f] to [%.7f,%.7f]',
            geoLat, geoLong, segLat1, segLong1, segLat2, segLong2)
        return (False, False)                            # so going East from the point won't reach the line segment

    # The point is inside a bounding box crated by segLong1/SegLat1, segLong1/segLat2, segLong2/segLat2 and segLong2/segLat1
    # Compute the exact crossing point for an imaginary line going either East or West
    ratio = (segLat1 - geoLat) / (segLat1 - segLat2)    # How far along the line segment to get to geoLat
    logging.debug('Ration [%3.2f]', ratio)

    # Compute the longitude on the line segment at geoLat. Could be East or West of geoLong
    crossLong = segLong1 + ratio * (segLong2 - segLong1)
    if geoLong > crossLong:                                # The point is East of the crossing point
        logging.debug('geoLong[%.7f] is West of crossLong [%.7f]', geoLong, crossLong)
        return (False, False)                            # so going East from the point won't reach the line segment
    elif (ratio == 0.0) and isInflection:                # Start of line touches an inflection - so it isn't a crossing
        logging.debug('Inflection')
        return (False, False)
    elif crossLong == geoLong:                            # That longitude, on the line segment, is the point
        logging.debug('Point is on the line segment')
        return (False, True)                                # so this is an edge case
    # a imaginary line going East from point (geoLat, geoLong)
    # crosses the line segment (segLong1, segLat1) to (segLong2, segLat2)
    logging.debug('Crosses')
    return (True, False)


def findNearestPolygon(shapes, records, long, lat):
    '''
    Find the nearest polygon to this longitude and latitude
    '''
    # Find the nearest polygon to this point
    nearestDist = nearestI = None
    for ii, shape in enumerate(shapes):
        # Only check polygons
        if shape.shapeType != 5:        # Not a polygon
            continue
        parts = shape.parts
        # The last "part" can be the number of points - an end if list marker.
        if parts[-1] != len(shape.points):
            # If not, add the this extra dummy part - the end of list marker
            parts.append(len(shape.points))
        for part in range(len(parts) - 1):        # Don't analyse the dummy part
            point2 = list(shape.points[parts[part]])        # The first point
            p2Long = point2[0]
            p2Lat = point2[1]
            for jj in range(parts[part], parts[part + 1] - 1):
                # The last end is the new beginning
                p1Long = p2Long
                p1Lat = p2Lat
                # Get the new end
                point2 = list(shape.points[jj + 1])
                p2Long = point2[0]
                p2Lat = point2[1]
                # Calculate the length of the segment
                segLen = (p2Long - p1Long)**2 + (p2Lat - p1Lat)**2
                if segLen == 0:        # If zero then either end will do
                    dist = (long - p1Long)**2 + (lat - p1Lat)**2
                else:
                    # Calculate percentage along the segment where the perpendicular line crosses
                    u = ((long - p1Long) * (p2Long - p1Long) + (lat - p1Lat) * (p2Lat - p1Lat)) / segLen
                    # If off the end, then truncate to the end
                    u = min(max(u, 0.0), 1.0)
                    # Calculate the mid point and distance to that mid point
                    midLong = p1Long + u * (p2Long - p1Long)
                    midLat = p1Lat + u * (p2Lat - p1Lat)
                    dist = (long - midLong)**2 + (lat - midLat)**2
                if (nearestDist is None) or (dist < nearestDist):
                    nearestDist = dist
                    nearestI = ii
    if nearestI is not None:
        return records[nearestI][0]
    else:
        return None


def findPolygon(shapes, records, thisPostcode, thisLocality, long, lat):
    '''
    Find a polygon that contains this longitude and latitude
    '''
    # Find a polygon that contains this point
    # Each shape has a bounding box and a number of parts
    for ii, shape in enumerate(shapes):
        # Only check polygons
        if shape.shapeType != 5:        # Not a polygon
            continue
        # Check if this point is inside or outside this polygon's bounding box
        # Bounding Box is (bottom left, upper right) - check that this point is inside the bounding box
        if long < shape.bbox[0]:   # This point is more easterly than the polygon
            continue
        if long > shape.bbox[2]:   # This point is more westerly than the polygon
            continue
        if lat < shape.bbox[1]:    # This point is more southerly than the polygon
            continue
        if lat > shape.bbox[3]:    # This point is more northerly than the polygon
            continue
        logging.debug('Checking:%s', records[ii][0])
        # There may be multiple "rings" in this polygon
        # Basically sub-sets of point, which make up each set
        parts = shape.parts
        # The last "part" can be the number of points - an end if list marker.
        if parts[-1] != len(shape.points):
            # If not, add the this extra dummy part - the end of list marker
            parts.append(len(shape.points))
        for part in range(len(parts) - 1):        # Don't analyse the dummy part
            # Count the number of time an imaginary line going East from this point intersects a polygon line segment
            count = 0
            # There's one less line segment than there are polygon points
            # The end of the previous line segment is the start of the next line segment
            point2 = list(shape.points[parts[part]])        # The first point
            p2Long = point2[0]
            p2Lat = point2[1]
            # On the edge at the start is in, so if this is the point, then we are done
            if (long == p2Long) and (lat == p2Lat):
                logging.debug('Point for thisPostcode(%s), thisLocality(%s)[%.7f,%.7f] is the start of the first line segment',
                             thisPostcode, thisLocality, long, lat)
                return records[ii][0]
            crossings = []
            # Check each line segment (from point[jj] to point[jj + 1])
            logging.debug('Checking from %d to %d', parts[part], parts[part + 1] - 1)
            for jj in range(parts[part], parts[part + 1] - 1):
                # The last end is the new beginning
                p1Long = p2Long
                p1Lat = p2Lat
                # Get the new end
                point2 = list(shape.points[jj + 1])
                p2Long = point2[0]
                p2Lat = point2[1]
                # On the edge is in, so if the test point is the next point, then we are done
                if (long == p2Long) and (lat == p2Lat):
                    logging.debug('Point for thisPostcode(%s), thisLocality(%s)[%.7f,%.7f] is the end of a line segment',
                                 thisPostcode, thisLocality, long, lat)
                    return records[ii][0]

                # Don't count lines that will touch the end point - that would create double counting
                if p2Lat == lat:        # Don't count lines that will touch the end point - that would create double counting
                    continue

                # Check if the start point of this line segment is a vertical inflection in the geometry
                # Crossing a segment at the start of the segment, when the start is a North/South inflection point
                # isn't crossing in, or out, of the polygon
                # Check if the previous segment and this segment are a North/South inflection
                if jj == parts[part]:      # if this is the first segment then the previous segment is actually the last segment
                    # The polygon should be closed, in which case the previous segment start
                    l = parts[part + 1] - 2
                else:
                    l = jj - 1   # otherwise the previous segment starts one point back
                pointL = list(shape.points[l])
                plLat = pointL[1]
                plLong = pointL[0]
                logging.debug('Checking end inflection for [%.7f,%.7f],[%.7f,%.7f],[%.7f,%.7f]',
                               plLong, plLat, p1Long, p1Lat, p2Long, p2Lat)
                # Inflections require longitude to be sequential
                inflection = True
                if (plLong < p1Long) and (p1Long > p2Long):     # an angle pointing to the right
                    inflection = False
                if (plLong > p1Long) and (p1Long < p2Long):     # an angle pointing to the left
                    inflection = False
                if inflection:          # Look for an angle pointing up, or pointing down
                    # We have plLong < p1Long < p2Long OR plLong > p1Long > p2Long
                    if (plLat < p1Lat) and (p1Lat < p2Lat): # a slope down
                        inflection = False
                    if (plLat < p1Lat) and (p1Lat < p2Lat): # a slope up
                        inflection = False
                logging.debug('%s', repr(inflection))
                (crosses, isEdge) = checkCrossing(lat, long, p1Lat, p1Long, p2Lat, p2Long, inflection)
                if isEdge:            # On the line is in
                    return records[ii][0]
                if crosses:             # Crosses or is on the edge
                    count += 1          # Count the crossings
                    crossings.append([p1Long, p1Lat, p2Long, p2Lat])

            logging.debug('line from thisPostcode(%s), thisLocality(%s)[%.7f,%.7f] to the East crossed (%s) polygon line segments for %s',
                         thisPostcode, thisLocality, long, lat, count, records[ii][0])
            # If the imaginary line going East from this point intersects an even number of polygon line segments
            # then the point is outside the polygon.
            # Points inside the polygon must intersect an odd number of line segments
            if (count % 2) == 1:        # The point is inside this polygon
                return records[ii][0]
            else:                       # The point is inside the polygon bounding box, outside the polygon
                logging.debug('thisPostcode(%s), thisLocality(%s) is inside bounding box(%s)',
                             thisPostcode, thisLocality, repr(shape.bbox))
                logging.debug('but thisPostcode(%s), thisLocality(%s) crosses polygon (%s) times', thisPostcode, thisLocality, count)
                logging.debug('polygon(%s)', repr(shape.points[parts[part]:parts[part + 1]]))
                for jj, cross in enumerate(crossings):
                    logging.debug('crossings[%s]', repr(cross))

    # The point is not inside any of the polygon bounding boxes
    return None


multiSpace = re.compile(r'\s\s+')     # Collapse mutiple white space to a single space
spaceHyphenSpace = re.compile(r'\s+-\s+')   # Remove white space around the hyphen in hyphenated streets
spaceDash = re.compile(r'\s+-')
dashSpace = re.compile(r'-\s+')
dashEnd = re.compile(r'-$')

def cleanText(text, removeCommas):
    if text is not None:
        text = str(text).upper()
        text = text.translate(str.maketrans('', '', ":'"))
        if removeCommas:
            text = text.translate(str.maketrans('', '', ','))
        text = multiSpace.sub(' ', text)     # Collapse mutiple white space to a single space
        text = spaceHyphenSpace.sub('-', text)   # Remove white space around the hyphen in hyphenated streets
        text = spaceDash.sub('-', text)
        text = dashSpace.sub('-', text)
        text = dashEnd.sub('', text)
    return text


# The main code
if __name__ == '__main__':
    '''
A script to create extraFlats.psv, extraLevels.psv, extraTrims.psv,
extraStates.psv, extraStreetTypes.psv, extraStreetSuffixes.psv
extraPostcodeSA1LGA.psv and extraLocality.psv from getConfig.json
    '''

    # Get the script name (without the '.py' extension)
    progName = os.path.basename(sys.argv[0])
    progName = progName[0:-3]        # Strip off the .py ending

    # Define the command line options
    parser = argparse.ArgumentParser(prog=progName)
    parser.add_argument('-A', '--ABSdir', dest='ABSdir', default='../ABS',
                         help='The directory containing the ABS data (default="../ABS"')
    parser.add_argument('-v', '--verbose', dest='verbose', type=int, choices=list(range(0, 5)),
                        help='The level of logging\n\t0=CRITICAL,1=ERROR,2=WARNING,3=INFO,4=DEBUG')
    parser.add_argument('-L', '--logDir', dest='logDir', default='.', help='The name of a logging directory')
    parser.add_argument('-l', '--logfile', dest='logfile', default=None, help='The name of the logging file')

    # Parse the command line options
    args = parser.parse_args()
    ABSdir = args.ABSdir
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

    # Read in the configuration file - which must exist
    configFile = 'getExtras.json'    # The default configuration file
    config = {}                 # The configuration data
    try:
        with open(configFile, 'rt', newline='', encoding='utf-8') as configfile:
            config = json.load(configfile)
    except IOError:
        logging.critical('configFile (%s) failed to load', configFile)
        logging.shutdown()
        sys.exit(EX_CONFIG)

    # Create any extra FLATs, LEVELs or TRIMs
    trims = set()
    heading = ['code']
    if 'FLAT' in config:
        with open('extraFlats.psv', 'wt', newline='', encoding='utf-8') as flatFile:
            flatWriter = csv.writer(flatFile, dialect=csv.excel, delimiter='|')
            flatWriter.writerow(heading)
            for code in config['FLAT']:
                if code == '/* comment */':
                    continue
                if code == 'flat':
                    for flat in config['FLAT']['flat']:
                        flatWriter.writerow([flat])
    if 'LEVEL' in config:
        with open('extraLevels.psv', 'wt', newline='', encoding='utf-8') as levelFile:
            levelWriter = csv.writer(levelFile, dialect=csv.excel, delimiter='|')
            levelWriter.writerow(heading)
            for code in config['LEVEL']:
                if code == '/* comment */':
                    continue
                if code == 'level':
                    for level in config['LEVEL']['level']:
                        levelWriter.writerow([level])
    if 'TRIM' in config:
        with open('extraTrims.psv', 'wt', newline='', encoding='utf-8') as trimFile:
            trimWriter = csv.writer(trimFile, dialect=csv.excel, delimiter='|')
            trimWriter.writerow(heading)
            for code in config['TRIM']:
                if code == '/* comment */':
                    continue
                if code == 'trim':
                    for trim in config['TRIM']['trim']:
                        trimWriter.writerow([trim])

    heading = ['stateAbbrev', 'abbrev']
    if 'STATES' in config:
        with open('extraStates.psv', 'wt', newline='', encoding='utf-8') as stateFile:
            stateWriter = csv.writer(stateFile, dialect=csv.excel, delimiter='|')
            stateWriter.writerow(heading)
            for stateAbbrev in config['STATES']:
                if stateAbbrev == '/* comment */':
                    continue
                for abbrev in config['STATES'][stateAbbrev]:
                    stateWriter.writerow([stateAbbrev, abbrev])

    # Then the extra street types
    if 'STREET_TYPE' in config:
        heading = ['streetType', 'abbrev']
        with open('extraStreetTypes.psv', 'wt', newline='', encoding='utf-8') as streetTypeFile:
            streetTypeWriter = csv.writer(streetTypeFile, dialect=csv.excel, delimiter='|')
            streetTypeWriter.writerow(heading)
            for streetType, abbrevs in config['STREET_TYPE'].items():
                if streetType == '/* comment */':
                    continue
                for abbrev in abbrevs:
                    streetTypeWriter.writerow([streetType, abbrev])

    # Then the extra street Suffixes
    if 'STREET_SUFFIX' in config:
        heading = ['streetSuffix', 'abbrev']
        with open('extraStreetSuffixes.psv', 'wt', newline='', encoding='utf-8') as streetSuffixFile:
            streetSuffixWriter = csv.writer(streetSuffixFile, dialect=csv.excel, delimiter='|')
            streetSuffixWriter.writerow(heading)
            for streetSuffix, abbrevs in config['STREET_SUFFIX'].items():
                if streetSuffix == '/* comment */':
                    continue
                for abbrev in abbrevs:
                    streetSuffixWriter.writerow([streetSuffix, abbrev])


    if ('POSTCODE_SA1' in config) or ('LOCALITY_POSTCODE' in config):
        # We may have new data to enhance postcode_SA1LGA.psv and/or locality_SA1LGA.psv and/or locality.psv

        # Start by reading in the POLYGONS for each LGA areas
        LGAshp = open(os.path.join(ABSdir, 'LGA', 'LGA_2020_AUST.shp'), 'rb')
        LGAdbf = open(os.path.join(ABSdir, 'LGA', 'LGA_2020_AUST.dbf'), 'rb')
        LGAshx = open(os.path.join(ABSdir, 'LGA', 'LGA_2020_AUST.shx'), 'rb')
        LGAsf = shapefile.Reader(shp=LGAshp, dbf=LGAdbf, shx=LGAshx)
        LGAshapes = LGAsf.shapes()
        LGAfields = LGAsf.fields
        LGArecords = LGAsf.records()

        # Read in the Australia Post locality file
        postcodeSA1LGA = {}
        postcodes = {}
        # state_name|postcode|locality_name|SA1_MAINCODE_2016|LGA_CODE_2020|longitude|latitude
        with open('postcode_SA1LGA.psv', 'rt', newline='', encoding='utf-8') as postcodeFile:
            postcodeReader = csv.DictReader(postcodeFile, dialect=csv.excel, delimiter='|')
            for row in postcodeReader:
                localityName = row['locality_name']
                if localityName not in postcodeSA1LGA:
                    postcodeSA1LGA[localityName] = {}
                postcode = row['postcode']
                postcodeSA1LGA[localityName][postcode] = [row['SA1_MAINCODE_2016'], row['LGA_CODE_2020'], row['longitude'], row['latitude']]
                name = cleanText(localityName, True)
                SA1 = row['SA1_MAINCODE_2016']
                if postcode not in postcodes:
                    postcodes[postcode] = {}
                if SA1 not in postcodes[postcode]:
                    postcodes[postcode][SA1] = []
                postcodes[postcode][SA1].append(cleanText(row['locality_name'], True))

        # Read in the locality.psv file
        locality = {}
        # LOCALITY_PID|LOCALITY_NAME|PRIMARY_POSTCODE|STATE_PID|ALIAS
        with open('locality.psv', 'rt', newline='', encoding='utf-8') as localityFile :
            localityReader = csv.DictReader(localityFile, dialect=csv.excel, delimiter='|')
            for row in localityReader :
                localityPid = row['LOCALITY_PID']
                localityName = cleanText(row['LOCALITY_NAME'], True)
                postcode = row['PRIMARY_POSTCODE']
                if localityPid not in locality:
                    locality[localityPid] = {}
                if postcode not in locality[localityPid]:
                    locality[localityPid][postcode] = []
                locality[localityPid][postcode].append([row['LOCALITY_NAME'], row['STATE_PID']])

        # Read in the state.psv file
        state = {}
        # STATE_PID|STATE_NAME|STATE_ABBREVIATION
        with open('state.psv', 'rt', newline='', encoding='utf-8') as stateFile:
            stateReader = csv.DictReader(stateFile, dialect=csv.excel, delimiter='|')
            for row in stateReader:
                state[row['STATE_PID']] = cleanText(row['STATE_NAME'], True)

    extraLocalities = []                # LOCALITY_PID|LOCALITY_NAME|PRIMARY_POSTCODE|STATE_PID|ALIAS
    extraPostcodeSA1LGA = []            # state_name|postcode|locality_name|SA1_MAINCODE_2016|LGA_CODE_2020|longitude|latitude
    if 'POSTCODE_SA1' in config:
        for name in config['POSTCODE_SA1']:
            if name == '/* comment */':
                continue
            if 'locality' not in config['POSTCODE_SA1'][name]:
                continue
            suburb = config['POSTCODE_SA1'][name]['locality']
            if 'SA1' not in config['POSTCODE_SA1'][name]:
                continue
            SA1 = str(config['POSTCODE_SA1'][name]['SA1'])
            postcode = name
            if postcode in postcodeSA1LGA:
                if SA1 in postcode[postcode]:
                    if suburb in postcode[postcode][SA1]:
                        continue            # We have this data
            longCode = config['POSTCODE_SA1'][postcode]['longitude']
            latCode = config['POSTCODE_SA1'][postcode]['latitude']
            if longCode == '':
                continue
            if latCode == '':
                continue
            try:
                longitude = float(longCode)
            except ValueError:
                continue
            if longitude == 0:
                continue
            try:
                latitude = float(latCode)
            except ValueError:
                continue
            if latitude == 0:
                continue
            LGA = findPolygon(LGAshapes, LGArecords, postcode, locality, longitude, latitude)
            if LGA is None:
                LGA = findNearestPolygon(LGAshapes, LGArecords, longitude, latitude)
            if LGA is None:
                continue
            statePid = SA1[0:1]
            state_name = state[statePid]
            extraPostcodeSA1LGA.append([state_name, postcode, suburb, SA1, LGA, longCode, latCode])

    if 'LOCALITY_POSTCODE' in config:
        for name in config['LOCALITY_POSTCODE']:
            if name == '/* comment */':
                continue
            if name not in locality:        # Can't make up data, just add alternative postcodes and/or names
                continue
            localityPid = name
            for entry in config['LOCALITY_POSTCODE'][localityPid]:
                if 'postcode' in entry:
                    postcode = entry['postcode']
                else:
                    postcode = list(locality[localityPid])[0]     # Take the other data from here
                otherPostcode = list(locality[localityPid])[0]
                otherData = locality[localityPid][otherPostcode][0]
                if 'locality' in entry:
                    localityName = entry['locality']
                else:
                    localityName = otherData[0]
                statePid = otherData[1]
                extraLocalities.append([localityPid, localityName, postcode, statePid, 'A'])


    # Now output all the extra locality postcode data
    if len(extraPostcodeSA1LGA) > 0:
        # state_name|postcode|locality_name|SA1_MAINCODE_2016|LGA_CODE_2020|longitude|latitude
        csvOutfile = open('extraPostcodeSA1LGA.psv', 'wt', newline='', encoding='utf-8')
        csvwriter = csv.writer(csvOutfile, dialect=csv.excel, delimiter='|')
        heading = ['state_name', 'postcode', 'locality_name', 'SA1_MAINCODE_2016', 'LGA_CODE_2020', 'longitude', 'latitude']
        csvwriter.writerow(heading)
        for row in extraPostcodeSA1LGA:
            csvwriter.writerow(row)

    # Now output all the extra locality  data
    if len(extraLocalities) > 0:
        # LOCALITY_PID|LOCALITY_NAME|PRIMARY_POSTCODE|STATE_PID|ALIAS
        csvOutfile = open('extraLocality.psv', 'wt', newline='', encoding='utf-8')
        csvwriter = csv.writer(csvOutfile, dialect=csv.excel, delimiter='|')
        heading = ['locality_pid', 'locality_name', 'postcode', 'state_pid', 'alias']
        csvwriter.writerow(heading)
        for row in extraLocalities:
            csvwriter.writerow(row)

    logging.shutdown()
    sys.stdout.flush()
    sys.exit(1)
