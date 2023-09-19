#!/usr/bin/env python

# pylint: disable=line-too-long, pointless-string-statement, invalid-name

'''
A script to assign SA1 and LGA codes to every G-NAF locality_pid


SYNOPSIS
$ python getLocalitySA1LGA.py [-G GNAFdir|--GNAFdir=GNAFdir] [-A ABSdir|--ABSdir=ABSdir] [LocalitySA1LGAoutputFile]
                              [-v loggingLevel|--verbose=logingLevel]
                              [-L logDir|--logDir=logDir] [-l logfile|--logfile=logfile]

REQUIRED


OPTIONS
-G GNAFdir|--GNAFdir=GNAFdir
The directory containing the G-NAF psv files - default ../G-NAF

-A ABSdir|--ABSdir=ABSdir
The directory containing the ABS SA1 and LGA files

LocalitySA1LGAoutputFile
The name of the output file of locality SA1 and LGA values to be created - default:locality_SA1LGA.psv

-v loggingLevel|--verbose=loggingLevel
Set the level of logging that you want (defaut INFO).

-L logDir
The directory where the log file will be written (default='.')

-l logfile|--logfile=logfile
The name of a logging file where you want all messages captured (default=None)

This script reads the SA1 and LGA polygons plus the G-NAF psv files of geocode locality information (*_LOCALITY_POINT_psv.psv)
Each G-NAF locality_pid has a latitude and longitude. This script finds the SA1 polygon and LGA polygons that bound this point.
'''

# Import all the modules that make life easy
import sys
import os
import argparse
import logging
import csv
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
Find the nearest polygon to this long and lat
    '''
    # Find the nearest polygon to this point
    nearestDist = nearestI = None
    for i, shape in enumerate(shapes):
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
            for j in range(parts[part], parts[part + 1] - 1):
                # The last end is the new beginning
                p1Long = p2Long
                p1Lat = p2Lat
                # Get the new end
                point2 = list(shape.points[j + 1])
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
                    nearestI = i
    if nearestI is not None:
        return records[nearestI][0]
    else:
        return None


def findPolygon(shapes, records, loc_pid, long, lat):
    '''
Find a polygon that contains this long and lat
    '''
    # Find a polygon that contains this point
    # Each shape has a bounding box and a number of parts
    for i, shape in enumerate(shapes):
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
        logging.debug('Checking:%s', records[i][0])
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
                logging.debug('Point for loc_pid(%s)[%.7f,%.7f] is the start of the first line segment',
                             loc_pid, long, lat)
                return records[i][0]
            crossings = []
            # Check each line segment (from point[j] to point[j + 1])
            logging.debug('Checking from %d to %d', parts[part], parts[part + 1] - 1)
            for j in range(parts[part], parts[part + 1] - 1):
                # The last end is the new beginning
                p1Long = p2Long
                p1Lat = p2Lat
                # Get the new end
                point2 = list(shape.points[j + 1])
                p2Long = point2[0]
                p2Lat = point2[1]
                # On the edge is in, so if the test point is the next point, then we are done
                if (long == p2Long) and (lat == p2Lat):
                    logging.debug('Point for loc_pid(%s)[%.7f,%.7f] is the end of a line segment',
                                 loc_pid, long, lat)
                    return records[i][0]

                # Don't count lines that will touch the end point - that would create double counting
                if p2Lat == lat:        # Don't count lines that will touch the end point - that would create double counting
                    continue

                # Check if the start point of this line segment is a vertical inflection in the geometry
                # Crossing a segment at the start of the segment, when the start is a North/South inflection point
                # isn't crossing in, or out, of the polygon
                # Check if the previous segment and this segment are a North/South inflection
                if j == parts[part]:      # if this is the first segment then the previous segment is actually the last segment
                    # The polygon should be closed, in which case the previous segment start
                    l = parts[part + 1] - 2
                else:
                    l = j - 1   # otherwise the previous segment starts one point back
                pointL = list(shape.points[l])
                plLat = pointL[1]
                plLong = pointL[0]
                logging.debug('Checking end inflection for [%.7f,%.7f],[%.7f,%.7f],[%.7f,%.7f]',
                               plLong, plLat, p1Long, p1Lat, p2Long, p2Lat)
                # Inflections require long to be sequential
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
                    return records[i][0]
                if crosses:             # Crosses or is on the edge
                    count += 1          # Count the crossings
                    crossings.append([p1Long, p1Lat, p2Long, p2Lat])

            logging.debug('line from loc_pid(%s)[%.7f,%.7f] to the East crossed (%s) polygon line segments for %s',
                         loc_pid, long, lat, count, records[i][0])
            # If the imaginary line going East from this point intersects an even number of polygon line segments
            # then the point is outside the polygon.
            # Points inside the polygon must intersect an odd number of line segments
            if (count % 2) == 1:        # The point is inside this polygon
                return records[i][0]
            else:                       # The point is inside the polygon bounding box, outside the polygon
                logging.debug('loc_pid(%s) is inside bounding box(%s)',
                             loc_pid, repr(shape.bbox))
                logging.debug('but loc_pid(%s) crosses polygon (%s) times', loc_pid, count)
                logging.debug('polygon(%s)', repr(shape.points[parts[part]:parts[part + 1]]))
                for j, cross in enumerate(crossings):
                    logging.debug('crossings[%s]', repr(cross))

    # The point is not inside any of the polygon bounding boxes
    return None


# The main code
if __name__ == '__main__':
    '''
A script to assign SA1 and LGA codes to every G-NAF locality_pid
Start by reading all the polygons for all the SA1 areas
Then by read all the polygons for all the LGA areas
Then reads the G-NAF psv file (*_LOCALITY_POINT_psv.psv)
Each G-NAF locality_pid has a latitude and longitude.
This script finds the SA1 and LGA polygons that bound this point.
    '''

    # Get the script name (without the '.py' extension)
    progName = os.path.basename(sys.argv[0])
    progName = progName[0:-3]        # Strip off the .py ending

    # Define the command line options
    parser = argparse.ArgumentParser(prog=progName)
    parser.add_argument('-G', '--GNAFdir', dest='GNAFdir', default='../G-NAF',
                        help='The name of the directory containing the G-NAF psv files - default ../G-NAF')
    parser.add_argument('-A', '--ABSdir', dest='ABSdir', default='../ABS',
                        help='The name of the directory containing the ABS SA1 and LGA files - default ../ABS')
    parser.add_argument ('LocalitySA1LGAoutputFile', nargs='?', default='locality_SA1LGA.psv',
                         help='The name of the output file of locality SA1 and LGA data to be created. (default locality_SA1LGA.psv)')
    parser.add_argument('-v', '--verbose', dest='verbose', type=int, choices=list(range(0, 5)),
                        help='The level of logging\n\t0=CRITICAL,1=ERROR,2=WARNING,3=INFO,4=DEBUG')
    parser.add_argument('-L', '--logDir', dest='logDir', default='.', help='The name of a logging directory')
    parser.add_argument('-l', '--logfile', dest='logfile', default=None, help='The name of the logging file')

    # Parse the command line options
    args = parser.parse_args()
    GNAFdir = args.GNAFdir
    ABSdir = args.ABSdir
    LocalitySA1LGAoutputFile = args.LocalitySA1LGAoutputFile
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

    # Start by reading in the POLYGONS for each POA area
    POAshp = open(os.path.join(ABSdir, 'PostalAreas', 'POA_2021_AUST_GDA2020.shp'), 'rb')
    POAdbf = open(os.path.join(ABSdir, 'PostalAreas', 'POA_2021_AUST_GDA2020.dbf'), 'rb')
    POAshx = open(os.path.join(ABSdir, 'PostalAreas', 'POA_2021_AUST_GDA2020.shx'), 'rb')
    POAsf = shapefile.Reader(shp=POAshp, dbf=POAdbf, shx=POAshx)
    POAshapes = POAsf.shapes()
    POAfields = POAsf.fields
    POArecords = POAsf.records()

    # Then read in the POLYGONS for each SA1 area
    SA1shp = open(os.path.join(ABSdir, 'SA1', 'SA1_2016_AUST.shp'), 'rb')
    SA1dbf = open(os.path.join(ABSdir, 'SA1', 'SA1_2016_AUST.dbf'), 'rb')
    SA1shx = open(os.path.join(ABSdir, 'SA1', 'SA1_2016_AUST.shx'), 'rb')
    SA1sf = shapefile.Reader(shp=SA1shp, dbf=SA1dbf, shx=SA1shx)
    SA1shapes = SA1sf.shapes()
    SA1fields = SA1sf.fields
    SA1records = SA1sf.records()

    # Then read in the POLYGONS for each LGA area
    LGAshp = open(os.path.join(ABSdir, 'LGA', 'LGA_2020_AUST.shp'), 'rb')
    LGAdbf = open(os.path.join(ABSdir, 'LGA', 'LGA_2020_AUST.dbf'), 'rb')
    LGAshx = open(os.path.join(ABSdir, 'LGA', 'LGA_2020_AUST.shx'), 'rb')
    LGAsf = shapefile.Reader(shp=LGAshp, dbf=LGAdbf, shx=LGAshx)
    LGAshapes = LGAsf.shapes()
    LGAfields = LGAsf.fields
    LGArecords = LGAsf.records()

    # Open the output file
    localitySA1LGAfile =  open(LocalitySA1LGAoutputFile, 'wt', newline='', encoding='utf-8')
    localitySA1LGAwriter = csv.writer(localitySA1LGAfile, dialect=csv.excel, delimiter='|')
    outRow = ['locality_pid', 'Postcode', 'SA1_MAINCODE_2016', 'LGA_CODE_2020', 'longitude', 'latitude']
    localitySA1LGAwriter.writerow(outRow)

    # Next read in all the locality GPS details
    # LOCALITY_POINT_PID|DATE_CREATED|DATE_RETIRED|LOCALITY_PID|PLANIMETRIC_ACCURACY|LONGITUDE|LATITUDE
    for SandT in ['ACT', 'NSW', 'NT', 'OT', 'QLD', 'SA', 'TAS', 'VIC', 'WA']:
        localityfile = os.path.join(GNAFdir, 'Standard', SandT + '_LOCALITY_POINT_psv.psv')
        with open(localityfile, 'rt', newline='', encoding='utf-8') as localityFile:
            localityReader = csv.DictReader(localityFile, dialect=csv.excel, delimiter='|')
            for row in localityReader:
                if row['DATE_RETIRED'] != '':        # Skip if retired
                    continue
                locality_pid = row['LOCALITY_PID']
                longCode = row['LONGITUDE']
                latCode = row['LATITUDE']
                logging.debug('Checking locality_pid(%s:%s,%s)', locality_pid, longCode, latCode)
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

                # Find the polygons that contains this point
                POA = findPolygon(POAshapes, POArecords, locality_pid, longitude, latitude)
                if POA is None:
                    logging.warning('locality_pid(%s)[%.7f,%.7f] is not inside any POA polygon - looking for nearest polygon',
                                    locality_pid, latitude, longitude)
                    POA = findNearestPolygon(POAshapes, POArecords, longitude, latitude)
                SA1 = findPolygon(SA1shapes, SA1records, locality_pid, longitude, latitude)
                if SA1 is None:
                    logging.warning('locality_pid(%s)[%.7f,%.7f] is not inside any SA1 polygon - looking for nearest polygon',
                                    locality_pid, latitude, longitude)
                    SA1 = findNearestPolygon(SA1shapes, SA1records, longitude, latitude)
                if SA1 is None:
                    logging.warning('locality_pid(%s)[%s,%s] is not inside any SA1 polygon bounding box',
                                    locality_pid, latCode, longCode)
                LGA = findPolygon(LGAshapes, LGArecords, locality_pid, longitude, latitude)
                if LGA is None:
                    logging.warning('locality_pid(%s)[%.7f,%.7f] is not inside any LGA polygon - looking for nearest polygon',
                                    locality_pid, latitude, longitude)
                    LGA = findNearestPolygon(LGAshapes, LGArecords, longitude, latitude)
                if LGA is None:
                    logging.warning('locality_pid(%s)[%s,%s] is not inside any LGA polygon bounding box',
                                    locality_pid, latCode, longCode)
                if (POA is not None) or (SA1 is not None) or (LGA is not None):
                    logging.debug('Found locality_pid(%s)[%s,%s], SA1(%s), LGA(%s)', locality_pid, longCode, latCode, SA1, LGA)
                    outRow = [locality_pid, POA, SA1, LGA, longitude, latitude]
                    localitySA1LGAwriter.writerow(outRow)

    localitySA1LGAfile.close()

    logging.shutdown()
    sys.stdout.flush()
    sys.exit(EX_OK)
