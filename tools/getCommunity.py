#!/usr/bin/env python

# pylint: disable=line-too-long, pointless-string-statement, invalid-name

'''
A script to assign SA1 and LGA codes to every G-NAF community_pid


SYNOPSIS
$ python getCommunitySA1LGA.py [-A ABSdir|--ABSdir=ABSdir] [CommunitySA1LGAoutputFile]
                              [-v loggingLevel|--verbose=logingLevel]
                              [-L logDir|--logDir=logDir] [-l logfile|--logfile=logfile]

REQUIRED


OPTIONS
-A ABSdir|--ABSdir=ABSdir
The directory containing the ABS SA1 and LGA files

CommunitySA1LGAoutputFile
The name of the output file of community SA1 and LGA values to be created - default:community_SA1LGA.psv

-v loggingLevel|--verbose=loggingLevel
Set the level of logging that you want (defaut INFO).

-L logDir
The directory where the log file will be written (default='.')

-l logfile|--logfile=logfile
The name of a logging file where you want all messages captured (default=None)

This script reads the Indigenious Comunity, Postal Araes, SA1 and LGA polygons plus the G-NAF psv files of localities.
For each Indigenious Community, which isn't a G-NAF locality, it computes the centre point of the community.
It uses that to find the postcode, SA1 and LGA codes for this community.
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
    for ii, thisShape in enumerate(shapes):
        # Only check polygons
        if thisShape.shapeType != 5:        # Not a polygon
            continue
        theseParts = thisShape.parts
        # The last "part" can be the number of points - an end if list marker.
        if theseParts[-1] != len(thisShape.points):
            # If not, add the this extra dummy part - the end of list marker
            theseParts.append(len(thisShape.points))
        for thisPart in range(len(theseParts) - 1):        # Don't analyse the dummy part
            point2 = list(thisShape.points[theseParts[thisPart]])        # The first point
            p2Long = point2[0]
            p2Lat = point2[1]
            for j in range(theseParts[thisPart], theseParts[thisPart + 1] - 1):
                # The last end is the new beginning
                p1Long = p2Long
                p1Lat = p2Lat
                # Get the new end
                point2 = list(thisShape.points[j + 1])
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


def findPolygon(shapes, records, loc_pid, long, lat):
    '''
Find a polygon that contains this long and lat
    '''
    # Find a polygon that contains this point
    # Every point is "inside" only one polygon, but a polygon can be inside another polygon (donut effect)
    # Each shape has a bounding box and a number of parts
    foundII = None
    foundShape = None
    for ii, thisShape in enumerate(shapes):
        # Only check polygons
        if thisShape.shapeType != 5:        # Not a polygon
            continue
        # Check if this point is inside or outside this polygon's bounding box
        # Bounding Box is (bottom left, upper right) - check that this point is inside the bounding box
        if long < thisShape.bbox[0]:   # This point is more easterly than the polygon
            continue
        if long > thisShape.bbox[2]:   # This point is more westerly than the polygon
            continue
        if lat < thisShape.bbox[1]:    # This point is more southerly than the polygon
            continue
        if lat > thisShape.bbox[3]:    # This point is more northerly than the polygon
            continue
        if foundII is not None:     # Check if this polygon surrounds the found polygon
            if (foundShape.bbox[0] > shape.bbox[0]) and (foundShape.bbox[2] < shape.bbox[2]):
                continue
        logging.debug('Checking:%s', records[i][0])
        # There may be multiple "rings" in this polygon
        # Basically sub-sets of point, which make up each set
        theseParts = thisShape.parts
        # The last "part" can be the number of points - an end if list marker.
        if theseParts[-1] != len(thisShape.points):
            # If not, add the this extra dummy part - the end of list marker
            theseParts.append(len(thisShape.points))
        for thisPart in range(len(theseParts) - 1):        # Don't analyse the dummy part
            # Count the number of time an imaginary line going East from this point intersects a polygon line segment
            count = 0
            # There's one less line segment than there are polygon points
            # The end of the previous line segment is the start of the next line segment
            point2 = list(thisShape.points[theseParts[thisPart]])        # The first point
            p2Long = point2[0]
            p2Lat = point2[1]
            # On the edge at the start is in, so if this is the point, then we are done
            if (long == p2Long) and (lat == p2Lat):
                logging.debug('Point for loc_pid(%s)[%.7f,%.7f] is the start of the first line segment',
                             loc_pid, long, lat)
                foundII = ii
                foundShape = shape
                break
            crossings = []
            # Check each line segment (from point[j] to point[j + 1])
            logging.debug('Checking from %d to %d', theseParts[thisPart], theseParts[thisPart + 1] - 1)
            for j in range(theseParts[thisPart], theseParts[thisPart + 1] - 1):
                # The last end is the new beginning
                p1Long = p2Long
                p1Lat = p2Lat
                # Get the new end
                point2 = list(thisShape.points[j + 1])
                p2Long = point2[0]
                p2Lat = point2[1]
                # On the edge is in, so if the test point is the next point, then we are done
                if (long == p2Long) and (lat == p2Lat):
                    logging.debug('Point for loc_pid(%s)[%.7f,%.7f] is the end of a line segment',
                                 loc_pid, long, lat)
                    foundII = ii
                    foundShape = shape
                    break

                # Don't count lines that will touch the end point - that would create double counting
                if p2Lat == lat:        # Don't count lines that will touch the end point - that would create double counting
                    continue

                # Check if the start point of this line segment is a vertical inflection in the geometry
                # Crossing a segment at the start of the segment, when the start is a North/South inflection point
                # isn't crossing in, or out, of the polygon
                # Check if the previous segment and this segment are a North/South inflection
                if j == theseParts[thisPart]:      # if this is the first segment then the previous segment is actually the last segment
                    # The polygon should be closed, in which case the previous segment start
                    l = theseParts[thisPart + 1] - 2
                else:
                    l = j - 1   # otherwise the previous segment starts one point back
                pointL = list(thisShape.points[l])
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
                    foundII = ii
                    foundShape = shape
                    break
                if crosses:             # Crosses or is on the edge
                    count += 1          # Count the crossings
                    crossings.append([p1Long, p1Lat, p2Long, p2Lat])

            else:
                logging.debug('line from loc_pid(%s)[%.7f,%.7f] to the East crossed (%s) polygon line segments for %s',
                             loc_pid, long, lat, count, records[i][0])
                # If the imaginary line going East from this point intersects an even number of polygon line segments
                # then the point is outside the polygon.
                # Points inside the polygon must intersect an odd number of line segments
                if (count % 2) == 1:        # The point is inside this polygon
                    foundII = ii
                    foundShape = shape
                    break
                else:                       # The point is inside the polygon bounding box, outside the polygon
                    logging.debug('loc_pid(%s) is inside bounding box(%s)',
                                 loc_pid, repr(thisShape.bbox))
                    logging.debug('but loc_pid(%s) crosses polygon (%s) times', loc_pid, count)
                    logging.debug('polygon(%s)', repr(thisShape.points[theseParts[thisPart]:theseParts[thisPart + 1]]))
                    for j, cross in enumerate(crossings):
                        logging.debug('crossings[%s]', repr(cross))

    if foundII is not None:
        return records[foundII][0]
    else:
        # The point is not inside any of the polygon bounding boxes
        return None


# The main code
if __name__ == '__main__':
    '''
A script to assign postcode, SA1 and LGA codes to every Indigenious Community (which isn't already a G-NAF locality)
Start by reading all the polygons for all the Postal Areas, SA1 areas and LGA areas.
Then read in all the G-NAF localities before reading in the Indigenious Communities.
Then by read all the polygons for all the LGA areas
Each Indigenious Community PID (ILOC), compute the average longitude and latitude.
Finally find the Postal Areas, SA1 and LGA polygons that bound this point.
    '''

    # Get the script name (without the '.py' extension)
    progName = os.path.basename(sys.argv[0])
    progName = progName[0:-3]        # Strip off the .py ending

    # Define the command line options
    parser = argparse.ArgumentParser(prog=progName)
    parser.add_argument('-A', '--ABSdir', dest='ABSdir', default='../ABS',
                        help='The name of the directory containing the ABS SA1 and LGA files - default ../ABS')
    parser.add_argument ('CommunitySA1LGAoutputFile', nargs='?', default='community_SA1LGA.psv',
                         help='The name of the output file of community SA1 and LGA data to be created. (default community_SA1LGA.psv)')
    parser.add_argument('-v', '--verbose', dest='verbose', type=int, choices=list(range(0, 5)),
                        help='The level of logging\n\t0=CRITICAL,1=ERROR,2=WARNING,3=INFO,4=DEBUG')
    parser.add_argument('-L', '--logDir', dest='logDir', default='.', help='The name of a logging directory')
    parser.add_argument('-l', '--logfile', dest='logfile', default=None, help='The name of the logging file')

    # Parse the command line options
    args = parser.parse_args()
    ABSdir = args.ABSdir
    CommunitySA1LGAoutputFile = args.CommunitySA1LGAoutputFile
    loggingLevel = args.verbose
    logDir = args.logDir
    logfile = args.logfile

    # Set up logging
    logging_levels = {0: logging.CRITICAL, 1: logging.ERROR, 2: logging.WARNING, 3: logging.INFO, 4: logging.DEBUG}
    logfmt = progName + ' [%(asctime)s]: %(message)s'
    if loggingLevel:    # Change the logging level from "WARN" if the -v vebose option is specified
        if logfile:        # and send it to a file if the -o logfile option is specified
            logging.basicConfig(format=logfmt, datefmt='%d/%m/%y %H:%M:%S %p',
                                level=logging_levels[loggingLevel], filemode='w', filename=os.path.join(logDir, logfile))
        else:
            logging.basicConfig(format=logfmt, datefmt='%d/%m/%y %H:%M:%S %p', level=logging_levels[loggingLevel])
    else:
        if logfile:        # send the default(WARN) logging to a file if the -o logfile option is specified
            logging.basicConfig(format=logfmt, datefmt='%d/%m/%y %H:%M:%S %p', filemode='w', filename=os.path.join(logDir, logfile))
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
    communitySA1LGAfile =  open(CommunitySA1LGAoutputFile, 'wt', newline='', encoding='utf-8')
    communitySA1LGAwriter = csv.writer(communitySA1LGAfile, dialect=csv.excel, delimiter='|')
    outRow = ['community_pid', 'community_name', 'state_pid', 'Postcode', 'SA1_MAINCODE_2016', 'LGA_CODE_2020', 'longitude', 'latitude']
    communitySA1LGAwriter.writerow(outRow)

    # Next read in all the Indigenous Communities
    ILOCshp = open(os.path.join(ABSdir, 'ILOC', 'ILOC_2021_AUST_GDA2020.shp'), 'rb')
    ILOCdbf = open(os.path.join(ABSdir, 'ILOC', 'ILOC_2021_AUST_GDA2020.dbf'), 'rb')
    ILOCshx = open(os.path.join(ABSdir, 'ILOC', 'ILOC_2021_AUST_GDA2020.shx'), 'rb')
    ILOCsf = shapefile.Reader(shp=ILOCshp, dbf=ILOCdbf, shx=ILOCshx)
    ILOCshapes = ILOCsf.shapes()
    ILOCfields = ILOCsf.fields
    ILOCrecords = ILOCsf.records()

    # Collect the set of primary names (names with their own polygon)
    primaryNames = set()
    for thisRecord, record in enumerate(ILOCrecords):
        community_pid = record.as_dict()['ILO_CODE21']
        name = record.as_dict()['ILO_NAME21'].upper()
        state_pid = record.as_dict()['STE_CODE21']
        if name.startswith('MIGRATORY'):
            continue
        if name.startswith('OUTSIDE AUSTRALIA'):
            continue
        if name.startswith('NO USUAL'):
            continue
        excludes = name.find(' EXC.')
        if excludes != -1:
            name = name[:excludes].strip()
        if name.endswith('CAMPS'):
            name = name[:-1]
        if name.endswith(' (QLD)'):
            name = name[:-6]
        if name.endswith(' (VIC.)'):
            name = name[:-7]
        if name.endswith(' (VIC)'):
            name = name[:-6]
        if name.endswith(' (TAS.)'):
            name = name[:-7]
        if name.endswith(' (TAS)'):
            name = name[:-6]
        names = name.split(' - ')
        if len(names) == 1:
            primaryNames.add(name)

    # Process all polygons
    for thisRecord, record in enumerate(ILOCrecords):
        community_pid = record.as_dict()['ILO_CODE21']
        name = record.as_dict()['ILO_NAME21'].upper()
        state_pid = record.as_dict()['STE_CODE21']
        if name.startswith('MIGRATORY'):
            continue
        if name.startswith('OUTSIDE AUSTRALIA'):
            continue
        if name.startswith('NO USUAL'):
            continue
        excludes = name.find(' EXC.')
        if excludes != -1:
            name = name[:excludes].strip()
        if name.endswith('CAMPS'):
            name = name[:-1]
        if name.endswith(' (QLD)'):
            name = name[:-6]
        if name.endswith(' (VIC.)'):
            name = name[:-7]
        if name.endswith(' (VIC)'):
            name = name[:-6]
        if name.endswith(' (TAS.)'):
            name = name[:-7]
        if name.endswith(' (TAS)'):
            name = name[:-6]

        # Look for alternate names
        names = name.split(' - ')
        extraNames = []
        trim = ['NORTH', 'NORTH-EAST', 'EAST', 'SOUTH-EAST', 'SOUTH', 'SOUTH-WEST', 'WEST', 'NORTH-WEST']
        trim += ['INNER', 'INNER NORTH', 'INNER NORTH-EAST', 'INNER EAST', 'INNER SOUTH-EAST', 'INNER SOUTH', 'INNER SOUTH-WEST', 'INNER WEST', 'INNER NORTH-WEST']
        trim += ['INNER CITY', 'INNER HOMELANDS']
        trim += ['OUTER', 'OUTER NORTH', 'OUTER NORTH-EAST', 'OUTER EAST', 'OUTER SOUTH-EAST', 'OUTER SOUTH', 'OUTER SOUTH-WEST', 'OUTER WEST', 'OUTER NORTH-WEST']
        trim += ['OUTER HOMELANDS']
        trim += ['CENTRAL', 'CENTRAL NORTH', 'CENTRAL NORTH-EAST', 'CENTRAL EAST', 'CENTRAL SOUTH-EAST', 'CENTRAL SOUTH', 'CENTRAL SOUTH-WEST', 'CENTRAL WEST', 'CENTRAL NORTH-WEST']
        trim += ['COAST', 'NORTH COAST', 'NORTH-EAST COAST', 'EAST COAST', 'SOUTH-EAST COAST', 'SOUTH COAST', 'SOUTH-WEST COAST', 'WEST COAST', 'NORTH-WEST COAST']
        trim += ['SOUTHERN HINTERLANDS', 'NORTHERN BEACHES', 'SOUTHERN RANGELANDS', 'OUTSTATIONS', 'VILLAGE CAMP']
        trim += ['SURROUNDS']

        # Check if a name was "name" - trim
        # and got split. If so rejoin and add two alternates
        for i in range(len(names) - 1, -1, -1):
            if i > 0:
                thisName = names[i].strip()
                # If the next name is trouble, then just add it to the previous name
                try:
                    trouble = trim.index(thisName)
                except ValueError as e:
                    trouble = None
                if trouble is not None:
                    names[i - 1] = names[i - 1].strip()
                    extraNames.append(names[i - 1] + ' (' + thisName + ')')
                    extraNames.append(names[i - 1] + ' ' + thisName)
                    names[i - 1] += ' - ' + thisName
                    del names[i]

        # Check if name has an alternate name or trim in ()
        for i, thisName in enumerate(names):
            names[i] = thisName.strip()
            # Check for an alternates at the end
            if names[i].endswith(')'):
                altStart = names[i].find('(')       # An alternate name in ()
                if altStart != -1:
                    alt1 = names[i][:altStart].strip()
                    alt2 = names[i][altStart + 1:-1].strip()
                    # Could be trim in brackets
                    try:
                        trouble = trim.index(alt2)
                    except ValueError as e:
                        trouble = None
                    if trouble is not None:     # trim in brackets
                        extraNames.append(alt1 + ' - ' + alt2)
                        extraNames.append(alt1 + ' ' + alt2)
                    else:
                        names[i] = alt1
                        alt2 = alt2.replace('HOMELANDS', 'HOMELAND')
                        alt2 = alt2.replace('ISLANDS', 'ISLAND')
                        extraNames.append(alt2)
            else:       # Look for an alternate name in the middle
                altStart = names[i].find('(')
                altEnd = names[i].find(')')
                if (altStart != -1) and (altEnd != -1) and (altStart < altEnd):
                    alt1 = names[i][:altStart].strip()
                    alt2 = names[i][altStart + 1:altEnd].strip()
                    alt3 = names[i][altEnd + 1:].strip()
                    # Could be trim in brackets
                    try:
                        trouble = trim.index(alt2)
                    except ValueError as e:
                        trouble = None
                    if trouble is not None:     # trim in brackets
                        extraNames.append(alt1 + ' - ' + alt2 + ' ' + alt3)
                        extraNames.append(alt1 + ' (' + alt2 + ') ' + alt3)
                    else:
                        alt3 = alt3.replace('HOMELANDS', 'HOMELAND')
                        alt3 = alt3.replace('ISLANDS', 'ISLAND')
                        extraNames.append(alt1 + ' ' + alt3)
                        extraNames.append(alt2 + ' ' + alt3)

        todoNames = names
        for thisName in extraNames:
            if thisName in primaryNames:
                continue
            todoNames.append(thisName)

        if len(todoNames) > 0:

            # Find this location and related data
            shape = ILOCshapes[thisRecord]
            longitude = (shape.bbox[0] + shape.bbox[2]) / 2.0
            latitude = (shape.bbox[1] + shape.bbox[3]) / 2.0


            # Find the polygons that contains this point
            POA = findPolygon(POAshapes, POArecords, community_pid, longitude, latitude)
            if POA is None:
                logging.warning('community_pid(%s)[%.7f,%.7f] is not inside any POA polygon - looking for nearest polygon',
                                community_pid, latitude, longitude)
                POA = findNearestPolygon(POAshapes, POArecords, longitude, latitude)
            if POA is None:
                logging.warning('community_pid(%s)[%s,%s] is not inside any POA polygon bounding box',
                                community_pid, latitude, longitude)
            SA1 = findPolygon(SA1shapes, SA1records, community_pid, longitude, latitude)
            if SA1 is None:
                logging.warning('community_pid(%s)[%.7f,%.7f] is not inside any SA1 polygon - looking for nearest polygon',
                                community_pid, latitude, longitude)
                SA1 = findNearestPolygon(SA1shapes, SA1records, longitude, latitude)
            if SA1 is None:
                logging.warning('community_pid(%s)[%s,%s] is not inside any SA1 polygon bounding box',
                                community_pid, latitude, longitude)
            LGA = findPolygon(LGAshapes, LGArecords, community_pid, longitude, latitude)
            if LGA is None:
                logging.warning('community_pid(%s)[%.7f,%.7f] is not inside any LGA polygon - looking for nearest polygon',
                                community_pid, latitude, longitude)
                LGA = findNearestPolygon(LGAshapes, LGArecords, longitude, latitude)
            if LGA is None:
                logging.warning('community_pid(%s)[%s,%s] is not inside any LGA polygon bounding box',
                                community_pid, latitude, longitude)

            for thisName in todoNames:
                if (POA is not None) or (SA1 is not None) or (LGA is not None):
                    logging.info('Found community_pid(%s:%s:%s)[%s,%s], POA(%s), SA1(%s), LGA(%s)', community_pid, state_pid, name, longitude, latitude, POA, SA1, LGA)
                    outRow = ['ILOC-' + community_pid, thisName, state_pid, POA, SA1, LGA, longitude, latitude]
                    communitySA1LGAwriter.writerow(outRow)
                # coded.add(thisName)

    communitySA1LGAfile.close()

    logging.shutdown()
    sys.stdout.flush()
    sys.exit(EX_OK)
