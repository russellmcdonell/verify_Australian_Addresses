#!/usr/bin/env python

# pylint: disable=unspecified-encoding, broad-exception-caught, line-too-long, invalid-name, pointless-string-statement

'''
A python script to load G-NAF database tables with the G-NAF data using SQLAlchemy definitions
SYNOPSIS
$ python loadSQLAlchemyDB.py 
                         [-D databaseType|--databaseType=databaseType]
                         [-G GNAFdir|--GNAFdir=GNAFdir]
                         [-u username|--username=username] [-p password|--password=password]
                         [-s Server|--Server=Server] [-d databaseName|--databaseName=databaseName]
                         [-v loggingLevel|--verbose=logingLevel] [-L logDir|--logDir=logDir] [-l logfile|--logfile=logfile]

REQUIRED
-D databaseType|--databaseType=databaseType
The type of database [eg:MSSQL/MySQL]

-G GNAFdir|--GNAFdir=GNAFdir
The directory containing the 'Authority Code' and 'Standard' directories, where all the G-NAF files will be found


OPTIONS
-u userName|--userName=userName]
The user name require to access the database

-p password|--password=password]
The password require to access the database

-s server|--server=server]
The address of the database server

-d databaseName|--databaseName=databaseName]
The name of the database

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
import collections
import json
import decimal
import datetime
import pandas as pd
from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError
from sqlalchemy_utils import database_exists
import defineSQLAlchemyDB as dbConfig

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
    Load the tables in a database with the G-NAF data
    '''

    # Get the script name (without the '.py' extension)
    progName = os.path.basename(sys.argv[0])
    progName = progName[0:-3]        # Strip off the .py ending

    # Define the command line options
    parser = argparse.ArgumentParser(prog=progName)
    parser.add_argument('-D', '--databaseType', dest='databaseType', required=True, help='The database Type [e.g.: MSSQL/MySQL]')
    parser.add_argument('-G', '--GNAFdir', dest='GNAFdir', default='../..', required=True,
                        help='The directory containing the "Authority Code" and "Standard" G-NAF folders')
    parser.add_argument('-u', '--username', dest='username', help='The user required to access the database')
    parser.add_argument('-p', '--password', dest='password', help='The user password required to access the database')
    parser.add_argument('-s', '--server', dest='server', help='The address of the database server')
    parser.add_argument('-d', '--databaseName', dest='databaseName', help='The name of the database')
    parser.add_argument('-v', '--verbose', dest='verbose', type=int, choices=list(range(0, 5)),
                        help='The level of logging\n\t0=CRITICAL,1=ERROR,2=WARNING,3=INFO,4=DEBUG')
    parser.add_argument('-L', '--logDir', dest='logDir', default='.', help='The name of a logging directory')
    parser.add_argument('-l', '--logFile', dest='logFile', default=None, help='The name of the logging file')
    parser.add_argument('args', nargs=argparse.REMAINDER)

    # Parse the command line options
    args = parser.parse_args()
    databaseType = args.databaseType
    GNAFdir = args.GNAFdir
    username = args.username
    password = args.password
    server = args.server
    databaseName = args.databaseName
    logDir = args.logDir
    logFile = args.logFile
    loggingLevel = args.verbose

    # Set up logging
    logging_levels = {0:logging.CRITICAL, 1:logging.ERROR, 2:logging.WARNING, 3:logging.INFO, 4:logging.DEBUG}
    logfmt = progName + ' [%(asctime)s]: %(message)s'
    if loggingLevel and (loggingLevel not in logging_levels) :
        sys.stderr.write(f'Error - invalid logging verbosity ({loggingLevel})\n')
        parser.print_usage(sys.stderr)
        sys.stderr.flush()
        sys.exit(EX_USAGE)
    if logFile :        # If sending to a file then check if the log directory exists
        # Check that the logDir exists
        if not os.path.isdir(logDir) :
            sys.stderr.write('Error - logDir ({logDir}) does not exits\n')
            parser.print_usage(sys.stderr)
            sys.stderr.flush()
            sys.exit(EX_USAGE)
        with open(os.path.join(logDir,logFile), 'w') as logfile :
            pass
        if loggingLevel :
            logging.basicConfig(format=logfmt, datefmt='%d/%m/%y %H:%M:%S %p', level=logging_levels[loggingLevel], filename=os.path.join(logDir, logFile))
        else :
            logging.basicConfig(format=logfmt, datefmt='%d/%m/%y %H:%M:%S %p', filename=os.path.join(logDir, logFile))
        print('Now logging to {os.path.join(logDir, logFile)}')
        sys.stdout.flush()
    else :
        if loggingLevel :
            logging.basicConfig(format=logfmt, datefmt='%d/%m/%y %H:%M:%S %p', level=logging_levels[loggingLevel])
        else :
            logging.basicConfig(format=logfmt, datefmt='%d/%m/%y %H:%M:%S %p')
        print('Now logging to sys.stderr')
        sys.stdout.flush()

    # Read in the configuration file - which must exist if required
    config = {}                 # The configuration data
    try:
        with open('SQLAlchemyDB.json', 'rt', newline='') as configfile:
            config = json.load(configfile, object_pairs_hook=collections.OrderedDict)
    except IOError:
        logging.critical('configFile (SQLAlchemyDB.json) failed to load')
        logging.shutdown()
        sys.exit(EX_CONFIG)

    # Check that we have a databaseName if we have a databaseType
    if databaseType not in config:
        logging.critical('databaseType(%s) not found in configuraton file(SQLAlchemyDB.json)', databaseType)
        logging.shutdown()
        sys.exit(EX_CONFIG)
    if 'connectionString' not in config[databaseType]:
        logging.critical('No %s connectionString defined in configuration file(SQLAlchemyDB.json)', databaseType)
        logging.shutdown()
        sys.exit(EX_CONFIG)
    connectionString = config[databaseType]['connectionString']
    if ('username' in config[databaseType]) and (username is None):
        username = config[databaseType]['username']
    if ('password' in config[databaseType]) and (password is None):
        password = config[databaseType]['password']
    if ('server' in config[databaseType]) and (server is None):
        server = config[databaseType]['server']
    if ('databaseName' in config[databaseType]) and (databaseName is None):
        databaseName = config[databaseType]['databaseName']

    # Check that we have all the required paramaters
    if username is None:
        logging.critical('Missing definition for "username"')
        logging.shutdown()
        sys.exit(EX_USAGE)
    if password is None:
        logging.critical('Missing definition for "password"')
        logging.shutdown()
        sys.exit(EX_USAGE)
    if server is None:
        logging.critical('Missing definition for "server"')
        logging.shutdown()
        sys.exit(EX_USAGE)
    if databaseName is None:
        logging.critical('Missing definition for "databaseName"')
        logging.shutdown()
        sys.exit(EX_USAGE)
    connectionString = connectionString.format(username=username, password=password, server=server, databaseName=databaseName)

    # Create the engine
    if databaseType == 'MSSQL':
        engine = create_engine(connectionString, use_setinputsizes=False, echo=True)
    else:
        engine = create_engine(connectionString, echo=True, pool_pre_ping=True, pool_recycle=3600, pool_size=5)

    # Check if the database exists
    if not database_exists(engine.url):
        logging.critical('Database %s does not exist', databaseName)
        logging.shutdown()
        sys.exit(EX_CONFIG)

    # Connect to the database
    try:
        conn = engine.connect()
    except OperationalError:
        logging.critical('Connection error for database %s', databaseName)
        logging.shutdown()
        sys.exit(EX_UNAVAILABLE)
    except Exception as e:
        logging.critical('Connection error for database %s', databaseName)
        logging.shutdown()
        sys.exit(EX_UNAVAILABLE)
    conn.close()
    Session = sessionmaker(bind=engine)
    # logging.getLogger('sqlalchemy.engine').setLevel(logging.DEBUG)

    metadata = MetaData()
    metadata.reflect(bind=engine)

    # Then the Standard files - which must be loaded in the correct order for Primary Key -> Foriegn Key relationships
    tablePhases = {'ADDRESS_SITE':0, 'MB_2011':0, 'MB_2016':0, 'STATE':0,
                   'ADDRESS_SITE_GEOCODE':1,  'LOCALITY':1,
                   'LOCALITY_ALIAS':2, 'LOCALITY_NEIGHBOUR':2, 'LOCALITY_POINT':2, 'STREET_LOCALITY':2,
                   'ADDRESS_DETAIL':3, 'STREET_LOCALITY_ALIAS':3, 'STREET_LOCALITY_POINT':3,
                   'ADDRESS_ALIAS':4, 'ADDRESS_DEFAULT_GEOCODE':4, 'ADDRESS_FEATURE':4, 'ADDRESS_MESH_BLOCK_2011':4, 'ADDRESS_MESH_BLOCK_2016':4,
                   'PRIMARY_SECONDARY':4
                  }

    # Delete all the rows in the Standard files
    for phase in [4, 3, 2, 1, 0]:
        for tablename, tablePhase in tablePhases.items():
            if tablePhase != phase:
                continue
            logging.info('Deleting rows from %s', tablename)
            try:
                table = metadata.tables[tablename]
            except Exception as e:
                table = metadata.tables[tablename.lower()]
            with Session() as session:      # Delete all the rows
                deleteRows = session.query(table).delete()
                session.commit()


    # Process the Authority Code files first
    files = []
    for dirEntry in os.scandir(os.path.join(GNAFdir, 'Authority Code')):
        filename = dirEntry.name
        if filename.endswith('_psv.psv'):
            files.append(filename)

    for filename in files:
        tablename = filename[15:-8]
        dtypes = {}
        parse_dates = []
        SQLAlchemyDtypes = {}
        logging.info('Deleting rows from %s', tablename)
        table = dbConfig.Base.metadata.tables[tablename]
        with Session() as session:      # Delete all the rows
            deleteRows = session.query(table).delete()
            session.commit()
        for column in table.columns:
            if column.type.python_type is decimal.Decimal:
                dtypes[column.name.upper()] = float
            elif column.type.python_type is datetime.date:
                parse_dates.append(column.name.upper())
            else:
                dtypes[column.name.upper()] = column.type.python_type
        with open(os.path.join(GNAFdir, 'Authority Code', filename), 'rt', encoding='utf-8') as csvfile:
            df = pd.read_csv(csvfile, sep='|', dtype=dtypes, parse_dates=parse_dates)
        for column in table.columns:        # Drop any rows where the primary key is null
            if column.primary_key:
                df = df.dropna(subset=[column.name.upper()])
        logging.info("Loading table %s, from file %s", tablename, filename)
        try:
            df.to_sql(tablename, con=engine, index=False, dtype=SQLAlchemyDtypes, if_exists='append', chunksize=100000)
        except Exception as e:
            logging.critical('Failed to load file %s to table %s - error %s:%s', filename, tablename, e, e.args)
            logging.critical('Data: %s', df.to_string())
            logging.shutdown()
            sys.exit(EX_DATAERR)

    # Load the standard files
    filePhases = {0:[], 1:[], 2:[], 3:[], 4:[]}
    for dirEntry in os.scandir(os.path.join(GNAFdir, 'Standard')):
        filename = dirEntry.name
        if filename.endswith('_psv.psv'):
            pre = filename.find('_')
            tablename = filename[pre + 1:-8]
            if tablename in tablePhases:
                phase = tablePhases[tablename]
                filePhases[phase].append((tablename, filename))
            else:
                logging.critical('No known table for file %s', filename)
                logging.shutdown()
                sys.exit(EX_OSFILE)
        else:
            logging.critical('Non-psv file %s', filename)
            logging.shutdown()
            sys.exit(EX_OSFILE)

    # Load the data
    for phase in range(5):
        for tablename, filename in filePhases[phase]:
            dtypes = {}
            parse_dates = []
            SQLAchemyDtypes = {}
            table = dbConfig.Base.metadata.tables[tablename]
            for column in table.columns:
                SQLAlchemyDtypes = column.type
                if column.type.python_type is decimal.Decimal:
                    dtypes[column.name.upper()] = float
                elif column.type.python_type is datetime.date:
                    parse_dates.append(column.name.upper())
                else:
                    dtypes[column.name.upper()] = column.type.python_type
            with open(os.path.join(GNAFdir, 'Standard', filename), 'rt', encoding='utf-8') as csvfile:
                df = pd.read_csv(csvfile, sep='|', dtype=dtypes, parse_dates=parse_dates)
            for column in table.columns:        # Drop any rows where the primary key is null
                if column.primary_key:
                    df = df.dropna(subset=[column.name.upper()])
            try:
                df.to_sql(tablename, con=engine, index=False, dtype=SQLAlchemyDtypes, if_exists='append', chunksize=100000)
            except Exception as e:
                logging.critical('Failed to load file %s to table %s - error %s:%s', filename, tablename, e, e.args)
                logging.critical('Data: %s', df.to_string())
                logging.shutdown()
                sys.exit(EX_DATAERR)

    print('All tables have been loaded')
