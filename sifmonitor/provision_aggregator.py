#! /usr/bin/env if element.tag == 'content':
# ---------------------------------------------------------------------------------------------------------
# Script Name: provision_aggregator.py
# ---------------------------------------------------------------------------------------------------------
#   Description: Python Utility to:
#                1. Pull data from RSS feeds, validate and insert/modify into Postgres database
#                   Currently the file is scheduled to run every 30 seconds
#
#   Pre-requisites: Installation of the pip requirements file:
#                   1. pip install -r requirements.pip
#
#   Author: Avinash Rayapudi
#
#   Usage: python provision_aggregator.py
# ---------------------------------------------------------------------------------------------------------


# Basic Modules
import threading
import time
import urllib.request
from datetime import datetime
import pytz
from lxml import etree
# User defined Modules
import os
import json
import postgresdb
import requests
import transaction_mapper
import settings
import sqlalchemy
from logger_settings import *
from get_serviceurnlayermapping import get_urn_table_mappings

global FIX_FLAG
global st

FIX_FLAG = False
MAX_ENTRY = 25
entrycount = 0
ec = 0
global entryid
entryid = []

duration = 0
entrycountflag = False
config = settings.read_config_file()


def build_xml_url(request_type, start_position=None, max_entry=None, start_time=None, entry_id=None):
    """
    :param request_type: Get request/ put request
    :param start_position: start position of the feed entry
    :param max_entry: number of entries in the feed for each iteration basically 25
    :param start_time: current time
    :param entry_id: 
    :return: 
    Get the xml url based on the params and type of request
    """

    # get_base_url = "URL to get the feed"
    get_base_url = config["SIFEED"]['get_base_url']
    # put_base_url = "URL to delete feed"
    put_base_url = config["SIFEED"]['put_base_url']
    # subscriber_id = 'subscriber_id'
    subscriber_id = config["SIFEED"]['subscriber_id']
    start_position = start_position
    max_entries = max_entry
    temporal_operator = "Before"
    start_time = start_time
    acceptance_state = "True"

    if request_type == 'get':
        url = "{}subscriberId={}&startPosition={}&maxentries={}&temporalOperator={}".format(get_base_url, subscriber_id,
                                                                                            start_position, max_entries,
                                                                                            temporal_operator)
    else:
        entry_id = entry_id.split(':')[-1]
        url = "{}entryId={}&acceptanceState={}&subscriberId={}".format(put_base_url, entry_id, acceptance_state,
                                                                       subscriber_id)
    return url


def get_xml(start_position):
    """
    get xml from the feed by passing required parameters 
    :param start_position: start position of the entry tag's
    :return: 
    """
    try:

        url = build_xml_url(request_type='get', start_position=start_position, max_entry=MAX_ENTRY)
        req = urllib.request.Request(url)
        req.add_header('Accept', 'application/atom+xml')
        xfp = urllib.request.urlopen(req)
        return xfp
    except urllib.error.URLError:
        logger.info("Failed to get XML on initial hit, retrying again . . .")
        time.sleep(2)
        try:
            xfp = urllib.request.urlopen(req)
            return xfp
        except Exception as error:
            logger.error("Failed to get XML on second hit, exiting now!!. Error: {}".format(error))
    except Exception as error:
        logger.error(error)


def put_xml(entry_id):
    """
    Deletes entry's in the feed 
    :param entry_id: 
    :return: 
    """
    try:
        url = build_xml_url(request_type='put', entry_id=entry_id)
        res = requests.put(url)
    except urllib.error.HTTPError as error:
        logger.error("\nurl field {} issue in `replicationfeed table`!!!:  {}".format(url, error))
        try:
            os.remove(settings.application_flag)
        except OSError as ose:
            pass
        exit()
    except Exception as error:
        logger.error(error)


def update_service_urn():
    """ Update the service_urn_mapping data which is stored in a file"""
    try:

        db = postgresdb.DB()
        credentials = eval(config["Database"]["dbs"])["srgis"]
        engine = db.connect(credentials)
        SERVICELAYERURNMAPPING = get_urn_table_mappings(engine)
        with open(settings.service_urn_file, 'w') as fp:
            json.dump(SERVICELAYERURNMAPPING, fp)
    except Exception as error:
        logger.error(error)
        try:
            os.remove(settings.application_flag)
            exit()
        except OSError:
            pass


def check_provisioning_sequences():
    """
    checks if there are any sequences in public
    :return: TRUE/False
    """
    check_seq_sql = "SELECT * FROM information_schema.sequences WHERE sequence_schema='{}';".format(
        settings.target_schema)
    result = postgresdb.execute_sql(check_seq_sql, fetch=True)
    return result


def create_sequences_provisioning():
    """
    If there are no sequences in provisioning, This will create sequences
    :return: None
    """
    db = postgresdb.DB()
    schema_tables = db.get_all_table_names("active")
    create_sequence_sql = ""
    last_val_sql = ""
    schema_sql = ""
    for table_name in schema_tables:
        last_val_sql = ""
        last_val = ""
        last_val_sql = "select nextval('active.{}_ogc_fid_seq');".format(table_name)
        last_val = db.get_lastval(last_val_sql)
        create_sequence_sql += """CREATE SEQUENCE IF NOT EXISTS {0}.{1}_ogc_fid_seq
                              INCREMENT 1
                              MINVALUE 1
                              MAXVALUE 9223372036854775807
                              START 1
                              CACHE 1;
                              ALTER TABLE {0}.{2}_ogc_fid_seq
                              OWNER TO postgres;
                              Alter sequence {0}.{3}_ogc_fid_seq RESTART WITH {4};
                              Alter table {0}.{5} alter ogc_fid set DEFAULT nextval('{0}."{6}_ogc_fid_seq"');""".format(
            settings.target_schema,
            table_name, table_name, table_name, last_val, table_name, table_name)
    try:
        postgresdb.execute_sql(create_sequence_sql)
        logger.info("Creating provisioning sequences for all tables")
    except Exception as error:
        try:
            os.remove(settings.application_flag)
        except OSError as ose:
            pass
        logger.error(error)
        exit()


def download_atom_feed():
    """Download the xml file and parse through and check the entry tags. 
    :return: None 
    """
    logger.info("******* START download_atom_feed() *******")
    starttime = datetime.now(tz=pytz.utc)
    # Create a file for application status flag
    open(settings.application_flag, 'w').close()

    global ns1
    global url
    global fp
    global mandatory_field_check
    db = postgresdb.DB()

    # Namespaces
    ns1 = {"atom": "http://www.w3.org/2005/Atom",
           "gml": "http://www.opengis.net/gml",
           "georss": "http://www.opengis.net/georss",
           "wfs": "http://www.opengis.org/wfs"}
    # Update serviceurn tables
    update_service_urn()
    start_position = 0
    entrycount = 0
    entrycount_res = 0
    while entrycount % 25 in (0, 25):
        xfp = get_xml(start_position)
        entrycount, start_position = parse_create_entry(xfp, start_position)
        entrycount_res += entrycount
        start_position = entrycount_res + 1
        if entrycount < 25:
            start_position = 0
            break
    # drop provisioning schema and create again if there is something to update in database
    drop_schema()
    # copy tables from active schema to provisioning schema
    copy_tables_schema()
    # create sequences for new tables in provisioning schema
    create_sequences_provisioning()

    if isinstance(mandatory_field_check, tuple) and (False in mandatory_field_check):
        mandatory_field_check = False
    elif isinstance(mandatory_field_check, tuple) and not (False in mandatory_field_check):
        mandatory_field_check = True
    else:
        mandatory_field_check = mandatory_field_check
        # mandatory_field will be( False,True, False) if false exists then its F
    if mandatory_field_check and transaction_flag:
        commit_flag = True
        # Commit to database only if the commit_flag is true
        # Perform transactions
        db.transaction_sql()
        try:
            flip_flag = flip_schema()
            if flip_flag:
                logger.error("Flip Schema exception occurred!!! Exiting!")
                open(settings.application_flag, 'w').close()
                FIX_FLAG = True
                exit()

            # for now it's commented, it will delete all the entries in the feed
            # Loop through entry id and clear the entry id tag
            update_feed_flag = config["Default"]["update_feed_flag"]
            if update_feed_flag:
                for e_id in entryid:
                    put_xml(entry_id=e_id)

        except Exception as error:
            try:
                os.remove(settings.application_flag)
            except OSError as ose:
                pass
            logger.error(error)
    elif transaction_flag and not mandatory_field_check:
        try:
            os.remove(settings.application_flag)
        except OSError as ose:
            pass

        logger.error("Transaction Aborted, nothing to update!!!")
    else:
        logger.info("No changes in XML, nothing to update!!!")
    # Remove application status flag
    try:
        os.remove(settings.application_flag)
    except OSError as ose:
        pass
    endtime = datetime.now(tz=pytz.utc)
    db.log_modification_history(starttime, endtime)
    logger.info("******** END download_atom_feed() *******")


def parse_create_entry(xfp, start_position):
    """

    :param xfp: 
    :param start_position: 
    :return: 
    """
    global entrycountflag
    entrycount = 0
    st = 0
    transactiontype = []
    transactiondict = {}
    geoTypeList = []
    geoTypeList2 = []
    update_list = []
    data = []
    updatedflag = False
    GT = []
    global transaction_flag
    global commit_flag
    global timestamp_update_date
    global mandatory_field_check
    global xml_update_timestamp

    tableinfo = []
    transaction_flag = False
    commit_flag = False
    try:
        # Loop through the SOM element
        for event, element in etree.iterparse(xfp):
            if event == 'end' and element.tag == '{http://www.w3.org/2005/Atom}entry':
                entrycount = entrycount + 1
                entrycountflag = True
                start_position += 1
                dataDict = {}
                transaction_flag = True
                for e in element:
                    if e.tag == '{http://www.w3.org/2005/Atom}id':
                        entryid.append(e.text)
                    for content in e:
                        if content.tag == "category":
                            transactiontype.append(content.text)
                        if content.tag == "content":
                            for transactiontp in content:
                                GT.append(etree.QName(transactiontp.tag).localname)
                                for geotype in transactiontp:
                                    tableinfo.append(etree.QName(geotype.tag).localname)
                                if etree.QName(transactiontp.tag).localname != 'siadapter':
                                    geotype = transactiontp
                                    GT[0] = transactiontype[0]
                                for data in geotype.iter():
                                    if etree.QName(data.tag).namespace == ns1['gml']:
                                        if etree.QName(data.getparent()).localname in dataDict.keys():
                                            del dataDict[etree.QName(data.getparent()).localname]
                                        dataDict[etree.QName(data.getparent()).localname] = etree.tostring(data,
                                                                                                           encoding='utf-8',
                                                                                                           method="xml")
                                        break

                                for data in transactiontp.iter():
                                    if etree.QName(data.tag).localname in dataDict.keys():
                                        continue
                                    elif etree.QName(data.tag).namespace == ns1['gml']:
                                        continue
                                    dataDict[etree.QName(data.tag).localname] = data.text
                                transactiondict[transactiontype[0]] = {
                                    (GT[0], etree.QName(geotype.tag).localname): dataDict}

                                # Perform one transaction at a time
                                mandatory_field_check = transaction(transactiondict, transactiontype)
                                # Check if mandatory_field_check is False
                                # and return/exit from this function
                                if (isinstance(mandatory_field_check, bool) and mandatory_field_check == False) or (
                                            isinstance(mandatory_field_check,
                                                       tuple) and False in mandatory_field_check):
                                    return
                                # Reinitialise the below variables
                                transactiontype = geoTypeList = []
                                transactiondict = {}
                                GT = []
                                del dataDict
                                del geotype
                                del data
                                del transactiontp
                                del content
                                del e
                                del element

            else:
                continue
    except Exception as error:
        logger.error(error)
        try:
            os.remove(settings.application_flag)
        except OSError as ose:
            pass
        exit()
    if entrycount == 0:
        mandatory_field_check = False
    return entrycount, start_position


def transaction(transactiondict, transactiontype):
    """
    Transaction mapper for Insert/Update/Delete
    :param transactiondict: 
    :param transactiontype: Insert/Update/Delete
    :return: None
    """
    try:
        transaction_types = transactiondict.keys()
        if "Insert" in transaction_types:
            commit_flag = transaction_insert(transactiondict)
        elif "Update" in transaction_types:
            commit_flag = transaction_update(transactiondict)
        elif "Delete" in transaction_types:
            commit_flag = transaction_delete(transactiondict)
        else:
            logger.error("\nERROR: Unexpected transaction occurred.")
    except Exception as error:
        try:
            os.remove(settings.application_flag)
        except OSError as ose:
            pass
        logger.error("Transaction Mapper Error: ", error)
    return commit_flag


def transaction_insert(transactiondict):
    """
    Insert table mapper for the type of transaction and table
    :param transactiondict: 
    :param transactiontype: Insert/Update/Delete
    :return: None
    """
    try:
        transactiondictTypes = list(transactiondict.get('Insert').keys())
        if ('siadapter', 'Centerlines') in transactiondictTypes:
            mandatory_check = transaction_mapper.mapcenterline_si_to_udm_insert(
                transactiondict)
        elif ('Insert', 'Centerlines') in transactiondictTypes:
            mandatory_check = transaction_mapper.mapcenterline_si_to_udm_insert(
                transactiondict),
        elif ('siadapter', 'CountyBoundary') in transactiondictTypes:
            mandatory_check = transaction_mapper.mapcountyboundary_si_to_udm_insert(
                transactiondict)
        elif ('Insert', 'CountyBoundary') in transactiondictTypes:
            mandatory_check = transaction_mapper.mapcountyboundary_si_to_udm_insert(
                transactiondict)
        elif ('siadapter', 'SiteStructure') in transactiondictTypes:
            mandatory_check = transaction_mapper.mapsitestructure_si_to_udm_insert(
                transactiondict)
        elif ('Insert', 'SiteStructure') in transactiondictTypes:
            mandatory_check = transaction_mapper.mapsitestructure_si_to_udm_insert(
                transactiondict)
        elif ('siadapter', 'UnincorporatedCommunityBoundary') in transactiondictTypes:
            mandatory_check = transaction_mapper.mapunincorporatedboundary_si_to_udm_insert(
                transactiondict)
        elif ('siadapter', 'IncorporatedMunicipalityBoundary') in transactiondictTypes:
            mandatory_check = transaction_mapper.mapincorporatedboundary_si_to_udm_insert(
                transactiondict)
        elif ('Insert', 'IncorporatedMunicipalityBoundary') in transactiondictTypes:
            mandatory_check = transaction_mapper.mapincorporatedboundary_si_to_udm_insert(
                transactiondict)
        elif ('Insert', 'UnincorporatedCommunityBoundary') in transactiondictTypes:
            mandatory_check = transaction_mapper.mapunincorporatedboundary_si_to_udm_insert(
                transactiondict)
        elif ('siadapter', 'StateBoundary') in transactiondictTypes:
            mandatory_check = transaction_mapper.mapstateboundary_si_to_udm_insert(
                transactiondict)
        elif ('Insert', 'StateBoundary') in transactiondictTypes:
            mandatory_check = transaction_mapper.mapstateboundary_si_to_udm_insert(
                transactiondict)
        elif ('Insert', 'ServiceBoundary') in transactiondictTypes:
            mandatory_check = transaction_mapper.mapserviceboundary_si_to_udm_insert(
                transactiondict)
        elif ('siadapter', 'ServiceBoundary') in transactiondictTypes:
            mandatory_check = transaction_mapper.mapserviceboundary_si_to_udm_insert(
                transactiondict)
    except Exception as error:
        logger.error("Table Mapper Error for Insert: ", error)
    return mandatory_check


def transaction_update(transactiondict):
    """
     Update Table mapper for different types of transaction and table
    :param transactiondict: 
    :param transactiontype: Insert/Update/Delete
    :return: None
    """
    try:
        transactiondictTypes = list(transactiondict.get('Update').keys())
        if ('siadapter', 'Centerlines') in transactiondictTypes:
            mandatory_check = transaction_mapper.mapcenterline_si_to_udm_update(
                transactiondict)
        elif ('Update', 'Centerlines') in transactiondictTypes:
            mandatory_check = transaction_mapper.mapcenterline_si_to_udm_update(
                transactiondict),
        elif ('siadapter', 'CountyBoundary') in transactiondictTypes:
            mandatory_check = transaction_mapper.mapcountyboundary_si_to_udm_update(
                transactiondict)
        elif ('Update', 'CountyBoundary') in transactiondictTypes:
            mandatory_check = transaction_mapper.mapcountyboundary_si_to_udm_update(
                transactiondict)
        elif ('siadapter', 'SiteStructure') in transactiondictTypes:
            mandatory_check = transaction_mapper.mapsitestructure_si_to_udm_update(
                transactiondict)
        elif ('Update', 'SiteStructure') in transactiondictTypes:
            mandatory_check = transaction_mapper.mapsitestructure_si_to_udm_update(
                transactiondict)
        elif ('siadapter', 'UnincorporatedCommunityBoundary') in transactiondictTypes:
            mandatory_check = transaction_mapper.mapunincorporatedboundary_si_to_udm_update(
                transactiondict)
        elif ('siadapter', 'IncorporatedMunicipalityBoundary') in transactiondictTypes:
            mandatory_check = transaction_mapper.mapincorporatedboundary_si_to_udm_update(
                transactiondict)
        elif ('Update', 'IncorporatedMunicipalityBoundary') in transactiondictTypes:
            mandatory_check = transaction_mapper.mapincorporatedboundary_si_to_udm_update(
                transactiondict)
        elif ('Update', 'UnincorporatedCommunityBoundary') in transactiondictTypes:
            mandatory_check = transaction_mapper.mapunincorporatedboundary_si_to_udm_update(
                transactiondict)
        elif ('siadapter', 'StateBoundary') in transactiondictTypes:
            mandatory_check = transaction_mapper.mapstateboundary_si_to_udm_update(
                transactiondict)
        elif ('Update', 'StateBoundary') in transactiondictTypes:
            mandatory_check = transaction_mapper.mapstateboundary_si_to_udm_update(
                transactiondict)
        elif ('Update', 'ServiceBoundary') in transactiondictTypes:
            mandatory_check = transaction_mapper.mapserviceboundary_si_to_udm_update(
                transactiondict)
        elif ('siadapter', 'ServiceBoundary') in transactiondictTypes:
            mandatory_check = transaction_mapper.mapserviceboundary_si_to_udm_update(
                transactiondict)
    except Exception as error:
        logger.error("Table Mapper Error for Update: ", error)
    return mandatory_check


def transaction_delete(transactiondict):
    """
    Delete Table mapper for mapping the transaction type to a table
    :param transactiondict: 
    :param transactiontype: Insert/Update/Delete
    :return: None
    """
    try:
        transactiondictTypes = list(transactiondict.get('Delete').keys())
        if ('siadapter', 'Centerlines') in transactiondictTypes:
            mandatory_check = transaction_mapper.mapcenterline_si_to_udm_delete(
                transactiondict)
        elif ('Delete', 'Centerlines') in transactiondictTypes:
            mandatory_check = transaction_mapper.mapcenterline_si_to_udm_delete(
                transactiondict)
        elif ('siadapter', 'CountyBoundary') in transactiondictTypes:
            mandatory_check = transaction_mapper.mapcountyboundary_si_to_udm_delete(
                transactiondict)
        elif ('Delete', 'CountyBoundary') in transactiondictTypes:
            mandatory_check = transaction_mapper.mapcountyboundary_si_to_udm_delete(
                transactiondict)
        elif ('siadapter', 'SiteStructure') in transactiondictTypes:
            mandatory_check = transaction_mapper.mapsitestructure_si_to_udm_delete(
                transactiondict)
        elif ('Delete', 'SiteStructure') in transactiondictTypes:
            mandatory_check = transaction_mapper.mapsitestructure_si_to_udm_delete(
                transactiondict)
        elif ('siadapter', 'UnincorporatedCommunityBoundary') in transactiondictTypes:
            mandatory_check = transaction_mapper.mapunincorporatedboundary_si_to_udm_delete(
                transactiondict)
        elif ('siadapter', 'IncorporatedMunicipalityBoundary') in transactiondictTypes:
            mandatory_check = transaction_mapper.mapincorporatedboundary_si_to_udm_delete(
                transactiondict)
        elif ('Delete', 'IncorporatedMunicipalityBoundary') in transactiondictTypes:
            mandatory_check = transaction_mapper.mapincorporatedboundary_si_to_udm_delete(
                transactiondict)
        elif ('Delete', 'UnincorporatedCommunityBoundary') in transactiondictTypes:
            mandatory_check = transaction_mapper.mapunincorporatedboundary_si_to_udm_delete(
                transactiondict)
        elif ('siadapter', 'StateBoundary') in transactiondictTypes:
            mandatory_check = transaction_mapper.mapstateboundary_si_to_udm_delete(
                transactiondict)
        elif ('Delete', 'StateBoundary') in transactiondictTypes:
            mandatory_check = transaction_mapper.mapstateboundary_si_to_udm_delete(
                transactiondict)
        elif ('Delete', 'ServiceBoundary') in transactiondictTypes:
            mandatory_check = transaction_mapper.mapserviceboundary_si_to_udm_delete(
                transactiondict)
        elif ('siadapter', 'ServiceBoundary') in transactiondictTypes:
            mandatory_check = transaction_mapper.mapserviceboundary_si_to_udm_delete(
                transactiondict)
    except Exception as error:
        logger.error("Table Mapper Error for Insert: ", error)
    return mandatory_check


def flip_schema():
    """
    Rename schema from active to provisioning and vice versa
    :return: None
    """

    flip_sql = """
            BEGIN;
            ALTER SCHEMA active RENAME TO bogus;
            ALTER SCHEMA {0} RENAME TO active;
            ALTER SCHEMA bogus RENAME TO {0};
            COMMIT;
        """.format(settings.target_schema)
    flip_flag_exception = False
    flip_flag_exception_retry = False
    flip_schema_results = postgresdb.retry_execute_sql(flip_sql)
    successful_dbs = flip_schema_results[1]
    if flip_schema_results[0] == True and len(successful_dbs) >= 1:
        postgresdb.retry_execute_sql(flip_sql, successful_dbs, retry=True)
        logger.info("Rename schema from active to provisioning and vice versa")
        flip_flag_exception = True
        return flip_flag_exception
    elif flip_schema_results[0] == True and not successful_dbs:
        return True
    else:
        return flip_flag_exception


def drop_schema(flip_flag_exception=None):
    """
    Drop provisioning cascade and recreate
    :return: None
    """
    db = postgresdb.DB()
    drop_sql = """DROP SCHEMA {0} CASCADE;
             CREATE SCHEMA {0};""".format(settings.target_schema)

    databases = postgresdb.get_databases()
    logger.info("Drop and recreate {0} schema".format(settings.target_schema))
    for database in databases:
        credentials = eval(config["Database"]["dbs"])[database]
        engine = db.connect(credentials)
        try:
            postgresdb.execute_sql(drop_sql)
        except sqlalchemy.exc.DatabaseError as sqlerror:
            logger.error(sqlerror)
        except Exception as error:
            logger.error(error)
            exit()


def copy_tables_schema():
    """
    Copy tables and data from active to new provisioning
    :return: None
    """
    # Check if provisioning sequences exist or else create those sequences

    db = postgresdb.DB()
    schema_tables = db.get_all_table_names("active")
    schema_sql = ""
    for table_name in schema_tables:
        schema_sql += """CREATE TABLE {0}.{1} (LIKE active.{2} INCLUDING  CONSTRAINTS INCLUDING INDEXES INCLUDING DEFAULTS); INSERT INTO {0}.{3} SELECT * FROM active.{4};""".format(
            settings.target_schema,
            table_name, table_name, table_name, table_name)

    db = postgresdb.DB()
    databases = postgresdb.get_databases()
    logger.info("Copy tables and data from active to new {0}".format(settings.target_schema))
    for database in databases:
        credentials = eval(config["Database"]["dbs"])[database]
        engine = db.connect(credentials)
        try:
            with engine.connect() as con:
                con.execute(schema_sql)
        except Exception as error:
            logger.error(error)
            exit()


def schedule_thread():
    if os.path.exists(settings.application_flag) and not FIX_FLAG:
        try:
            os.remove(settings.application_flag)
        except OSError:
            pass
    """Run downloadAtomfeed() every 30 seconds as a separate thread"""
    while 1:
        try:
            open(settings.service_urn_file, 'w').close()
            # Check if the application is running, start a new thread only if application flag is set to False or not available
            # If application flag does not exist then only initiate a new the thread
            if not os.path.exists(settings.application_flag):
                transaction_mapper.TRANSACTION_RESULTS = {}
                t = threading.Thread(target=download_atom_feed)
                t.start()
                t.join()
                time.sleep(settings.THREAD_SLEEP_TIME)
            else:
                # Go through scheduled delay for running a thread
                time.sleep(settings.THREAD_SLEEP_TIME)
        except Exception as error:
            logger.error("Threading Error: ", error)
            exit()
        except OSError as ose:
            os.remove(settings.application_flag)
            pass


if __name__ == "__main__":
    schedule_thread()  # Schedule as a thread runs as a backend job
