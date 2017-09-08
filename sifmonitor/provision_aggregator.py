#! /usr/bin/env python3
# ---------------------------------------------------------------------------------------------------------
# Script Name: provision_aggregator.py
# ---------------------------------------------------------------------------------------------------------
#   Description: Python Utility to:
#                1. Pull data from RSS feeds, validate and insert/modify into Postgres database
#                   Currently the file will be scheduled to run every 30 seconds
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
from lxml import etree
# User defined Modules
import os
import json
import postgresdb
import transaction_mapper
import schema_mapper
import settings
import sqlalchemy
from sqlalchemy.exc import *
from logger_settings import *
from get_serviceurnlayermapping import get_urn_table_mappings

global FIX_FLAG
FIX_FLAG = False

def update_service_urn():
    """ Update the service_urn_mapping data which is stored in a file"""    
    try:
        db = postgresdb.DB()
        credentials = settings.read_json(settings.CREDENTIAL_FILE).get('srgis')
        engine = db.connect(credentials)
        SERVICELAYERURNMAPPING = get_urn_table_mappings(engine)
        with open(settings.service_urn_file, 'w') as fp:
            json.dump(SERVICELAYERURNMAPPING, fp)
    except Exception as error:
        logger.error(error)
        try:
            os.remove(settings.application_flag)
        except OSError:
            pass


def check_provisioning_sequences():
    """
    checks if there are any sequences in public
    :return: TRUE/False
    """
    check_seq_sql = "SELECT * FROM information_schema.sequences WHERE sequence_schema='{}';".format(settings.target_schema)
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
        create_sequence_sql += """CREATE SEQUENCE IF NOT EXISTS provisioning.{}_ogc_fid_seq
                              INCREMENT 1
                              MINVALUE 1
                              MAXVALUE 9223372036854775807
                              START 1
                              CACHE 1;
                              ALTER TABLE provisioning.{}_ogc_fid_seq
                              OWNER TO postgres;
                              Alter sequence provisioning.{}_ogc_fid_seq RESTART WITH {};
                              Alter table provisioning.{} alter ogc_fid set DEFAULT nextval('provisioning."{}_ogc_fid_seq"');""".format(table_name, table_name, table_name, last_val, table_name, table_name)
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
    """Download the xml file and parse through and check the entry tags. Retrieve
     values from replication feed table
    :return: None 
    """
    logger.info("******* START download_atom_feed() *******")   
    # Create a file for application status flag
    open(settings.application_flag, 'w').close()
    
    global ns1
    global url
    global fp
    global mandatory_field_check
    postgres = postgresdb.DB()
    feedList = postgres.get_replicationfeeds()
    try:
        if feedList:
            lastupdateprocessed = feedList[4]
            item_id = feedList[0]
            url = feedList[2]
            fp = urllib.request.urlopen(url)
        else:
            logger.warning("No values retrieved for replicationfeed table")
    
    except urllib.error.HTTPError as error:
        logger.error("\nurl field {} issue in `replicationfeed table`!!!:  {}".format(url, error))
        try:
            os.remove(settings.application_flag)
        except OSError as ose:
            pass
        exit()
    
    except Exception as error:
        try:
            os.remove(settings.application_flag)
        except OSError as ose:
            pass
        logger.error("\nERROR in `replicationfeed table`!!!:  {}"
                     .format(error))
        exit()

    # Namespaces
    ns1 = {"atom": "http://www.w3.org/2005/Atom",
           "gml": "http://www.opengis.net/gml",
           "georss": "http://www.opengis.net/georss",
           "wfs": "http://www.opengis.org/wfs"}
    # Update serviceurn tables
    update_service_urn()
    # Parse XML file and validate if there has been any changes to timestamp
    parse_create_entry(lastupdateprocessed)
    if isinstance(mandatory_field_check, tuple) and (False in mandatory_field_check):
        mandatory_field_check = False
    elif isinstance(mandatory_field_check, tuple) and not (False in mandatory_field_check):
        mandatory_field_check = True
    else:
        mandatory_field_check = mandatory_field_check
        # mandatory_field will be( False,True, Fale) if false exists then its F

    if mandatory_field_check and transaction_flag:
        commit_flag = True
        # Commit to database only if the commit_flag is true
        # Update the replicationfeeds database value with the timestamp from xml only if this commit flag is True
        # Perform transaction
        db = postgresdb.DB()
        try:
            db.transaction_sql()
        except Exception as error:
            logger.error(error)
            try:
                os.remove(settings.application_flag)
            except:
                pass
        # Update schema and copy schema
        try:
            flip_flag = flip_schema()
            if flip_flag:
                logger.error("Flip Schema exception occurred!!! Exiting!")
                open(settings.application_flag, 'w').close()
                FIX_FLAG = True
                exit()

            drop_schema()
            # update_schema()
            copy_tables_schema()
            create_sequences_provisioning()
        except Exception as error:
            try:
                os.remove(settings.application_flag)
            except OSError as ose:
                pass
            logger.error(error)
            # exit()
        updatedTime = datetime.strptime(
            xml_update_timestamp, "%Y-%m-%dT%H:%M:%Sz").replace(tzinfo=None)
        if updatedTime > lastupdateprocessed.replace(tzinfo=None):
            timestamp_update_date = xml_update_timestamp.split('Z')[0]
            db = postgresdb.DB()
            db.update_last_processed(timestamp_update_date,
                                           lastupdateprocessed,
                                           item_id)
    
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
    logger.info("******** END download_atom_feed() *******")


def parse_create_entry(previousupdatedate):
    """
     Parse the XML and check the necessary entries
    :param previousupdatedate: Last update processed time stamp
    :return: None
    """

    transactiontype = []
    transactiondict = {}
    geoTypeList = []
    geoTypeList2 = []
    update_list = []
    data = []
    updatedflag = False
    global transaction_flag
    global commit_flag
    global timestamp_update_date
    global mandatory_field_check
    global xml_update_timestamp
    transaction_flag = False
    commit_flag = False
    try:
        # Loop through the SOM element
        for event, element in etree.iterparse(fp):
            if event == 'end' and element.tag == '{http://www.w3.org/2005/Atom}updated':
                # Get both updatedate tag from xml file
                update_list.append(element.text)
                if len(update_list) > 1:
                    # Convert the string date to datetime object type for both
                    # updatetimstamp from xml file
                    updatedTime = datetime.strptime(
                        update_list[0], "%Y-%m-%dT%H:%M:%Sz").replace(tzinfo=None)
                    xml_update_timestamp = update_list[0]
                    updatedTimeelement = datetime.strptime(
                        update_list[1], "%Y-%m-%dT%H:%M:%Sz").replace(tzinfo=None)
                    timestamp_update_date = update_list[0].split('Z')[0]
                    previous_replicationupdate = datetime.strftime(
                        previousupdatedate, "%Y-%m-%dT%H:%M:%S")
                    # Break from the entry tag1 when both update at fields are
                    # matching with DB update and go to next entry tag
                    if (updatedTime == previousupdatedate.replace(tzinfo=None)
                        ) and (updatedTimeelement == previousupdatedate.replace(
                                                                                tzinfo=None)):
                        update_list.pop(1)
                        updatedflag = False
                        continue

                    # check DB updatetime and xml update time and update flag
                    # and loop through entry or sub nodes
                    if updatedTimeelement > previousupdatedate.replace(tzinfo=None):
                        updatedflag = True
                    else:
                        updatedflag = False
                        update_list.pop(1)
                        continue

                    # Pop the second element from update_list to filter out
                    # different updatetags
                    update_list.pop(1)
            # Based on the updateflag , if set to true then only perform
            # transactions, or else continue to next entry
            if event == 'end' and element.tag == '{http://www.w3.org/2005/Atom}entry' and updatedflag:
                alias_address = 0
                alias_street_segment = 0
                dataDict = {}
                transaction_flag = True
                
                for e in element:
                    # browse through the content to get the transaction
                    for content in e:
                        if content.tag == "{http://www.opengis.org/wfs}Transaction":
                            for transactiontp in content:  # insert,update or delete
                                transactiontype.append(etree.QName(transactiontp.tag).localname)
                                for geoType in transactiontp:  # SIAdapter, Centerline etc
                                    geoTypeList.append(etree.QName(geoType.tag).localname)
                                    if etree.QName(geoType.tag).localname != 'siadapter':
                                        geoType = transactiontp
                                    for geoType2 in geoType:
                                        geoTypeList2.append(geoType2.tag)  # centerline, stateboundery ect
                                        for data in geoType2.iter():
                                            if etree.QName(data.tag).namespace == ns1['gml']:
                                                if etree.QName(data.getparent()).localname in dataDict.keys():
                                                    del dataDict[etree.QName(data.getparent()).localname]
                                                dataDict[etree.QName(data.getparent()).localname] = etree.tostring(data,
                                                                                                                   encoding='utf-8',
                                                                                                                   method="xml")
                                                break
                                        for data in geoType2.iter():
                                            if etree.QName(data.tag).localname in (
                                                    'StreetSegment', 'AliasStreetSegment'):
                                                alias_street_segment += 1
                                            if etree.QName(data.tag).localname in ('Address', 'AliasAddress'):
                                                alias_address += 1
                                            if etree.QName(data.tag).localname in dataDict.keys():
                                                continue
                                            elif etree.QName(data.tag).namespace == ns1['gml']:
                                                continue
                                            elif alias_street_segment > 1 and etree.QName(
                                                    data.tag).localname in schema_mapper.skip_fields:
                                                continue
                                            elif alias_address > 1 and etree.QName(data.tag).localname in schema_mapper.skip_fields:
                                                continue
                                            dataDict[etree.QName(data.tag).localname] = data.text
                                        transactiondict[etree.QName(transactiontp.tag).localname] = {(etree.QName(
                                            geoType.tag).localname, geoType2.tag.replace(
                                            '{urn:nena:xml:ns:SIProvisioningExchange:2.0}', '')): dataDict}
                                        # Perform one transaction at a time
                                        mandatory_field_check = transaction(transactiondict, transactiontype)
                                        # Check if mandatory_field_check is False
                                        # and return/exit from this function
                                        if (isinstance(mandatory_field_check,
                                                       bool) and mandatory_field_check == False) or (
                                                    isinstance(mandatory_field_check,
                                                               tuple) and False in mandatory_field_check):
                                            return
                                        # Reinitialise the below variables
                                        transactiontype = geoTypeList = []
                                        transactiondict = {}
                                        del geoType
                                        del geoType2
                                        del data
                                        del transactiontp
                                        del content
                                        del e
                                        del element
            elif not updatedflag:
                mandatory_field_check = False
            else:
                continue
    except Exception as error:
        logger.error(error)
        try:
            os.remove(settings.application_flag)
        except OSError as ose:
            pass
        exit()


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
    flip_sql = """ALTER SCHEMA active RENAME TO bogus;
             ALTER SCHEMA provisioning RENAME TO active; 
             ALTER SCHEMA bogus RENAME TO provisioning;
         """
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
    drop_sql = """DROP SCHEMA provisioning CASCADE;
             CREATE SCHEMA provisioning;"""
    
    databases = postgresdb.get_databases()
    logger.info("Drop and recreate provisioning schema")
    for database in databases:
        credentials = settings.read_json(settings.CREDENTIAL_FILE).get(database)
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
        schema_sql += """CREATE TABLE provisioning.{} (LIKE active.{} INCLUDING  CONSTRAINTS INCLUDING INDEXES INCLUDING DEFAULTS); INSERT INTO provisioning.{} SELECT * FROM active.{};""".format(table_name, table_name, table_name, table_name, table_name)
    db = postgresdb.DB()
    databases = postgresdb.get_databases()
    logger.info("Copy tables and data from active to new provisioning")
    for database in databases:
        credentials = settings.read_json(settings.CREDENTIAL_FILE).get(database)
        engine = db.connect(credentials)
        try:
            with engine.connect() as con:
                con.execute(schema_sql)
        except Exception as error:
            logger.error(error)
            exit()


def schedule_thread():

    if os.path.exists(settings.application_flag) and not FIX_FLAG:
        os.remove(settings.application_flag)
    """Run downloadAtomfeed() every 30 seconds as a separate thread"""
    while 1:
        try:
            # Check if the application is running, start a new thread only if application flag is set to False or not available
            # If application flag doesnt exist then only initiate the thread
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
    schedule_thread()  # Schedule as a thread which runs as a backend job
