#! /usr/bin/env python3
# ---------------------------------------------------------------------------------------------------------
# Script Name: transaction_mapper.py
# --------------------------------------------------------------------------------------------------------
#   Description: Maps the transactions to a particular table
# --------------------------------------------------------------------------------------------------------

# Basic Modules
import json
# User defined modules
import postgresdb
import os
import schema_mapper
import settings
import serviceurnlayermapping
from logger_settings import *

application_flag = 'application_flag.txt'
global TRANSACTION_RESULTS
TRANSACTION_RESULTS = {}


def get_service_urn(transaction_data):
    try:
        transaction_type = list(transaction_data.keys())[0]
        table_map_type = list(transaction_data[transaction_type].keys())[0]
        key = transaction_data[transaction_type][table_map_type].get('ServiceURN')
        with open(settings.service_urn_file, 'r') as fp:
            data = json.load(fp)
            table_name = data.get(key)
        if not table_name:
            try:
                logger.error("Unable to get the serviceurnlayer mapping table!!!!")
                os.remove(settings.application_flag)
                exit()
            except OSError as ose:
                pass
        return table_name
    except Exception as error:
        logger.error(error)
        try:
            os.remove(settings.application_flag)
        except OSError:
            pass


def get_paravalues(paramkeylist, table_values):
    """
    Get paravalues for the table, common function for all geocomm tables
    :param paramkeylist: 
    :param table_values: 
    :return: Paravalues
    """
    paravalues = []
    for param in paramkeylist:
        if isinstance(table_values.get(param), str):
            paravalues.append(str(table_values.get(param)))
        elif isinstance(table_values.get(param), bytes):
            paravalues.append(table_values.get(param).decode('utf-8'))
        else:
            paravalues.append(table_values.get(param))
    return paravalues


# ---------------------centerline--------------------------
def mapcenterline_si_to_udm_insert(transaction_data, transaction_type='Insert'):
    """
    Map centerlines to UDM Insert
    :param transaction_data: Dictionary containing the column values for the table  
    :param transaction_type:  Insert/Update/Delete transaction
    :return: None
    """
    table_name = 'roadcenterline'

    table_values = list(transaction_data.get(transaction_type).values())[0]
    table_map = schema_mapper.get_table_fields(table_name)
    paramkeylist = table_map.get('paramkeylist')

    mandatory_check = check_mandatory_fields(table_values, table_name)
    if mandatory_check:
        # get paravalues for the table
        paravalues = get_paravalues(paramkeylist, table_values)
        sql = """Insert into {}.{} (wkb_geometry, srcunqid, srcofdata, premod, predir, pretype, pretypesep, strname, posttype, postdir, postmod, addrngprel, addrngprer, fromaddl, fromaddr, toaddl, toaddr,parityl,parityr, updatedate, effective, expire, countryl, countryr, statel, stater, countyl, countyr, addcodel, addcoder, incmunil, incmunir, uninccomml, uninccommr, nbrhdcommr, nbrhdcomml,roadclass, speedlimit, oneway, postcomml, postcommr, zipcodel, zipcoder, esnl, esnr )
        values(ST_Multi(ST_SetSRID(ST_GeomFromGML('{}'),4326)),'{}','{}','{}','{}','{}', '{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}', '{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}', '{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}');""".format(
            settings.target_schema,
            table_name, paravalues[0], paravalues[1], paravalues[2], paravalues[3], paravalues[4], paravalues[5],
            paravalues[6], paravalues[7], paravalues[8], paravalues[9], paravalues[10], paravalues[11], paravalues[12],
            paravalues[13], paravalues[14], paravalues[15], paravalues[16], paravalues[17], paravalues[18],
            paravalues[19], paravalues[20], paravalues[21], paravalues[22], paravalues[23], paravalues[24],
            paravalues[25], paravalues[26], paravalues[27], paravalues[28], paravalues[29], paravalues[30],
            paravalues[31], paravalues[32], paravalues[33], paravalues[34], paravalues[35], paravalues[36],
            paravalues[37], paravalues[38], paravalues[39], paravalues[40], paravalues[41], paravalues[42],
            paravalues[43], paravalues[44])
        sql = sql.replace("'None'", 'NULL')
        if TRANSACTION_RESULTS.get(table_name):
            TRANSACTION_RESULTS[table_name] += 1
        else:
            TRANSACTION_RESULTS[table_name] = 1
        if TRANSACTION_RESULTS.get('sql'):
            TRANSACTION_RESULTS['sql'].append(sql)
        else:
            TRANSACTION_RESULTS['sql'] = [sql]
    else:
        logger.error("{} Error!!: Mandatory Field not found!!! Transaction declined".format(table_name))
    return mandatory_check


def mapcenterline_si_to_udm_update(transaction_data, transaction_type='Update'):
    """
    Map centerlines to UDM Update
    :param transaction_data: Dictionary containing the column values for the table  
    :param transaction_type: Update transaction
    :return: None
    """
    table_name = "roadcenterline"
    table_values = list(transaction_data.get(transaction_type).values())[0]
    srcunqid = table_values.get('UniqueId')
    table_map = schema_mapper.get_table_fields(table_name)
    paramkeylist = table_map.get('paramkeylist')
    mandatory_check = check_mandatory_fields(table_values, table_name)

    if mandatory_check:
        paravalues = get_paravalues(paramkeylist, table_values)
        sql = """UPDATE {}.{} SET wkb_geometry = ST_Multi(ST_SetSRID(ST_GeomFromGML(
        '{}'),4326)), srcunqid = '{}', srcofdata = '{}', premod = '{}',
         predir = '{}', pretype = '{}', pretypesep = '{}', strname = '{}',
         posttype = '{}', postdir = '{}', postmod = '{}', addrngprel = '{}',
         addrngprer = '{}', fromaddl = '{}', fromaddr = '{}', toaddl = '{}',
         toaddr = '{}', parityl = '{}', parityr = '{}', updatedate = '{}',
         effective = '{}', expire = '{}', countryl = '{}', countryr = '{}',
         statel = '{}', stater = '{}', countyl = '{}', countyr = '{}',
         addcodel = '{}', addcoder = '{}', incmunil = '{}', incmunir = '{}',
         uninccommr = '{}', uninccomml = '{}', nbrhdcommr = '{}',
         nbrhdcomml = '{}', roadclass = '{}', speedlimit = '{}',
         oneway = '{}', postcomml = '{}', postcommr = '{}', zipcodel = '{}',
         zipcoder = '{}',esnl = '{}', esnr = '{}' """.format(settings.target_schema, table_name,
                                                             paravalues[0], paravalues[1], paravalues[2], paravalues[3],
                                                             paravalues[4], paravalues[5], paravalues[6], paravalues[7],
                                                             paravalues[8], paravalues[9], paravalues[10],
                                                             paravalues[11],
                                                             paravalues[12], paravalues[13], paravalues[14],
                                                             paravalues[15],
                                                             paravalues[16], paravalues[17], paravalues[18],
                                                             paravalues[19],
                                                             paravalues[20], paravalues[21], paravalues[22],
                                                             paravalues[23],
                                                             paravalues[24], paravalues[25], paravalues[26],
                                                             paravalues[27],
                                                             paravalues[28], paravalues[29], paravalues[30],
                                                             paravalues[31],
                                                             paravalues[32], paravalues[33], paravalues[34],
                                                             paravalues[35],
                                                             paravalues[36], paravalues[37], paravalues[38],
                                                             paravalues[39],
                                                             paravalues[40], paravalues[41], paravalues[42],
                                                             paravalues[43],
                                                             paravalues[44]) + " WHERE srcunqid = '" + srcunqid + "';"
        sql = sql.replace("'None'", 'NULL')
        if TRANSACTION_RESULTS.get(table_name):
            TRANSACTION_RESULTS[table_name] += 1
        else:
            TRANSACTION_RESULTS[table_name] = 1
        if TRANSACTION_RESULTS.get('sql'):
            TRANSACTION_RESULTS['sql'].append(sql)
        else:
            TRANSACTION_RESULTS['sql'] = [sql]
    else:
        logger.error('{} Error!!: Mandatory Field not found!!! Transaction declined'.format(table_name))
    return mandatory_check


def mapcenterline_si_to_udm_delete(transaction_data, transaction_type='Delete'):
    """
    Map centerlines to UDM Delete
    :param transaction_data: Dictionary containing the column values for the table 
    :param transaction_type: Delete transaction
    :return: None
    """
    table_name = "roadcenterline"
    table_values = list(transaction_data.get(transaction_type).values())[0]
    paramkeylist = ['UniqueId']
    paravalues = get_paravalues(paramkeylist, table_values)

    sql = "Delete from {}.{} where srcunqid = '{}';".format(settings.target_schema,
                                                           table_name, paravalues[0])
    if TRANSACTION_RESULTS.get(table_name):
        TRANSACTION_RESULTS[table_name] += 1
    else:
        TRANSACTION_RESULTS[table_name] = 1
    if TRANSACTION_RESULTS.get('sql'):
        TRANSACTION_RESULTS['sql'].append(sql)
    else:
        TRANSACTION_RESULTS['sql'] = [sql]
    return True


# ---------------------------------CountyBoundary-------------------
def mapcountyboundary_si_to_udm_insert(transaction_data,
                                       transaction_type='Insert'):
    """
    Map countyboundary to UDM Insert
    :param transaction_data: Dictionary containing the column values for the table 
    :param transaction_type: Insert transaction
    :return: None
    """
    table_name = 'countyboundary'
    table_values = list(transaction_data.get(transaction_type).values())[0]
    table_map = schema_mapper.get_table_fields(table_name)
    paramkeylist = table_map.get('paramkeylist')

    mandatory_check = check_mandatory_fields(table_values, table_name)
    if mandatory_check:
        paravalues = get_paravalues(paramkeylist, table_values)

        sql = """Insert into {}.{} (wkb_geometry, srcunqid,
               srcofdata, updatedate, effective, expire, country, state,
               county)
               values(ST_Multi(ST_SetSRID(ST_GeomFromGML('{}'),4326)),'{}','{}','{}',
               '{}','{}','{}','{}','{}');""".format(settings.target_schema, table_name,
                                                    paravalues[0], paravalues[1], paravalues[2], paravalues[3],
                                                    paravalues[4], paravalues[5], paravalues[6], paravalues[7],
                                                    paravalues[8])
        sql = sql.replace("'None'", 'NULL')
        if TRANSACTION_RESULTS.get(table_name):
            TRANSACTION_RESULTS[table_name] += 1
        else:
            TRANSACTION_RESULTS[table_name] = 1
        if TRANSACTION_RESULTS.get('sql'):
            TRANSACTION_RESULTS['sql'].append(sql)
        else:
            TRANSACTION_RESULTS['sql'] = [sql]
    else:
        logger.error('{} Error!!: Mandatory Field not found!!! Transaction declined'.format(table_name))
    return mandatory_check


def mapcountyboundary_si_to_udm_update(transaction_data,
                                       transaction_type='Update'):
    """
    Map CountyBoundary to UDM Update
    :param transaction_data: Dictionary containing the column values for the table 
    :param transaction_type: Update transaction
    :return: None
    """
    table_name = "countyboundary"
    table_values = list(transaction_data.get(transaction_type).values())[0]
    srcunqid = table_values.get('UniqueId')
    table_map = schema_mapper.get_table_fields(table_name)
    paramkeylist = table_map.get('paramkeylist')
    mandatory_check = check_mandatory_fields(table_values, table_name)

    if mandatory_check:
        paravalues = get_paravalues(paramkeylist, table_values)
        sql = """UPDATE {}.{} SET wkb_geometry = ST_Multi(ST_SetSRID(
        ST_GeomFromGML('{}'),4326)), srcunqid = '{}', srcofdata = '{}',
        updatedate = '{}', effective = '{}', expire = '{}', country = '{}',
        state = '{}', county = '{}' """.format(settings.target_schema, table_name,
                                               paravalues[0], paravalues[1], paravalues[2], paravalues[3],
                                               paravalues[4], paravalues[5], paravalues[6], paravalues[7],
                                               paravalues[8]) + " WHERE srcunqid = '" + srcunqid + "';"
        sql = sql.replace("'None'", 'NULL')
        if TRANSACTION_RESULTS.get(table_name):
            TRANSACTION_RESULTS[table_name] += 1
        else:
            TRANSACTION_RESULTS[table_name] = 1
        if TRANSACTION_RESULTS.get('sql'):
            TRANSACTION_RESULTS['sql'].append(sql)
        else:
            TRANSACTION_RESULTS['sql'] = [sql]
    else:
        logger.error('{} Error!!: Mandatory Field not found!!! Transaction declined'.format(table_name))
    return mandatory_check


def mapcountyboundary_si_to_udm_delete(transaction_data,
                                       transaction_type='Delete'):
    """
    Map CountyBoundary to UDM Delete
    :param transaction_data: Dictionary containing the column values for the table
    :param transaction_type: Delete transaction
    :return: None
    """
    table_name = "countyboundary"
    table_values = list(transaction_data.get(transaction_type).values())[0]
    paramkeylist = ['UniqueId']
    paravalues = get_paravalues(paramkeylist, table_values)
    sql = "Delete from {}.{} where srcunqid = '{}';".format(settings.target_schema,
                                                           table_name, paravalues[0])
    if TRANSACTION_RESULTS.get(table_name):
        TRANSACTION_RESULTS[table_name] += 1
    else:
        TRANSACTION_RESULTS[table_name] = 1
    if TRANSACTION_RESULTS.get('sql'):
        TRANSACTION_RESULTS['sql'].append(sql)
    else:
        TRANSACTION_RESULTS['sql'] = [sql]
    return True


# -------------------------------------SiteStructure----------------------------------
def mapsitestructure_si_to_udm_insert(transaction_data,
                                      transaction_type='Insert'):
    """
    Map SiteStructure to UDM Insert
    :param transaction_data: Dictionary containing the column values for the table
    :param transaction_type: Insert transaction
    :return: None
    """
    table_name = 'ssap'
    table_values = list(transaction_data.get(transaction_type).values())[0]
    table_map = schema_mapper.get_table_fields(table_name)
    paramkeylist = table_map.get('paramkeylist')

    mandatory_check = check_mandatory_fields(table_values, table_name)
    if mandatory_check:
        paravalues = get_paravalues(paramkeylist, table_values)
        sql = """Insert into {}.{} (wkb_geometry, srcunqid,
        srcofdata, updatedate, effective, expire,country, state, county,
        addcode, incmuni, uninccomm, nbrhdcomm, premod, predir, pretype,
        pretypesep, strname,posttype, postdir, postmod, addnumpre, addnum,
        addnumsuf, milepost, esn, postcomm, zipcode, building,floor, unit,
        room, seat, landmark, location, placetype, adddatauri)
        values(ST_SetSRID(ST_GeomFromGML('{}'),4326),'{}','{}','{}','{}','{}',
        '{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}',
        '{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}',
        '{}','{}','{}');""".format(settings.target_schema, table_name,
                                   paravalues[0], paravalues[1], paravalues[2], paravalues[3],
                                   paravalues[4], paravalues[5], paravalues[6], paravalues[7],
                                   paravalues[8], paravalues[9], paravalues[10], paravalues[11],
                                   paravalues[12], paravalues[13], paravalues[14], paravalues[15],
                                   paravalues[16], paravalues[17], paravalues[18], paravalues[19],
                                   paravalues[20], paravalues[21], paravalues[22], paravalues[23],
                                   paravalues[24], paravalues[25], paravalues[26], paravalues[27],
                                   paravalues[28], paravalues[29], paravalues[30], paravalues[31],
                                   paravalues[32], paravalues[33], paravalues[34], paravalues[35],
                                   paravalues[36])
        sql = sql.replace("'None'", 'NULL')
        if TRANSACTION_RESULTS.get(table_name):
            TRANSACTION_RESULTS[table_name] += 1
        else:
            TRANSACTION_RESULTS[table_name] = 1
        if TRANSACTION_RESULTS.get('sql'):
            TRANSACTION_RESULTS['sql'].append(sql)
        else:
            TRANSACTION_RESULTS['sql'] = [sql]
    else:
        logger.error('SiteStructure Error!!: Mandatory Field not found!!! Transaction declined')
    return mandatory_check


def mapsitestructure_si_to_udm_update(transaction_data,
                                      transaction_type='Update'):
    """
    Map SiteStructure to UDM Update
    :param transaction_data: Dictionary containing the column values for the table
    :param transaction_type: Update transaction
    :return: None
    """
    table_name = "ssap"
    table_values = list(transaction_data.get(transaction_type).values())[0]
    srcunqid = table_values.get('UniqueId')
    table_map = schema_mapper.get_table_fields(table_name)
    paramkeylist = table_map.get('paramkeylist')
    mandatory_check = check_mandatory_fields(table_values, table_name)

    if mandatory_check:
        paravalues = get_paravalues(paramkeylist, table_values)
        sql = """UPDATE {}.{} SET wkb_geometry = ST_SetSRID(ST_GeomFromGML(
        '{}'),4326), srcunqid = '{}' , srcofdata = '{}' , updatedate = '{}' ,
        effective = '{}' , expire = '{}' ,country = '{}' , state = '{}' ,
        county = '{}' , addcode = '{}' , incmuni = '{}' , uninccomm = '{}' ,
        nbrhdcomm = '{}' , premod = '{}' , predir = '{}' , pretype = '{}' ,
        pretypesep = '{}' , strname = '{}' ,posttype = '{}' , postdir = '{}' ,
        postmod = '{}' , addnumpre = '{}' , addnum = '{}' , addnumsuf = '{}' ,
        milepost = '{}' , esn = '{}' , postcomm = '{}' , zipcode = '{}' ,
        building = '{}',floor = '{}' , unit = '{}' , room = '{}' , seat = '{}',
        landmark = '{}', location = '{}', placetype = '{}',
        adddatauri = '{}'""".format(settings.target_schema, table_name,
                                    paravalues[0], paravalues[1], paravalues[2], paravalues[3],
                                    paravalues[4], paravalues[5], paravalues[6], paravalues[7],
                                    paravalues[8], paravalues[9], paravalues[10], paravalues[11],
                                    paravalues[12], paravalues[13], paravalues[14], paravalues[15],
                                    paravalues[16], paravalues[17], paravalues[18], paravalues[19],
                                    paravalues[20], paravalues[21], paravalues[22], paravalues[23],
                                    paravalues[24], paravalues[25], paravalues[26], paravalues[27],
                                    paravalues[28], paravalues[29], paravalues[30], paravalues[31],
                                    paravalues[32], paravalues[33], paravalues[34], paravalues[35],
                                    paravalues[36]) + " WHERE srcunqid = '" + srcunqid + "';"
        sql = sql.replace("'None'", 'NULL')
        # action_statement = "Update successful for SiteStructure!!!"
        if TRANSACTION_RESULTS.get(table_name):
            TRANSACTION_RESULTS[table_name] += 1
        else:
            TRANSACTION_RESULTS[table_name] = 1
        if TRANSACTION_RESULTS.get('sql'):
            TRANSACTION_RESULTS['sql'].append(sql)
        else:
            TRANSACTION_RESULTS['sql'] = [sql]
    else:
        logger.error('SiteStructure Error!!: Mandatory Field not found!!! Transaction declined')
    return mandatory_check


def mapsitestructure_si_to_udm_delete(transaction_data,
                                      transaction_type='Delete'):
    """
    Map SiteStructure to UDM Delete
    :param transaction_data: Dictionary containing the column values for the table
    :param transaction_type: Delete transaction
    :return: None
    """
    table_name = "ssap"
    table_values = list(transaction_data.get(transaction_type).values())[0]
    paramkeylist = ['UniqueId']
    paravalues = get_paravalues(paramkeylist, table_values)
    sql = "Delete from {}.{} where srcunqid = '{}';".format(settings.target_schema,
                                                           table_name, paravalues[0])
    # action_statement = "Deletion successful for SiteStructure!!!"
    if TRANSACTION_RESULTS.get(table_name):
        TRANSACTION_RESULTS[table_name] += 1
    else:
        TRANSACTION_RESULTS[table_name] = 1
    if TRANSACTION_RESULTS.get('sql'):
        TRANSACTION_RESULTS['sql'].append(sql)
    else:
        TRANSACTION_RESULTS['sql'] = [sql]
    return True


# --------------------------------------UnincorporatedBoundary-----------------------------
def mapunincorporatedboundary_si_to_udm_insert(transaction_data,
                                               transaction_type='Insert'):
    """
    Map UnincorporatedBoundary to UDM Insert
    :param transaction_data: Dictionary containing the column values for the table
    :param transaction_type: Insert transaction
    :return: None
    """
    table_name = 'uninccommboundary'
    table_values = list(transaction_data.get(transaction_type).values())[0]
    table_map = schema_mapper.get_table_fields(table_name)
    paramkeylist = table_map.get('paramkeylist')

    mandatory_check = check_mandatory_fields(table_values, table_name)
    if mandatory_check:
        paravalues = get_paravalues(paramkeylist, table_values)
        sql = """Insert into {}.{} (wkb_geometry, srcunqid,
               srcofdata, updatedate, effective, expire, country, state,
               county, addcode, uninccomm)
               values(ST_Multi(ST_SetSRID(ST_GeomFromGML('{}'),4326)),'{}','{}','{}','{}',
               '{}','{}','{}','{}','{}','{}');""".format(settings.target_schema, table_name,
                                                         paravalues[0], paravalues[1], paravalues[2], paravalues[3],
                                                         paravalues[4], paravalues[5], paravalues[6], paravalues[7],
                                                         paravalues[8], paravalues[9], paravalues[10])
        sql = sql.replace("'None'", 'NULL')
        if TRANSACTION_RESULTS.get(table_name):
            TRANSACTION_RESULTS[table_name] += 1
        else:
            TRANSACTION_RESULTS[table_name] = 1
        if TRANSACTION_RESULTS.get('sql'):
            TRANSACTION_RESULTS['sql'].append(sql)
        else:
            TRANSACTION_RESULTS['sql'] = [sql]
    else:
        logger.error('{} Error!!: Mandatory Field not found!!! Transaction declined'.format(table_name))
    return mandatory_check


def mapunincorporatedboundary_si_to_udm_update(transaction_data,
                                               transaction_type='Update'):
    """
    Map UnincorporatedBoundary to UDM Update
    :param transaction_data: Dictionary containing the column values for the table
    :param transaction_type: Update Transaction
    :return: None
    """
    table_name = "uninccommboundary"
    table_values = list(transaction_data.get(transaction_type).values())[0]
    srcunqid = table_values.get('UniqueId')
    table_map = schema_mapper.get_table_fields(table_name)
    paramkeylist = table_map.get('paramkeylist')
    mandatory_check = check_mandatory_fields(table_values, table_name)

    if mandatory_check:
        paravalues = get_paravalues(paramkeylist, table_values)
        sql = """UPDATE {}.{} SET wkb_geometry = ST_Multi(ST_SetSRID(
        ST_GeomFromGML('{}'),4326)), srcunqid = '{}', srcofdata = '{}',
        updatedate = '{}', effective = '{}', expire = '{}', country = '{}',
        state = '{}', county = '{}', addcode = '{}',
        uninccomm = '{}' """.format(settings.target_schema, table_name,
                                    paravalues[0], paravalues[1], paravalues[2], paravalues[3],
                                    paravalues[4], paravalues[5], paravalues[6], paravalues[7],
                                    paravalues[8], paravalues[9],
                                    paravalues[10]) + " WHERE srcunqid = '" + srcunqid + "';"
        sql = sql.replace("'None'", 'NULL')
        # action_statement = "Update successful for unincorporatedCommunityBoundary!!!"
        if TRANSACTION_RESULTS.get(table_name):
            TRANSACTION_RESULTS[table_name] += 1
        else:
            TRANSACTION_RESULTS[table_name] = 1
        if TRANSACTION_RESULTS.get('sql'):
            TRANSACTION_RESULTS['sql'].append(sql)
        else:
            TRANSACTION_RESULTS['sql'] = [sql]
    else:
        logger.error('unincorporatedCommunityBoundary Error!!: Mandatory Field not found!!! Transaction declined')
    return mandatory_check


def mapunincorporatedboundary_si_to_udm_delete(transaction_data,
                                               transaction_type='Delete'):
    """
    Map UnincorporatedBoundary to UDM Delete
    :param transaction_data: Dictionary containing the column values for the table
    :param transaction_type: Delete Transaction
    :return: None
    """
    table_name = "uninccommboundary"
    table_values = list(transaction_data.get(transaction_type).values())[0]
    paramkeylist = ['UniqueId']
    paravalues = get_paravalues(paramkeylist, table_values)
    sql = "Delete from {}.{} where srcunqid = '{}';".format(settings.target_schema, table_name,
                                                           paravalues[0])
    # action_statement = "Deletion successful for uninccommboundary!!!"
    if TRANSACTION_RESULTS.get(table_name):
        TRANSACTION_RESULTS[table_name] += 1
    else:
        TRANSACTION_RESULTS[table_name] = 1
    if TRANSACTION_RESULTS.get('sql'):
        TRANSACTION_RESULTS['sql'].append(sql)
    else:
        TRANSACTION_RESULTS['sql'] = [sql]
    return True


# ---------------------------------------IncorporatedBoundary-----------------------
def mapincorporatedboundary_si_to_udm_insert(transaction_data,
                                             transaction_type='Insert'):
    """
    Map UnincorporatedBoundary to UDM Insert
    :param transaction_data: Dictionary containing the column values for the table
    :param transaction_type: Insert Transaction
    :return: None
    """
    table_name = 'incmunicipalboundary'
    table_values = list(transaction_data.get(transaction_type).values())[0]
    table_map = schema_mapper.get_table_fields(table_name)
    paramkeylist = table_map.get('paramkeylist')

    mandatory_check = check_mandatory_fields(table_values, table_name)
    if mandatory_check:
        paravalues = get_paravalues(paramkeylist, table_values)
        sql = """Insert into {}.{} (wkb_geometry, srcunqid,
               srcofdata, updatedate, effective, expire, country, state,
               county, addcode, muni)
               values(ST_Multi(ST_SetSRID(ST_GeomFromGML('{}'),4326)),'{}','{}','{}','{}',
               '{}','{}','{}','{}','{}','{}');""".format(settings.target_schema, table_name,
                                                         paravalues[0], paravalues[1], paravalues[2], paravalues[3],
                                                         paravalues[4], paravalues[5], paravalues[6], paravalues[7],
                                                         paravalues[8], paravalues[9], paravalues[10])
        sql = sql.replace("'None'", 'NULL')
        if TRANSACTION_RESULTS.get(table_name):
            TRANSACTION_RESULTS[table_name] += 1
        else:
            TRANSACTION_RESULTS[table_name] = 1
        if TRANSACTION_RESULTS.get('sql'):
            TRANSACTION_RESULTS['sql'].append(sql)
        else:
            TRANSACTION_RESULTS['sql'] = [sql]
    else:
        logger.error('{} Error!!: Mandatory Field not found!!! Transaction declined'.format(table_name))
    return mandatory_check


def mapincorporatedboundary_si_to_udm_update(transaction_data,
                                             transaction_type='Update'):
    """
    Map IncorporatedBoundary to UDM Update
    :param transaction_data: Dictionary containing the column values for the table
    :param transaction_type: Update Transaction
    :return: None
    """
    table_name = "incmunicipalboundary"
    table_values = list(transaction_data.get(transaction_type).values())[0]
    srcunqid = table_values.get('UniqueId')
    table_map = schema_mapper.get_table_fields(table_name)
    paramkeylist = table_map.get('paramkeylist')
    mandatory_check = check_mandatory_fields(table_values, table_name)

    if mandatory_check:
        paravalues = get_paravalues(paramkeylist, table_values)
        sql = """ UPDATE {}.{} SET wkb_geometry = ST_multi(ST_SetSRID(
        ST_GeomFromGML('{}'),4326)), srcunqid = '{}', srcofdata = '{}',
        updatedate = '{}', effective = '{}', expire = '{}', country = '{}',
        state = '{}', county = '{}' , addcode = '{}', muni = '{}' """.format(settings.target_schema, table_name,
                                                                             paravalues[0], paravalues[1],
                                                                             paravalues[2], paravalues[3],
                                                                             paravalues[4], paravalues[5],
                                                                             paravalues[6], paravalues[7],
                                                                             paravalues[8], paravalues[9],
                                                                             paravalues[
                                                                                               10]) + " WHERE srcunqid = '" + srcunqid + "';"
        sql = sql.replace("'None'", 'NULL')
        # action_statement = "Update successful for IncorporatedMunicipalityBoundary!!!"
        if TRANSACTION_RESULTS.get(table_name):
            TRANSACTION_RESULTS[table_name] += 1
        else:
            TRANSACTION_RESULTS[table_name] = 1
        if TRANSACTION_RESULTS.get('sql'):
            TRANSACTION_RESULTS['sql'].append(sql)
        else:
            TRANSACTION_RESULTS['sql'] = [sql]

    else:
        logger.error('Error!!: Mandatory Field not found!!! Transaction declined')
    return mandatory_check


def mapincorporatedboundary_si_to_udm_delete(transaction_data,
                                             transaction_type='Delete'):
    """
    Map IncorporatedBoundary to UDM Delete
    :param transaction_data: Dictionary containing the column values for the table
    :param transaction_type: Delete Transaction 
    :return: None
    """
    table_name = "incmunicipalboundary"
    table_values = list(transaction_data.get(transaction_type).values())[0]
    paramkeylist = ['UniqueId']
    paravalues = get_paravalues(paramkeylist, table_values)
    sql = "Delete from {}.{} where srcunqid = '{}';".format(settings.target_schema,
                                                           table_name, paravalues[0])
    if TRANSACTION_RESULTS.get(table_name):
        TRANSACTION_RESULTS[table_name] += 1
    else:
        TRANSACTION_RESULTS[table_name] = 1
    if TRANSACTION_RESULTS.get('sql'):
        TRANSACTION_RESULTS['sql'].append(sql)
    else:
        TRANSACTION_RESULTS['sql'] = [sql]
    return True


# ----------------------------------------StateBoundary-------------------------
def mapstateboundary_si_to_udm_insert(transaction_data,
                                      transaction_type='Insert'):
    """
    Map StateBoundary to UDM Insert
    :param transaction_data: Dictionary containing the column values for the table
    :param transaction_type: Insert Transaction
    :return: None
    """
    table_name = 'stateboundary'
    table_values = list(transaction_data.get(transaction_type).values())[0]
    table_map = schema_mapper.get_table_fields(table_name)
    paramkeylist = table_map.get('paramkeylist')
    mandatory_check = check_mandatory_fields(table_values, table_name)
    if mandatory_check:
        paravalues = get_paravalues(paramkeylist, table_values)
        sql = """Insert into {}.{} (wkb_geometry, srcunqid,
        srcofdata, updatedate, effective, expire, country, state)
        values(ST_Multi(ST_SetSRID(ST_GeomFromGML('{}'),4326)),'{}','{}','{}','{}','{}',
        '{}','{}');""".format(settings.target_schema, table_name,
                              paravalues[0], paravalues[1], paravalues[2], paravalues[3],
                              paravalues[4], paravalues[5], paravalues[6], paravalues[7])
        sql = sql.replace("'None'", 'NULL')
        if TRANSACTION_RESULTS.get(table_name):
            TRANSACTION_RESULTS[table_name] += 1
        else:
            TRANSACTION_RESULTS[table_name] = 1
        if TRANSACTION_RESULTS.get('sql'):
            TRANSACTION_RESULTS['sql'].append(sql)
        else:
            TRANSACTION_RESULTS['sql'] = [sql]

    else:
        logger.error('{} Error!!: Mandatory Field not found!!! Transaction declined'.format(table_name))
    return mandatory_check


def mapstateboundary_si_to_udm_update(transaction_data, transaction_type='Update'):
    """
    Map StateBoundary to UDM Update
    :param transaction_data: Dictionary containing the column values for the table
    :param transaction_type: Update Transaction
    :return: None
    """
    table_name = "stateboundary"
    table_values = list(transaction_data.get(transaction_type).values())[0]
    srcunqid = table_values.get('UniqueId')
    table_map = schema_mapper.get_table_fields(table_name)
    paramkeylist = table_map.get('paramkeylist')
    mandatory_check = check_mandatory_fields(table_values, table_name)

    if mandatory_check:
        paravalues = get_paravalues(paramkeylist, table_values)
        sql = """ UPDATE {}.{} SET wkb_geometry = ST_Multi(ST_SetSRID(
        ST_GeomFromGML('{}'),4326)), srcunqid = '{}', srcofdata = '{}',
        updatedate = '{}', effective = '{}', expire = '{}', country = '{}',
        state = '{}' """.format(settings.target_schema, table_name,
                                paravalues[0], paravalues[1], paravalues[2], paravalues[3],
                                paravalues[4], paravalues[5], paravalues[6],
                                paravalues[7]) + " WHERE srcunqid = '" + srcunqid + "';"
        sql = sql.replace("'None'", 'NULL')
        # action_statement = "Update successful for {}!!!".format(table_name)
        if TRANSACTION_RESULTS.get(table_name):
            TRANSACTION_RESULTS[table_name] += 1
        else:
            TRANSACTION_RESULTS[table_name] = 1
        if TRANSACTION_RESULTS.get('sql'):
            TRANSACTION_RESULTS['sql'].append(sql)
        else:
            TRANSACTION_RESULTS['sql'] = [sql]
    else:
        logger.error('{} Error!!: Mandatory Field not found!!! Transaction declined'.format(table_name))
    return mandatory_check


def mapstateboundary_si_to_udm_delete(transaction_data,
                                      transaction_type='Delete'):
    """
    Map StateBoundary to UDM Delete
    :param transaction_data: Dictionary containing the column values for the table
    :param transaction_type: Delete transaction
    :return: None
    """
    table_name = "stateboundary"
    table_values = list(transaction_data.get(transaction_type).values())[0]
    paramkeylist = ['UniqueId']
    paravalues = get_paravalues(paramkeylist, table_values)
    sql = "Delete from {}.{} where srcunqid = '{}';".format(settings.target_schema,
                                                           table_name, paravalues[0])

    # action_statement = "Deletion successful for {}!!!".format(table_name)
    if TRANSACTION_RESULTS.get(table_name):
        TRANSACTION_RESULTS[table_name] += 1
    else:
        TRANSACTION_RESULTS[table_name] = 1
    if TRANSACTION_RESULTS.get('sql'):
        TRANSACTION_RESULTS['sql'].append(sql)
    else:
        TRANSACTION_RESULTS['sql'] = [sql]
    return True


# -------------------------------------ServiceBoundary----------------------------
def mapserviceboundary_si_to_udm_insert(transaction_data,
                                        transaction_type='Insert'):
    """
    Map ServiceBoundary to UDM Insert
    :param transaction_data: Dictionary containing the column values for the table
    :param transaction_type: Insert Transaction 
    :return: None
    """
    postgres = postgresdb.DB()
    table_name = 'serviceboundary'
    # service_table_name = postgres.get_service_urn_layer_mapping(
    #    transaction_data)
    service_table_name = get_service_urn(transaction_data)
    table_values = list(transaction_data.get(transaction_type).values())[0]
    table_map = schema_mapper.get_table_fields(table_name)
    paramkeylist = table_map.get('paramkeylist')
    mandatory_check = check_mandatory_fields(table_values, table_name)
    if mandatory_check:
        paravalues = get_paravalues(paramkeylist, table_values)
        sql = """Insert into {}.{} (wkb_geometry, srcunqid,
        srcofdata, updatedate, effective, expire, country, state, county,
        agencyid, routeuri, serviceurn, servicenum, vcarduri, displayname)
        values(ST_Multi(ST_SetSRID(ST_GeomFromGML('{}'),4326)),'{}','{}','{}','{}','{}',
        '{}','{}','{}','{}','{}','{}','{}','{}','{}');""".format(settings.target_schema, service_table_name,
                                                                 paravalues[0], paravalues[1], paravalues[2],
                                                                 paravalues[3],
                                                                 paravalues[4], paravalues[5], paravalues[6],
                                                                 paravalues[7],
                                                                 paravalues[8], paravalues[9], paravalues[10],
                                                                 paravalues[11],
                                                                 paravalues[12], paravalues[13], paravalues[14])
        sql = sql.replace("'None'", 'NULL')
        # action_statement = "Insertion of values into {} table successful".format(service_table_name)
        if TRANSACTION_RESULTS.get(table_name):
            TRANSACTION_RESULTS[service_table_name] += 1
        else:
            TRANSACTION_RESULTS[service_table_name] = 1
        if TRANSACTION_RESULTS.get('sql'):
            TRANSACTION_RESULTS['sql'].append(sql)
        else:
            TRANSACTION_RESULTS['sql'] = [sql]
    else:
        logger.error('Error!!: Mandatory Field not found!!! Transaction declined'.format(table_name))
    return mandatory_check


def mapserviceboundary_si_to_udm_update(transaction_data,
                                        transaction_type='Update'):
    """
    Map ServiceBoundary to UDM Update
    :param transaction_data: Dictionary containing the column values for the table
    :param transaction_type: Update Transaction
    :return: None
    """
    postgres = postgresdb.DB()
    table_name = 'serviceboundary'
    # service_table_name = postgres.get_service_urn_layer_mapping(
    #    transaction_data)
    service_table_name = get_service_urn(transaction_data)
    table_values = list(transaction_data.get(transaction_type).values())[0]
    table_map = schema_mapper.get_table_fields(table_name)
    paramkeylist = table_map.get('paramkeylist')
    srcunqid = table_values.get('UniqueId')
    mandatory_check = check_mandatory_fields(table_values, table_name)
    if mandatory_check:
        paravalues = get_paravalues(paramkeylist, table_values)
        sql = """  UPDATE {}.{} SET  wkb_geometry = ST_Multi(ST_SetSRID(
        ST_GeomFromGML('{}'),4326)), srcunqid = '{}', srcofdata = '{}',
        updatedate = '{}', effective = '{}', expire = '{}', country = '{}',
        state = '{}', county = '{}', agencyid = '{}', routeuri = '{}',
        serviceurn = '{}', servicenum = '{}', vcarduri = '{}',
        displayname = '{}'  """.format(settings.target_schema,
                                       service_table_name, paravalues[0], paravalues[1], paravalues[2],
                                       paravalues[3], paravalues[4], paravalues[5], paravalues[6],
                                       paravalues[7], paravalues[8], paravalues[9], paravalues[10],
                                       paravalues[11], paravalues[12], paravalues[13],
                                       paravalues[14]) + " WHERE srcunqid = '" + srcunqid + "';"
        sql = sql.replace("'None'", 'NULL')
        # action_statement = "Update values into {} table successful".format(service_table_name)
        if TRANSACTION_RESULTS.get(table_name):
            TRANSACTION_RESULTS[service_table_name] += 1
        else:
            TRANSACTION_RESULTS[service_table_name] = 1
        if TRANSACTION_RESULTS.get('sql'):
            TRANSACTION_RESULTS['sql'].append(sql)
        else:
            TRANSACTION_RESULTS['sql'] = [sql]
    else:
        logger.error('{} Error!!: Mandatory Field not found!!! Transaction declined'.format(table_name))
    return mandatory_check


def mapserviceboundary_si_to_udm_delete(transaction_data,
                                        transaction_type='Delete'):
    """
    Map ServiceBoundary to UDM Delete
    :param transaction_data: Dictionary containing the column values for the table
    :param transaction_type: Delete transaction
    :return: None
    """
    postgres = postgresdb.DB()
    # service_table_name = postgres.get_service_urn_layer_mapping(
    #    transaction_data)
    service_table_name = get_service_urn(transaction_data)
    table_values = list(transaction_data.get(transaction_type).values())[0]
    paramkeylist = ['UniqueId']
    paravalues = get_paravalues(paramkeylist, table_values)
    sql = "Delete from {}.{} where srcunqid = '{}';".format(settings.target_schema,
                                                           service_table_name, paravalues[0])
    # action_statement = "Deletion of  values into {} table successfully".format(service_table_name)
    if TRANSACTION_RESULTS.get(service_table_name):
        TRANSACTION_RESULTS[service_table_name] += 1
    else:
        TRANSACTION_RESULTS[service_table_name] = 1
    if TRANSACTION_RESULTS.get('sql'):
        TRANSACTION_RESULTS['sql'].append(sql)
    else:
        TRANSACTION_RESULTS['sql'] = [sql]
    return True


def check_mandatory_fields(transaction_data, table_name):
    """
    Check mandatory fields for the table
    :param transaction_data: Dictionary containing the column values for the table
    :param table_name: Table name for the transaction
    :return: mandatory_check_flag
    """
    mandatory_check_flag = True
    table_map = schema_mapper.get_table_fields(table_name)
    mandatory_fields = table_map.get('mandatory_fields')
    transaction_data_keys = [key.lower()
                             for key in list(transaction_data.keys())]
    table_keys = [key.lower() for key in mandatory_fields]
    mf_check = len(set(transaction_data_keys) & set(table_keys)) == len(table_keys)
    if mf_check:
        for field in transaction_data:
            if not transaction_data.get(field) and (field.lower() in table_keys):
                mandatory_check_flag = False
                break
    else:
        mandatory_check_flag = False
    return mandatory_check_flag
