import psycopg2
from enum import Enum
from datetime import datetime
import json
import postgresdb
import models
from postgresdb import DB

# ---------------------Centerline--------------------------

def MapCenterlineSI_to_UDM_Insert(transaction_data, transaction_type='Insert'):
    table_name = 'centerline'
    centerlines = list(transaction_data.get(transaction_type).values())[0]
    table_map = models.get_table_fields(table_name)
    paramkeylist = table_map.get('paramkeylist')

    paravalues = []
    mandatory_check = check_mandatory_fields(centerlines, table_name)
    if mandatory_check:
        for param in paramkeylist:
            if isinstance(centerlines[param], str):  # or isinstance(centerlines[param], bytes):
                paravalues.append(str(centerlines[param]))
            elif isinstance(centerlines[param], bytes):  # or isinstance(centerlines[param], bytes):
                paravalues.append(centerlines[param].decode('utf-8'))
                # paravalues.append("ST_SetSRID(ST_GeomFromGML('{}'),4326)".format(centerlines[param].decode('utf-8')))
            else:
                paravalues.append(centerlines[param])

        sql = 'Insert into ' + table_name + """(wkb_geometry, srcunqid, srcofdata, premod, predir, pretype, pretypesep, strname, posttype, postdir, postmod, addrngprel, addrngprer, fromaddl, fromaddr, toaddl,toaddr,parityl,parityr, updatedate, effective, expire, countryl, countryr, statel, stater, countyl, countyr, addcodel, addcoder, incmunil, incmunir, uninccomml, uninccommr, nbrhdcomml, nbrhdcommr, roadclass, speedlimit, oneway, postcomml, postcommr, zipcodel, zipcoder, esnl, esnr )
        values(ST_SetSRID(ST_GeomFromGML('{}'),4326),'{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}');""".format(
            paravalues[0], paravalues[1], paravalues[2], paravalues[3], paravalues[4], paravalues[5], paravalues[6],
            paravalues[7], paravalues[8], paravalues[9], paravalues[10], paravalues[11], paravalues[12], paravalues[13],
            paravalues[14], paravalues[15], paravalues[16], paravalues[17], paravalues[18], paravalues[19],
            paravalues[20], paravalues[21], paravalues[22], paravalues[23], paravalues[24], paravalues[25], paravalues[26],
            paravalues[27], paravalues[28], paravalues[29], paravalues[30], paravalues[31], paravalues[32], paravalues[33],
            paravalues[34], paravalues[35], paravalues[36], paravalues[37], paravalues[38], paravalues[39], paravalues[40],
            paravalues[41], paravalues[42], paravalues[43], paravalues[44])

        geocomm = postgresdb.DB(database='geocomm')
        action_statement = "Insertion successful for Centerline!!!"
        geocomm.transaction_sql(sql, action_statement)
    else:
        print('Error!!: Mandatory Field not found!!! Transaction declined')


def MapCenterlineSI_to_UDM_Delete(transaction_data, transaction_type='Delete'):
    table_name = "centerline"
    centerlines = list(transaction_data.get(transaction_type).values())[0]
    paramkeylist = ['RoadSegmentUniqueId']
    
    paravalues = []
    for param in paramkeylist:
        if isinstance(centerlines[param], str):  # or isinstance(centerlines[param], bytes):
            paravalues.append(str(centerlines[param]))
        elif isinstance(centerlines[param], bytes):  # or isinstance(centerlines[param], bytes):
            paravalues.append(centerlines[param].decode('utf-8'))
        else:
            paravalues.append(centerlines[param])

    sql = "Delete from centerline where srcunqid = '{}';".format(paravalues[0])
    geocomm = postgresdb.DB(database='geocomm')
    action_statement = "Deletion successful for Centerline!!!"
    geocomm.transaction_sql(sql, action_statement)


def MapCenterlineSI_to_UDM_Update(transaction_data, transaction_type='Update'):
    table_name = "centerline"
    centerlines = list(transaction_data.get(transaction_type).values())[0]
    srcunqid = centerlines.get('RoadSegmentUniqueId')
    table_map = models.get_table_fields(table_name)
    paramkeylist = table_map.get('paramkeylist')
    mandatory_check = check_mandatory_fields(centerlines, table_name)

    paravalues = []  # list Comprehension
    if mandatory_check:
        for param in paramkeylist:
            if isinstance(centerlines[param], str):  # or isinstance(centerlines[param], bytes):
                paravalues.append(str(centerlines[param]))
            elif isinstance(centerlines[param], bytes):  # or isinstance(centerlines[param], bytes):
                paravalues.append(centerlines[param].decode('utf-8'))
            else:
                paravalues.append(centerlines[param])
        sql = """UPDATE centerline SET wkb_geometry = ST_SetSRID(ST_GeomFromGML('{}'),4326), srcunqid = '{}', srcofdata = '{}',premod = '{}',predir = '{}',pretype = '{}',pretypesep = '{}',strname = '{}',posttype = '{}',postdir = '{}',postmod = '{}',addrngprel = '{}',addrngprer = '{}',fromaddl = '{}',fromaddr = '{}',toaddl = '{}',toaddr = '{}',parityl = '{}',parityr = '{}',updatedate = '{}',effective = '{}',expire = '{}',countryl = '{}',countryr = '{}',statel = '{}',stater = '{}',countyl = '{}',countyr = '{}',addcodel = '{}',addcoder = '{}',incmunil = '{}',incmunir = '{}',uninccomml = '{}',uninccommr = '{}',nbrhdcomml = '{}', nbrhdcommr = '{}',roadclass = '{}',speedlimit = '{}',oneway = '{}',postcomml = '{}',postcommr = '{}',zipcodel = '{}',zipcoder = '{}',esnl = '{}', esnr = '{}' """.format(
        paravalues[0], paravalues[1], paravalues[2], paravalues[3], paravalues[4], paravalues[5], paravalues[6],
        paravalues[7], paravalues[8], paravalues[9], paravalues[10], paravalues[11], paravalues[12], paravalues[13],
        paravalues[14], paravalues[15], paravalues[16], paravalues[17], paravalues[18], paravalues[19], paravalues[20],
        paravalues[21], paravalues[22], paravalues[23], paravalues[24], paravalues[25], paravalues[26], paravalues[27],
        paravalues[28], paravalues[29], paravalues[30], paravalues[31], paravalues[32], paravalues[33], paravalues[34],
        paravalues[35], paravalues[36], paravalues[37], paravalues[38], paravalues[39], paravalues[40], paravalues[41],
        paravalues[42], paravalues[43], paravalues[44]) + " WHERE srcunqid = '" + srcunqid + "';"
        
        geocomm = postgresdb.DB(database='geocomm')
        action_statement = "Update successful for Centerline!!!"
        geocomm.transaction_sql(sql, action_statement)
    else:
        print('Error!!: Mandatory Field not found!!! Transaction declined')


# ---------------------------------CountyBoundary-------------------


def MapCountyBoundarySI_to_UDM_Insert(transaction_data, transaction_type='Insert'):
    table_name = 'countyboundary'
    CountyBoundary = list(transaction_data.get(transaction_type).values())[0]
    table_map = models.get_table_fields(table_name)
    paramkeylist = table_map.get('paramkeylist')

    paravalues = []
    mandatory_check = check_mandatory_fields(CountyBoundary, TABLE_COUNTYBOUNDARY)
    if mandatory_check:

        for param in paramkeylist:
            if isinstance(CountyBoundary[param], str):
                paravalues.append(str(CountyBoundary[param]))
            elif isinstance(CountyBoundary[param], bytes):
                paravalues.append(CountyBoundary[param].decode('utf-8'))
            else:
                paravalues.append(CountyBoundary[param])
        sql = 'Insert into ' + TABLE_COUNTYBOUNDARY + """(wkb_geometry, srcunqid, srcofdata, updatedate, effective, expire, country, state, county)
            values(ST_SetSRID(ST_GeomFromGML('{}'),4326),'{}','{}','{}','{}','{}','{}','{}','{}');""".format(paravalues[0], paravalues[1], paravalues[2], paravalues[3], paravalues[4],paravalues[5],  paravalues[6], paravalues[7], paravalues[8])

        geocomm = postgresdb.DB(database='geocomm')
        action_statement = "Insertion successful for CountyBoundary!!!"
        geocomm.transaction_sql(sql, action_statement)

    else:
        print('Error!!: Mandatory Field not found!!! Transaction declined')


def MapCountyBoundarySI_to_UDM_Delete(transaction_data, transaction_type='Delete'):
    table_name = "countyboundary"
    CountyBoundary = list(transaction_data.get(transaction_type).values())[0]
    paramkeylist = ['UniqueId']

    paravalues = []  # list Comprehension
    for param in paramkeylist:
        if isinstance(CountyBoundary[param], str):
            paravalues.append(str(CountyBoundary[param]))
        elif isinstance(CountyBoundary[param], bytes):
            paravalues.append(CountyBoundary[param]).decode('utf-8')
        else:
            paravalues.append(CountyBoundary[param])

    sql = "Delete from countyboundary where srcunqid = '{}';".format(paravalues[0])

    geocomm = postgresdb.DB(database='geocomm')
    action_statement = "Deletion successful for countyboundary!!!"
    geocomm.transaction_sql(sql, action_statement)


def MapCountyBoundarySI_to_UDM_Update(transaction_data, transaction_type='Update'):
    table_name = "countyboundary"
    CountyBoundary = list(transaction_data.get(transaction_type).values())[0]
    srcunqid = CountyBoundary.get('UniqueId')
    table_map = models.get_table_fields(table_name)
    paramkeylist = table_map.get('paramkeylist')
    mandatory_check = check_mandatory_fields(CountyBoundary, table_name)

    paravalues = []
    if mandatory_check:
        for param in paramkeylist:
            if isinstance(CountyBoundary[param], str):
                paravalues.append(str(CountyBoundary[param]))
            elif isinstance(CountyBoundary[param], bytes):
                paravalues.append(CountyBoundary[param].decode('utf-8'))
            else:
                paravalues.append(CountyBoundary[param])

        sql = """UPDATE countyboundary SET wkb_geometry = ST_SetSRID(ST_GeomFromGML('{}'),4326), srcunqid = '{}', srcofdata = '{}', updatedate = '{}', effective = '{}', expire = '{}', country = '{}', state = '{}', county = '{}' """.format(
            paravalues[0], paravalues[1], paravalues[2], paravalues[3], paravalues[4], paravalues[5], paravalues[6],
            paravalues[7], paravalues[8]) + " WHERE srcunqid = '" + srcunqid + "';"
        geocomm = postgresdb.DB(database='geocomm')
        action_statement = "Update successful for countyboundary!!!"
        geocomm.transaction_sql(sql, action_statement)
    else:
        print('Error!!: Mandatory Field not found!!! Transaction declined')

# -------------------------------------SiteStructure----------------------------------


def MapSiteStructureSI_to_UDM_Insert(transaction_data, transaction_type='Insert'):
    table_name = 'ssap'
    SiteStructure = list(transaction_data.get(transaction_type).values())[0]
    table_map = models.get_table_fields(table_name)
    paramkeylist = table_map.get('paramkeylist')

    paravalues = []
    mandatory_check = check_mandatory_fields(SiteStructure, table_name)
    if mandatory_check:
        for param in paramkeylist:
            if isinstance(SiteStructure[param], str):
                paravalues.append(str(SiteStructure[param]))
            elif isinstance(SiteStructure[param], bytes):
                paravalues.append(SiteStructure[param].decode('utf-8'))
            else:
                paravalues.append(SiteStructure[param])

        sql = 'Insert into ' + table_name + """(wkb_geometry, srcunqid, srcofdata, updatedate, effective, expire,country, state, county, addcode, incmuni, uninccomm, nbrhdcomm, premod, predir, pretype, pretypesep, strname,posttype, postdir, postmod, addnumpre, addnum, addnumsuf, milepost, esn, postcomm, zipcode, building,floor, unit, room, seat, landmark, location, placetype, adddatauri) 
        values(ST_SetSRID(ST_GeomFromGML('{}'),4326),'{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}');""".format(
            paravalues[0], paravalues[1], paravalues[2], paravalues[3], paravalues[4], paravalues[5], paravalues[6],
            paravalues[7], paravalues[8], paravalues[9], paravalues[10], paravalues[11], paravalues[12], paravalues[13],
            paravalues[14], paravalues[15], paravalues[16], paravalues[17], paravalues[18], paravalues[19], paravalues[20],
            paravalues[21], paravalues[22], paravalues[23], paravalues[24], paravalues[25], paravalues[26], paravalues[27],
            paravalues[28], paravalues[29], paravalues[30], paravalues[31], paravalues[32], paravalues[33], paravalues[34],
            paravalues[35], paravalues[36])

        geocomm = postgresdb.DB(database='geocomm')
        action_statement = "Insertion successful for SiteStructure!!!"
        geocomm.transaction_sql(sql, action_statement)
    else:
        print('Error!!: Mandatory Field not found!!! Transaction declined')


def MapSiteStructureSI_to_UDM_Delete(transaction_data, transaction_type='Delete'):
    table_name = "ssap"
    SiteStructure = list(transaction_data.get(transaction_type).values())[0]
    paramkeylist = ['UniqueId']

    paravalues = []
    for param in paramkeylist:
        if isinstance(SiteStructure[param], str):
            paravalues.append(str(SiteStructure[param]))
        elif isinstance(SiteStructure[param], bytes):
            paravalues.append(SiteStructure[param].decode('utf-8'))
        else:
            paravalues.append(SiteStructure[param])

    sql = "Delete from ssap where srcunqid = '{}';".format(paravalues[0])

    geocomm = postgresdb.DB(database='geocomm')
    action_statement = "Deletion successful for SiteStructure!!!"
    geocomm.transaction_sql(sql, action_statement)


def MapSiteStructureSI_to_UDM_Update(transaction_data, transaction_type='Update'):
    table_name = "ssap"
    SiteStructure = list(transaction_data.get(transaction_type).values())[0]
    srcunqid = SiteStructure.get('UniqueId')
    table_map = models.get_table_fields(table_name)
    paramkeylist = table_map.get('paramkeylist')
    mandatory_check = check_mandatory_fields(SiteStructure, table_name)

    paravalues = []  # list Comprehension
    if mandatory_check:
        for param in paramkeylist:
            if isinstance(SiteStructure[param], str):
                paravalues.append(str(SiteStructure[param]))
            elif isinstance(SiteStructure[param], bytes):
                paravalues.append(SiteStructure[param].decode('utf-8'))
            else:
                paravalues.append(SiteStructure[param])
        sql = """UPDATE ssap SET wkb_geometry = ST_SetSRID(ST_GeomFromGML('{}'),4326), srcunqid = '{}' , srcofdata = '{}' , updatedate = '{}' , effective = '{}' , expire = '{}' ,country = '{}' , state = '{}' , county = '{}' , addcode = '{}' , incmuni = '{}' , uninccomm = '{}' , nbrhdcomm = '{}' , premod = '{}' , predir = '{}' , pretype = '{}' , pretypesep = '{}' , strname = '{}' ,posttype = '{}' , postdir = '{}' , postmod = '{}' , addnumpre = '{}' , addnum = '{}' , addnumsuf = '{}' , milepost = '{}' , esn = '{}' , postcomm = '{}' , zipcode = '{}' , building = '{}' ,floor = '{}' , unit = '{}' , room = '{}' , seat = '{}' , landmark = '{}' , location = '{}' , placetype = '{}' , adddatauri = '{}' """.format(
            paravalues[0], paravalues[1], paravalues[2], paravalues[3], paravalues[4], paravalues[5], paravalues[6],
            paravalues[7], paravalues[8], paravalues[9], paravalues[10], paravalues[11], paravalues[12], paravalues[13],
            paravalues[14], paravalues[15], paravalues[16], paravalues[17], paravalues[18], paravalues[19], paravalues[20],
            paravalues[21], paravalues[22], paravalues[23], paravalues[24], paravalues[25], paravalues[26], paravalues[27],
            paravalues[28], paravalues[29], paravalues[30], paravalues[31], paravalues[32], paravalues[33], paravalues[34],
            paravalues[35], paravalues[36]) + " WHERE srcunqid = '" + srcunqid + "';"

        geocomm = postgresdb.DB(database='geocomm')
        action_statement = "Update successful for SiteStructure!!!"
        geocomm.transaction_sql(sql, action_statement)
    else:
        print('Error!!: Mandatory Field not found!!! Transaction declined')


# --------------------------------------UnincorporatedBoundary-----------------------------


def MapUnincorporatedBoundarySI_to_UDM_Insert(transaction_data, transaction_type='Insert'):
    table_name = 'uninccommboundary'
    unincorporatedCommunityBoundary = list(transaction_data.get(transaction_type).values())[0]
    table_map = models.get_table_fields(table_name)
    paramkeylist = table_map.get('paramkeylist')

    paravalues = []
    mandatory_check = check_mandatory_fields(unincorporatedCommunityBoundary, table_name)
    if mandatory_check:

        for param in paramkeylist:
            if isinstance(unincorporatedCommunityBoundary[param], str):
                paravalues.append(str(unincorporatedCommunityBoundary[param]))
            elif isinstance(unincorporatedCommunityBoundary[param], bytes):
                paravalues.append(unincorporatedCommunityBoundary[param].decode('utf-8'))
            else:
                paravalues.append(unincorporatedCommunityBoundary[param])

        sql = 'Insert into ' + table_name + """(wkb_geometry, srcunqid, srcofdata, updatedate, effective, expire, country, state, county, addcode, uninccomm)
            values(ST_SetSRID(ST_GeomFromGML('{}'),4326),'{}','{}','{}','{}','{}','{}','{}','{}','{}','{}');""".format(
            paravalues[0], paravalues[1], paravalues[2], paravalues[3], paravalues[4], paravalues[5], paravalues[6],
            paravalues[7], paravalues[8], paravalues[9], paravalues[10])
        geocomm = postgresdb.DB(database='geocomm')
        action_statement = "Insertion successful for uninccommboundary!!!"
        geocomm.transaction_sql(sql, action_statement)
    else:
        print('Error!!: Mandatory Field not found!!! Transaction declined')


def MapUnincorporatedBoundarySI_to_UDM_Delete(transaction_data, transaction_type='Delete'):
    table_name = "uninccommboundary"
    unincorporatedCommunityBoundary = list(transaction_data.get(transaction_type).values())[0]
    paramkeylist = ['UniqueId']

    paravalues = []
    for param in paramkeylist:
        if isinstance(unincorporatedCommunityBoundary[param], str):
            paravalues.append(str(unincorporatedCommunityBoundary[param]))
        elif isinstance(unincorporatedCommunityBoundary[param], bytes):
            paravalues.append(unincorporatedCommunityBoundary[param].decode('utf-8'))
        else:
            paravalues.append(unincorporatedCommunityBoundary[param])

    sql = "Delete from uninccommboundary where srcunqid = '{}';".format(paravalues[0])

    geocomm = postgresdb.DB(database='geocomm')
    action_statement = "Deletion successful for uninccommboundary!!!"
    geocomm.transaction_sql(sql, action_statement)


def MapUnincorporatedBoundarySI_to_UDM_Update(transaction_data, transaction_type='Update'):
    table_name = "uninccommboundary"
    unincorporatedCommunityBoundary = list(transaction_data.get(transaction_type).values())[0]
    srcunqid = unincorporatedCommunityBoundary.get('UniqueId')
    table_map = models.get_table_fields(table_name)
    paramkeylist = table_map.get('paramkeylist')
    mandatory_check = check_mandatory_fields(unincorporatedCommunityBoundary, table_name)

    paravalues = []  # list Comprehension
    if mandatory_check:
        for param in paramkeylist:
            if isinstance(unincorporatedCommunityBoundary[param], str):
                paravalues.append(str(unincorporatedCommunityBoundary[param]))
            elif isinstance(unincorporatedCommunityBoundary[param], bytes):
                paravalues.append(unincorporatedCommunityBoundary[param].decode('utf-8'))
            else:
                paravalues.append(unincorporatedCommunityBoundary[param])

        sql = """UPDATE uninccommboundary SET wkb_geometry = ST_SetSRID(ST_GeomFromGML('{}'),4326), srcunqid = '{}' , srcofdata = '{}' , updatedate = '{}' , effective = '{}' , expire = '{}' , country = '{}' , state = '{}' , county = '{}' , addcode = '{}' , uninccomm = '{}' """.format(
            paravalues[0], paravalues[1], paravalues[2], paravalues[3], paravalues[4], paravalues[5], paravalues[6],
            paravalues[7], paravalues[8], paravalues[9], paravalues[10]) + " WHERE srcunqid = '" + srcunqid + "';"

        geocomm = postgresdb.DB(database='geocomm')
        action_statement = "Update successful for unincorporatedCommunityBoundary!!!"
        geocomm.transaction_sql(sql, action_statement)
    else:
        print('Error!!: Mandatory Field not found!!! Transaction declined')


# ---------------------------------------IncorporatedBoundary-----------------------


def MapIncorporatedBoundarySI_to_UDM_Insert(transaction_data, transaction_type='Insert'):
    table_name = 'incmunicipalboundary'
    IncorporatedMunicipalityBoundary = list(transaction_data.get(transaction_type).values())[0]
    table_map = models.get_table_fields(table_name)
    paramkeylist = table_map.get('paramkeylist')

    paravalues = []
    mandatory_check = check_mandatory_fields(IncorporatedMunicipalityBoundary, TABLE_INCORPORATEDMUNICIPALITYBOUNDARY)
    if mandatory_check:
        for param in paramkeylist:
            if isinstance(IncorporatedMunicipalityBoundary[param], str):  # or isinstance(centerlines[param], bytes):
                paravalues.append(str(IncorporatedMunicipalityBoundary[param]))
            elif isinstance(IncorporatedMunicipalityBoundary[param], bytes):  # or isinstance(centerlines[param], bytes):
                paravalues.append(IncorporatedMunicipalityBoundary[param].decode('utf-8'))
            else:
                paravalues.append(IncorporatedMunicipalityBoundary[param])
        sql = 'Insert into ' + TABLE_INCORPORATEDMUNICIPALITYBOUNDARY + """(wkb_geometry, srcunqid, srcofdata, updatedate, effective, expire, country, state, county, addcode, muni)
        values(ST_SetSRID(ST_GeomFromGML('{}'),4326),'{}','{}','{}','{}','{}','{}','{}','{}','{}','{}');""".format(
            paravalues[0], paravalues[1], paravalues[2], paravalues[3], paravalues[4], paravalues[5], paravalues[6],
            paravalues[7], paravalues[8], paravalues[9], paravalues[10])

        geocomm = postgresdb.DB(database='geocomm')
        action_statement = "Insertion successful for incmunicipalboundary!!!"
        geocomm.transaction_sql(sql, action_statement)
    else:
        print('Error!!: Mandatory Field not found!!! Transaction declined')


def MapIncorporatedBoundarySI_to_UDM_Delete(transaction_data, transaction_type='Delete'):
    table_name = "incmunicipalboundary"
    IncorporatedMunicipalityBoundary = list(transaction_data.get(transaction_type).values())[0]
    paramkeylist = ['UniqueId']

    paravalues = []
    for param in paramkeylist:
        if isinstance(IncorporatedMunicipalityBoundary[param], str):  # or isinstance(centerlines[param], bytes):
            paravalues.append(str(IncorporatedMunicipalityBoundary[param]))
        elif isinstance(IncorporatedMunicipalityBoundary[param], bytes):  # or isinstance(centerlines[param], bytes):
            paravalues.append(IncorporatedMunicipalityBoundary[param].decode('utf-8'))
        else:
            paravalues.append(IncorporatedMunicipalityBoundary[param])

    sql = "Delete from incmunicipalboundary where srcunqid = '{}';".format(paravalues[0])
    geocomm = postgresdb.DB(database='geocomm')
    action_statement = "Deletion successful for IncorporatedMunicipalityBoundary!!!"
    geocomm.transaction_sql(sql, action_statement)


def MapIncorporatedBoundarySI_to_UDM_Update(transaction_data, transaction_type='Update'):
    table_name = "incmunicipalboundary"
    IncorporatedMunicipalityBoundary = list(transaction_data.get(transaction_type).values())[0]
    srcunqid = IncorporatedMunicipalityBoundary.get('UniqueId')
    table_map = models.get_table_fields(table_name)
    paramkeylist = table_map.get('paramkeylist')
    mandatory_check = check_mandatory_fields(IncorporatedMunicipalityBoundary, table_name)

    paravalues = []  # list Comprehension
    if mandatory_check:
        for param in paramkeylist:
            if isinstance(IncorporatedMunicipalityBoundary[param], str):  # or isinstance(centerlines[param], bytes):
                paravalues.append(str(IncorporatedMunicipalityBoundary[param]))
            elif isinstance(IncorporatedMunicipalityBoundary[param], bytes):  # or isinstance(centerlines[param], bytes):
                paravalues.append(IncorporatedMunicipalityBoundary[param].decode('utf-8'))
            else:
                paravalues.append(IncorporatedMunicipalityBoundary[param])

        sql = """ UPDATE incmunicipalboundary SET wkb_geometry = ST_SetSRID(ST_GeomFromGML('{}'),4326), srcunqid = '{}', srcofdata = '{}', updatedate = '{}', effective = '{}', expire = '{}', country = '{}', state = '{}', county = '{}' , addcode = '{}', muni = '{}' """.format(
            paravalues[0], paravalues[1], paravalues[2], paravalues[3], paravalues[4], paravalues[5], paravalues[6],
            paravalues[7], paravalues[8], paravalues[9], paravalues[10]) + " WHERE srcunqid = '" + srcunqid + "';"
        geocomm = postgresdb.DB(database='geocomm')
        action_statement = "Update successful for IncorporatedMunicipalityBoundary!!!"
        geocomm.transaction_sql(sql, action_statement)
    else:
        print('Error!!: Mandatory Field not found!!! Transaction declined')


# ----------------------------------------StateBoundary-------------------------


def MapStateBoundarySI_to_UDM_Insert(transaction_data, transaction_type='Insert'):
    table_name = 'stateboundary'
    stateboundary = list(transaction_data.get(transaction_type).values())[0]
    table_map = models.get_table_fields(table_name)
    paramkeylist = table_map.get('paramkeylist')

    paravalues = []
    mandatory_check = check_mandatory_fields(stateboundary, TABLE_STATEBOUNDARY)
    if mandatory_check:
        for param in paramkeylist:
            if isinstance(stateboundary[param], str):
                paravalues.append(stateboundary[param])
            elif isinstance(stateboundary[param], bytes):
                paravalues.append(stateboundary[param].decode('utf-8'))
            else:
                paravalues.append(stateboundary[param])

        sql = 'Insert into ' + TABLE_STATEBOUNDARY + """(wkb_geometry, srcunqid, srcofdata, updatedate, effective, expire, country, state)
        values(ST_SetSRID(ST_GeomFromGML('{}'),4326),'{}','{}','{}','{}','{}','{}','{}');""".format(paravalues[0], paravalues[1], paravalues[2], paravalues[3], paravalues[4], paravalues[5], paravalues[6], paravalues[7])

        geocomm = postgresdb.DB(database='geocomm')
        action_statement = "Insertion successful for stateboundary!!!"
        geocomm.transaction_sql(sql, action_statement)
    else:
        print('Error!!: Mandatory Field not found!!! Transaction declined')


def MapStateBoundarySI_to_UDM_Delete(transaction_data, transaction_type='Delete'):
    table_name = "stateboundary"
    stateboundary = list(transaction_data.get(transaction_type).values())[0]
    paramkeylist = ['UniqueId']

    paravalues = []
    for param in paramkeylist:
        if isinstance(stateboundary[param], str):
            paravalues.append(stateboundary[param])
        elif isinstance(stateboundary[param], bytes):
            paravalues.append(stateboundary[param].decode('utf-8'))
        else:
            paravalues.append(stateboundary[param])

    sql = "Delete from stateboundary where srcunqid = '{}';".format(paravalues[0])

    geocomm = postgresdb.DB(database='geocomm')
    action_statement = "Deletion successful for stateboundary!!!"
    geocomm.transaction_sql(sql, action_statement)


def MapStateBoundarySI_to_UDM_Update(transaction_data, transaction_type='Update'):
    table_name = "stateboundary"
    stateboundary = list(transaction_data.get(transaction_type).values())[0]
    srcunqid = stateboundary.get('UniqueId')
    table_map = models.get_table_fields(table_name)
    paramkeylist = table_map.get('paramkeylist')
    mandatory_check = check_mandatory_fields(stateboundary, table_name)

    paravalues = []  # list Comprehension
    if mandatory_check:
        for param in paramkeylist:
            if isinstance(stateboundary[param], str):
                paravalues.append(stateboundary[param])
            elif isinstance(stateboundary[param], bytes):
                paravalues.append(stateboundary[param].decode('utf-8'))
            else:
                paravalues.append(stateboundary[param])

        sql = """ UPDATE stateboundary SET wkb_geometry = ST_SetSRID(ST_GeomFromGML('{}'),4326), srcunqid = '{}', srcofdata = '{}', updatedate = '{}', effective = '{}', expire = '{}', country = '{}', state = '{}' """.format(
            paravalues[0], paravalues[1], paravalues[2], paravalues[3], paravalues[4], paravalues[5], paravalues[6],
            paravalues[7]) + " WHERE srcunqid = '" + srcunqid + "';"
        geocomm = postgresdb.DB(database='geocomm')
        action_statement = "Update successful for stateboundary!!!"
        geocomm.transaction_sql(sql, action_statement)
    else:
        print('Error!!: Mandatory Field not found!!! Transaction declined')

# # ----------------------------------------
# def MapSifAdapter_Insert(sifAdapter):
#     if (sifAdapter.centerlines != None):
#         MapCenterlineSI_to_UDM_Insert(sifAdapter.centerlines)

#     if (sifAdapter.countyBoundary != None):
#         MapCountyBoundarySI_to_UDM_Insert(sifAdapter.countyBoundary)

#     if (sifAdapter.siteStructure != None):
#         MapSiteStructureSI_to_UDM_Insert(sifAdapter.siteStructure)

#     if (sifAdapter.unincorporatedCommunityBoundary != None):
#         MapUnincorporatedBoundarySI_to_UDM_Insert(sifAdapter.unincorporatedCommunityBoundary)

#     if (sifAdapter.incorporatedMunicipalityBoundary != None):
#         MapIncorporatedBoundarySI_to_UDM_Insert(sifAdapter.incorporatedMunicipalityBoundary)

#     if (sifAdapter.stateBoundary != None):
#         MapStateBoundarySI_to_UDM_Insert(sifAdapter.stateBoundary)


# def MapSifAdapter_Update(sifAdapter):
#     if (sifAdapter.centerlines != None):
#         MapCenterlineSI_to_UDM_Update(sifAdapter.centerlines)

#     if (sifAdapter.countyBoundary != None):
#         MapCountyBoundarySI_to_UDM_Update(sifAdapter.countyBoundary)

#     if (sifAdapter.siteStructure != None):
#         MapSiteStructureSI_to_UDM_Update(sifAdapter.siteStructure)

#     if (sifAdapter.unincorporatedCommunityBoundary != None):
#         MapUnincorporatedBoundarySI_to_UDM_Update(sifAdapter.unincorporatedCommunityBoundary)

#     if (sifAdapter.incorporatedMunicipalityBoundary != None):
#         MapIncorporatedBoundarySI_to_UDM_Update(sifAdapter.incorporatedMunicipalityBoundary)

#     if (sifAdapter.stateBoundary != None):
#         MapStateBoundarySI_to_UDM_Update(sifAdapter.stateBoundary)


# def MapSifAdapter_Delete(sifAdapter):
#     if (sifAdapter.centerlines != None):
#         MapCenterlineSI_to_UDM_Delete(sifAdapter.centerlines)

#     if (sifAdapter.countyBoundary != None):
#         MapCountyBoundarySI_to_UDM_Delete(sifAdapter.countyBoundary)

#     if (sifAdapter.siteStructure != None):
#         MapSiteStructureSI_to_UDM_Delete(sifAdapter.siteStructure)

#     if (sifAdapter.unincorporatedCommunityBoundary != None):
#         MapUnincorporatedBoundarySI_to_UDM_Delete(sifAdapter.unincorporatedCommunityBoundary)

#     if (sifAdapter.incorporatedMunicipalityBoundary != None):
#         MapIncorporatedBoundarySI_to_UDM_Delete(sifAdapter.incorporatedMunicipalityBoundary)

#     if (sifAdapter.stateBoundary != None):
#         MapStateBoundarySI_to_UDM_Delete(sifAdapter.stateBoundary)

# which will get me previousupdatedate from postgres.replicationFeeds




# --------------Not Currently Used---------
# --------------


def get_service_urn_layer_mapping(transaction_data):
    table_name = "serviceurnlayermapping"
    transaction_type = list(transaction_data.keys())[0]
    import pdb;pdb.set_trace()
    table_map_type = list(transaction_data[transaction_type].keys())[0]
    key = transaction_data[transaction_type][table_map_type].get('ServiceURN')
    conn = connectdb('postgres')
    cursor = conn.cursor()
    sql = "select layername from {} where serviceurn = '{}';".format(table_name, key)
    cursor.execute(sql)
    result = cursor.fetchall()[0][0]  # result will vbe police | fire | sheriiff
    if not result:
        print("Error: postgres.serviceurnlayermapping has no rows in the table!!!")
    return result


def MapServiceBoundarySI_to_UDM_Insert(transaction_data, transaction_type='Insert'):
    import pdb;pdb.set_trace()
    geocomm = postgresdb.DB(database='geocomm')
    table_name = geocomm.get_service_urn_layer_mapping(transaction_data)
    ServiceBoundary = list(transaction_data.get(transaction_type).values())[0]
    table_map = models.get_table_fields(table_name)
    paramkeylist = table_map.get('paramkeylist')

    paravalues = []
    mandatory_check = check_mandatory_fields(ServiceBoundary, table_name)
    if mandatory_check:
        for param in paramkeylist:
            if isinstance(ServiceBoundary[param], str):
                paravalues.append(ServiceBoundary[param])
            elif isinstance(ServiceBoundary[param], bytes):
                paravalues.append(ServiceBoundary[param].decode('utf-8'))
            else:
                paravalues.append(ServiceBoundary[param])
        sql = 'Insert into ' + table_name + """(wkb_geometry, srcunqid, srcofdata, updatedate, effective, expire, country, state, county, agencyid, routeuri, serviceurn, servicenum, vcarduri, displayname) values(
    ST_SetSRID(ST_GeomFromGML('{}'),4326),'{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}',
    '{}');""".format(paravalues[0], paravalues[1], paravalues[2], paravalues[3], paravalues[4], paravalues[5],
                     paravalues[6], paravalues[7], paravalues[8], paravalues[9], paravalues[10], paravalues[11],
                     paravalues[12],
                     paravalues[13], paravalues[14])
        
        action_statement = "Insertion of values into stateboundary table sucessfull"
        geocomm.transaction_sql(sql, action_statement)
    else:
        print('Error!!: Mandatory Field not found!!! Transaction declined')


def MapServiceBoundarySI_to_UDM_Update(transaction_data, transaction_type='Update'):
    geocomm = postgresdb.DB(database='geocomm')
    table_name = geocomm.get_service_urn_layer_mapping(transaction_data)
    ServiceBoundary = list(transaction_data.get(transaction_type).values())[0]
    table_map = models.get_table_fields(table_name)
    paramkeylist = table_map.get('paramkeylist')
    paravalues = []
    srcunqid = ServiceBoundary.get('UniqueId')    
    mandatory_check = check_mandatory_fields(ServiceBoundary, table_name)
    if mandatory_check:
        for param in paramkeylist:
            if isinstance(ServiceBoundary[param], str):
                paravalues.append(ServiceBoundary[param])
            elif isinstance(ServiceBoundary[param], bytes):
                paravalues.append(ServiceBoundary[param].decode('utf-8'))
            else:
                paravalues.append(ServiceBoundary[param])

        sql = """  UPDATE {} SET  wkb_geometry = ST_SetSRID(ST_GeomFromGML('{}'),4326), srcunqid = '{}', srcofdata = '{}', updatedate = '{}', effective = '{}', expire = '{}', country = '{}', state = '{}', county = '{}', agencyid = '{}', routeuri = '{}', serviceurn = '{}', servicenum = '{}', vcarduri = '{}', displayname = '{}'  """.format(
        table_name, paravalues[0], paravalues[1], paravalues[2], paravalues[3], paravalues[4], paravalues[5],
        paravalues[6], paravalues[7], paravalues[8], paravalues[9], paravalues[10], paravalues[11], paravalues[12],
        paravalues[13], paravalues[14]) + " WHERE srcunqid = '" + srcunqid + "';"
        action_statement = "Update values into stateboundary table sucessfull"
        geocomm.transaction_sql(sql, action_statement)
    else:
        print('Error!!: Mandatory Field not found!!! Transaction declined')


def MapServiceBoundarySI_to_UDM_Delete(transaction_data, transaction_type='Delete'):
    geocomm = postgresdb.DB(database='geocomm')
    table_name = geocomm.get_service_urn_layer_mapping(transaction_data)
    ServiceBoundary = list(transaction_data.get(transaction_type).values())[0]
    paramkeylist = ['UniqueId']

    paravalues = []
    for param in paramkeylist:
        if isinstance(ServiceBoundary[param], str):
            paravalues.append(ServiceBoundary[param])
        elif isinstance(ServiceBoundary[param], bytes):
            paravalues.append(ServiceBoundary[param].decode('utf-8'))
        else:
            paravalues.append(ServiceBoundary[param])

    sql = "Delete from {} where srcunqid = '{}';".format(table_name, paravalues[0])
    action_statement = "Deletion of  values into stateboundary table sucessfull"
    geocomm.transaction_sql(sql, action_statement)



# -------------------------------------
TABLE_Roadalias = "roadalias"


def AddAliasCenterlineParameters_Insert(aliasStree):
    sql = 'Insert into ' + TABLE_Roadalias + """(srcunqid, rsrcunqid, srcofdata, updatedate, effective, expire, apremod, apredir, apretype, apretypesep, astrname, aposttype, apostdir, apostmod )
        values(ST_SetSRID(ST_GeomFromGML(%s),4326),%s,%s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"""

    paramkeylist = ['RoadSegmentUniqueId',
                    'SourceofData',
                    'DateUpdated',
                    'EffectiveDate',
                    'ExpirationDate',
                    'StreetNamePreModifier',
                    'StreetNamePreDirectional',
                    'StreetNamePreType',
                    'StreetNamePreTypeSeparator'
                    'StreetName',
                    'StreetNamePostType',
                    'StreetNamePostDirectional',
                    'StreetNamePostModifier']

    paravalues = [aliasStree[x] for x in paramkeylist]  # list Comprehension

    # centerlins={'ESNRight':'us','ESNleft':'uk'}
    # parakeys=['ESNLeft','ESNRight']
    # paravalues=['us','uk'] after the list comprehension


    try:
        cursor = connectdb(str(DB.POSTGRES)).cursor()
        cursor.execute(sql, paravalues)
        con.commit()
        print("AliasStree inserted")
    except psycopg2.Error as e:
        print(e.pgerror)
    finally:
        con.close()


def AddAliasCenterlineParameters_Update(aliasStree):
    sql = 'Update into ' + TABLE_Roadalias + """(srcunqid, rsrcunqid, srcofdata, updatedate, effective, expire, apremod, apredir, apretype, apretypesep, astrname, aposttype, apostdir, apostmod )
        values(ST_SetSRID(ST_GeomFromGML(%s),4326),%s,%s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"""

    paramkeylist = ['RoadSegmentUniqueId',
                    'SourceofData',
                    'DateUpdated',
                    'EffectiveDate',
                    'ExpirationDate',
                    'StreetNamePreModifier',
                    'StreetNamePreDirectional',
                    'StreetNamePreType',
                    'StreetNamePreTypeSeparator'
                    'StreetName',
                    'StreetNamePostType',
                    'StreetNamePostDirectional',
                    'StreetNamePostModifier']

    paravalues = [aliasStree[x] for x in paramkeylist]  # list Comprehension

    try:
        cursor = connectdb(str(DB.POSTGRES)).cursor()
        cursor.execute(sql, paravalues)
        con.commit()
        print("AliasStree Updated")
    except psycopg2.Error as e:
        print(e.pgerror)
    finally:
        con.close()


def AddAliasCenterlineParameters_Delete(aliasStree):
    sql = 'Delete from ' + TABLE_Roadalias + """(srcunqid, rsrcunqid, srcofdata, updatedate, effective, expire, apremod, apredir, apretype, apretypesep, astrname, aposttype, apostdir, apostmod )
        values(ST_SetSRID(ST_GeomFromGML(%s),4326),%s,%s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"""

    paramkeylist = ['RoadSegmentUniqueId',
                    'SourceofData',
                    'DateUpdated',
                    'EffectiveDate',
                    'ExpirationDate',
                    'StreetNamePreModifier',
                    'StreetNamePreDirectional',
                    'StreetNamePreType',
                    'StreetNamePreTypeSeparator'
                    'StreetName',
                    'StreetNamePostType',
                    'StreetNamePostDirectional',
                    'StreetNamePostModifier']

    paravalues = [aliasStree[x] for x in paramkeylist]  # list Comprehension

    try:
        cursor = connectdb(str(DB.POSTGRES)).cursor()
        cursor.execute(sql, paravalues)
        con.commit()
        print("AliasStree Delete")
    except psycopg2.Error as e:
        print(e.pgerror)
    finally:
        con.close()
        # ---------------------------------------------------------------


def check_mandatory_fields(transaction_data, table_name):
    mandatory_check_flag = True
    table_map = models.get_table_fields(table_name)
    mandatory_fields = table_map.get('mandatory_fields')
    paramkeylist = [key.lower() for key in table_map.get('paramkeylist')]
    transaction_data_keys = [key.lower() for key in list(transaction_data.keys())]
    table_keys = [key.lower() for key in mandatory_fields]
    mf_check = len(set(transaction_data_keys) & set(table_keys)) == len(table_keys)
    if mf_check:
        for field in transaction_data:
            if not transaction_data.get(field) and (field.lower() not in paramkeylist):
                mandatory_check_flag = False
                break
    else:
        mandatory_check_flag = False
    return mandatory_check_flag


# def MapServiceBoundarySI_to_UDM_Update(transaction_data,)