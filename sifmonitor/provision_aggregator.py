from lxml import etree
import urllib.request
from datetime import datetime
import transaction_mapper
import postgresdb
import threading
import time
from pprint import pprint
import sys


def download_atom_feed():
    global ns1
    global url
    global fp
    postgres = postgresdb.DB(database='postgres')
    feedList = postgres.get_replicationfeeds() 
    
    # Check if values were retrieved from replicationfeed table or else push some dummy row into local
    try:
        if feedList:
            lastupdateprocessed = feedList[4]
            url = feedList[2]
            fp = urllib.request.urlopen(url)
        else:
            print("No values retrieved for postgres.replicationfeed table")
            lastupdateprocessed = feedList[4]
            url = feedList[2]
            fp = urllib.request.urlopen(url)
    except Exception as error:
        print("\nERROR!!!:  {}".format(error))
    
    ns1 = {"atom": "http://www.w3.org/2005/Atom", "gml": "http://www.opengis.net/gml",
           "georss": "http://www.opengis.net/georss", "wfs": "http://www.opengis.org/wfs"}

    # Parse the XML file and validate if there has been any changes to timestamp
    parse_create_entry(lastupdateprocessed)
    if not transaction_flag:
        print("No changes in XML, nothing to update!!!")


def parse_create_entry(previousupdatedate):
    """Parse the XML and create the necessary entries"""
    transactionList = []
    entryList = []
    insertList = []
    transactionType = []
    transactionDict = {}
    geoTypeList = []
    geoTypeList2 = []
    dataDict = {}
    update_list = []
    updatedbflag = False
    transaction_results = []
    updatedflag = False
    global transaction_flag 
    transaction_flag= False

    # loops through the SOM element
    for event, element in etree.iterparse(fp):
        if (event == 'end' and element.tag == '{http://www.w3.org/2005/Atom}updated'):
            # Get both updatedate tag from xml file
            update_list.append(element.text)
            if(len(update_list) > 1):
                # convert the string date to datetime objetc type for both updatetimstamp from xml file
                updatedTime = datetime.strptime(update_list[0], "%Y-%m-%dT%H:%M:%Sz").replace(tzinfo=None)
                updatedTimeelement = datetime.strptime(update_list[1], "%Y-%m-%dT%H:%M:%Sz").replace(tzinfo=None)

                previous_replicationupdate = datetime.strftime(previousupdatedate, "%Y-%m-%dT%H:%M:%S")
                # # Check if XML timestamp(T1) and replicationFeed Updatedate timestamp differs,
                # # if they differ update DB timestamp with XML file timestamp
                if updatedTime > previousupdatedate.replace(tzinfo=None) and not updatedbflag:
                    updatedbflag = True
                    timestamp_update_date = update_list[0].split('Z')[0]
                    postgres = postgresdb.DB(database='postgres')
                    postgres.update_last_processed(timestamp_update_date, previousupdatedate)
                    # latestupdatedate = transaction_mapper.getReplicationFeed()

                # Break from the entry tag1 when both update at fields are matching with DB update and go to next entry tag
                if (updatedTime == previousupdatedate.replace(tzinfo=None)) and (updatedTimeelement == previousupdatedate.replace(tzinfo=None)):
                    update_list.pop(1)
                    updatedflag = False
                    continue

                # check DB updatetime and xml update time and update flag and loop through entry or sub nodes
                if (updatedTimeelement > previousupdatedate.replace(tzinfo=None)):
                    #(updatedTime > previousupdatedate.replace(tzinfo=None)) or (updatedTimeelement > previousupdatedate.replace(tzinfo=None)):# previously set to > now set to !=
                    updatedflag = True
                else:
                    update_list.pop(1)
                    continue

                # Pop the second element from update_list to filter out different updatetags
                update_list.pop(1)

        # print(element.tag)
        # Based on the updateflag , if set to true thn only perform transactions, or else continue to next entry
        if event == 'end' and element.tag == '{http://www.w3.org/2005/Atom}entry' and updatedflag:
            transaction_flag = True
            # print ('entry')
            for e in element:
                for content in e: #browse through the content to get the trsanction
                    print(content.tag)
                    if content.tag == "{http://www.opengis.org/wfs}Transaction":
                        # transactionList.append(content),transactionList.append(content)
                        for transactiontp in content:  # insert,update or delete
                            transactionType.append(etree.QName(transactiontp.tag).localname)
                            for geoType in transactiontp: #SIFAdapter, Centerline etc
                                geoTypeList.append(etree.QName(geoType.tag).localname)
                                if etree.QName(geoType.tag).localname!='SIFAdapter':
                                    geoType=transactiontp
                                for geoType2 in geoType:
                                    geoTypeList2.append(geoType2.tag)  # centerline, stateboundery ect
                                    for data in geoType2.iter():
                                        # if etree.QName(data.tag).localname=='LineGeometry':
                                        if etree.QName(data.tag).namespace == ns1['gml']:
                                            if etree.QName(data.getparent()).localname in dataDict.keys():
                                                del dataDict[etree.QName(data.getparent()).localname]
                                            dataDict[etree.QName(data.getparent()).localname] = etree.tostring(data,encoding='utf-8',method="xml")
                                            break

                                    for data in geoType2.iter():
                                        if etree.QName(data.tag).localname in dataDict.keys():
                                            continue
                                        if etree.QName(data.tag).namespace == ns1['gml']:
                                            continue
                                        dataDict[etree.QName(data.tag).localname] = data.text
                                    transactionDict[(etree.QName(transactiontp.tag).localname)]={(etree.QName(geoType.tag).localname,geoType2.tag.replace('{urn:nena:xml:ns:SIFProvisioningExchange:2.0}','')):dataDict}
                                    # Perform one transaction at a time
                                    transaction(transactionDict, transactionType)
                                    #run_thread_instance(transaction, (transactionDict, transactionType))
                                    # Empty the below variables
                                    transactionType = geoTypeList = []
                                    transactionDict = {}
                                    del geoType
                                    del geoType2
                                    del data
                                    del transactiontp
                                    del content
                                    del e
                                    del element
        else:
            continue


def transaction(transactionDict, transactionType):
    transaction_types = transactionDict.keys()
    if 'Insert' in transaction_types:
        TransactionInsert(transactionDict, transactionType)
    elif "Update" in transaction_types:
        TransactionUpdate(transactionDict, transactionType)
    elif "Delete" in transaction_types:
        TransactionDelete(transactionDict, transactionType)
    else:
        print("\nERROR: Unexpected transaction occurred.")


def TransactionUpdate(transactionDict, transactionType):
    transactionDictTypes = list(transactionDict.get('Update').keys())
    if ('SIFAdapter', 'Centerlines') in transactionDictTypes:
        transaction_mapper.MapCenterlineSI_to_UDM_Update(transactionDict)
    elif ('Update', 'Centerlines') in transactionDictTypes:
        transaction_mapper.MapCenterlineSI_to_UDM_Update(transactionDict),
    elif ('SIFAdapter', 'CountyBoundary') in transactionDictTypes:
        transaction_mapper.MapCountyBoundarySI_to_UDM_Update(transactionDict)
    elif ('Update', 'CountyBoundary') in transactionDictTypes:
        transaction_mapper.MapCountyBoundarySI_to_UDM_Update(transactionDict)
    elif ('SIFAdapter', 'SiteStructure')in transactionDictTypes:
        transaction_mapper.MapSiteStructureSI_to_UDM_Update(transactionDict)
    elif ('Update', 'SiteStructure') in transactionDictTypes:
        transaction_mapper.MapSiteStructureSI_to_UDM_Update(transactionDict)
    elif ('SIFAdapter', 'UnincorporatedCommunityBoundary') in transactionDictTypes:
        transaction_mapper.MapUnincorporatedBoundarySI_to_UDM_Update(transactionDict)
    elif ('SIFAdapter', 'IncorporatedMunicipalityBoundary') in transactionDictTypes:
        transaction_mapper.MapIncorporatedBoundarySI_to_UDM_Update(transactionDict)
    elif ('Update', 'IncorporatedMunicipalityBoundary') in transactionDictTypes:
        transaction_mapper.MapIncorporatedBoundarySI_to_UDM_Update(transactionDict)
    elif('Update', 'UnincorporatedCommunityBoundary')in transactionDictTypes:
        transaction_mapper.MapUnincorporatedBoundarySI_to_UDM_Update(transactionDict)
    elif ('SIFAdapter', 'StateBoundary') in transactionDictTypes:
        transaction_mapper.MapStateBoundarySI_to_UDM_Update(transactionDict)
    elif('Update', 'StateBoundary')in transactionDictTypes:
        transaction_mapper.MapStateBoundarySI_to_UDM_Update(transactionDict)


def TransactionInsert(transactionDict, transactionType):
    transactionDictTypes = list(transactionDict.get('Insert').keys())
    if ('SIFAdapter', 'Centerlines') in transactionDictTypes:
        transaction_mapper.MapCenterlineSI_to_UDM_Insert(transactionDict)
    elif ('Insert', 'Centerlines') in transactionDictTypes:
        transaction_mapper.MapCenterlineSI_to_UDM_Insert(transactionDict),
    elif ('SIFAdapter', 'CountyBoundary') in transactionDictTypes:
        transaction_mapper.MapCountyBoundarySI_to_UDM_Insert(transactionDict)
    elif ('Insert', 'CountyBoundary') in transactionDictTypes:
        transaction_mapper.MapCountyBoundarySI_to_UDM_Insert(transactionDict)
    elif ('SIFAdapter', 'SiteStructure')in transactionDictTypes:
        transaction_mapper.MapSiteStructureSI_to_UDM_Insert(transactionDict)
    elif ('Insert', 'SiteStructure') in transactionDictTypes:
        transaction_mapper.MapSiteStructureSI_to_UDM_Insert(transactionDict)
    elif ('SIFAdapter', 'UnincorporatedCommunityBoundary') in transactionDictTypes:
        transaction_mapper.MapUnincorporatedBoundarySI_to_UDM_Insert(transactionDict)
    elif ('SIFAdapter', 'IncorporatedMunicipalityBoundary') in transactionDictTypes:
        transaction_mapper.MapIncorporatedBoundarySI_to_UDM_Insert(transactionDict)
    elif ('Insert', 'IncorporatedMunicipalityBoundary') in transactionDictTypes:
        transaction_mapper.MapIncorporatedBoundarySI_to_UDM_Insert(transactionDict)
    elif('Insert', 'UnincorporatedCommunityBoundary')in transactionDictTypes:
        transaction_mapper.MapUnincorporatedBoundarySI_to_UDM_Insert(transactionDict)
    elif ('SIFAdapter', 'StateBoundary') in transactionDictTypes:
        transaction_mapper.MapStateBoundarySI_to_UDM_Insert(transactionDict)
    elif('Insert', 'StateBoundary') in transactionDictTypes:
        transaction_mapper.MapStateBoundarySI_to_UDM_Insert(transactionDict)
    elif('Insert', 'ServiceBoundary') in transactionDictTypes:
        transaction_mapper.MapServiceBoundarySI_to_UDM_Insert(transactionDict)


def TransactionDelete(transactionDict, transactionType):
    transactionDictTypes = list(transactionDict.get('Delete').keys())
    if ('SIFAdapter', 'Centerlines') in transactionDictTypes:
        transaction_mapper.MapCenterlineSI_to_UDM_Delete(transactionDict)
    elif ('Delete', 'Centerlines') in transactionDictTypes:
        transaction_mapper.MapCenterlineSI_to_UDM_Delete(transactionDict)
    elif ('SIFAdapter', 'CountyBoundary') in transactionDictTypes:
        transaction_mapper.MapCountyBoundarySI_to_UDM_Delete(transactionDict)
    elif ('Delete', 'CountyBoundary') in transactionDictTypes:
        transaction_mapper.MapCountyBoundarySI_to_UDM_Delete(transactionDict)
    elif ('SIFAdapter', 'SiteStructure') in transactionDictTypes:
        transaction_mapper.MapSiteStructureSI_to_UDM_Delete(transactionDict)
    elif ('Delete', 'SiteStructure') in transactionDictTypes:
        transaction_mapper.MapSiteStructureSI_to_UDM_Delete(transactionDict)
    elif ('SIFAdapter', 'UnincorporatedCommunityBoundary') in transactionDictTypes:
        transaction_mapper.MapUnincorporatedBoundarySI_to_UDM_Delete(transactionDict)
    elif ('SIFAdapter', 'IncorporatedMunicipalityBoundary') in transactionDictTypes:
        transaction_mapper.MapIncorporatedBoundarySI_to_UDM_Delete(transactionDict)
    elif ('Delete', 'IncorporatedMunicipalityBoundary') in transactionDictTypes:
        transaction_mapper.MapIncorporatedBoundarySI_to_UDM_Delete(transactionDict)
    elif ('Delete', 'UnincorporatedCommunityBoundary') in transactionDictTypes:
        transaction_mapper.MapUnincorporatedBoundarySI_to_UDM_Delete(transactionDict)
    elif ('SIFAdapter', 'StateBoundary') in transactionDictTypes:
        transaction_mapper.MapStateBoundarySI_to_UDM_Delete(transactionDict)
    elif ('Delete', 'StateBoundary') in transactionDictTypes:
        transaction_mapper.MapStateBoundarySI_to_UDM_Delete(transactionDict)
    elif ('Delete', 'ServiceBoundary') in transactionDictTypes:
        transaction_mapper.MapServiceBoundarySI_to_UDM_Delete(transactionDict)


def run_thread_instance(function_name, arguments):
    t = threading.Thread(target=function_name, args=arguments)
    t.start()


def schedule_thread():
    """Run downloadAtomfeed() every 30 seconds and schedule as a seperate thread"""
    while 1:
        t = threading.Thread(target=downloadAtomFeed)
        time.sleep(30)
        t.start()


if __name__ == "__main__":
    # schedule_thread()  # Schedule as a thread which runs as a backend job
    download_atom_feed()
