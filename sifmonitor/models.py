
def get_table_fields(table_name):
    table_mapping = { 
        'centerline': 
            {'fields': ['wkb_geometry', 'srcunqid', 'srcofdata', 'premod', 'predir', 'pretype', 'pretypesep', 'strname', 'posttype', 'postdir', 'postmod', 'addrngprel', 'addrngprer', 'fromaddl', 'fromaddr', 'toaddl', 'toaddr', 'parityl', 'parityr', 'updatedate', 'effective', 'expire', 'countryl', 'countryr', 'statel', 'stater', 'countyl', 'countyr', 'addcodel', 'addcoder', 'incmunil', 'incmunir', 'uninccomml', 'uninccommr', 'nbrhdcomml', 'nbrhdcommr', 'roadclass', 'speedlimit', 'oneway', 'postcomml', 'postcommr', 'zipcodel',   'zipcoder', 'esnl', 'esnr'],
             'paramkeylist': ['LineGeometry', 'RoadSegmentUniqueId', 'SourceofData', 'StreetNamePreModifier', 'StreetNamePreDirectional', 'StreetNamePreType', 'StreetNamePreTypeSeparator', 'StreetName', 'StreetNamePostType', 'StreetNamePostDirectional', 'StreetNamePostModifier', 'LeftAddressNumberPrefix', 'RightAddressNumberPrefix', 'LeftFromAddressNumber', 'RightFromAddressNumber', 'LeftToAddressNumber', 'RightToAddressNumber', 'ParityLeft', 'ParityRight', 'DateUpdated', 'EffectiveDate', 'ExpirationDate', 'CountryLeft', 'CountryRight', 'StateLeft', 'StateRight', 'CountyLeft', 'CountyRight', 'AdditionalCodeLeft', 'AdditionalCodeRight', 'IncorporatedMunicipalityNameLeft', 'IncorporatedMunicipalityNameRight', 'UnincorporatedCommunityNameRight', 'UnincorporatedCommunityNameLeft', 'NeighborhoodCommunityNameRight', 'NeighborhoodCommunityNameLeft', 'RoadClass', 'SpeedLimit', 'OneWay', 'PostalCommunityNameLeft', 'PostalCommunityNameRight', 'PostalCodeLeft', 'PostalCodeRight', 'ESNLeft', 'ESNRight'],
             'mandatory_fields': ['RoadSegmentUniqueId', 'SourceofData', 'STREETNAME', 'LEFTFROMADDRESSNUMBER', 'RIGHTFROMADDRESSNUMBER', 'LEFTTOADDRESSNUMBER', 'RIGHTTOADDRESSNUMBER', 'PARITYLEFT', 'PARITYRIGHT', 'DATEUPDATED', 'COUNTRYLEFT', 'COUNTRYRIGHT', 'STATERIGHT', 'STATELEFT', 'COUNTYLEFT', 'COUNTYRIGHT', 'INCORPORATEDMUNICIPALITYNAMELEFT', 'INCORPORATEDMUNICIPALITYNAMERIGHT', 'ROADCLASS', 'ESNLEFT', 'ESNRIGHT']},
        
        'incmunicipalboundary': 
            {'fields': ['wkb_geometry', 'srcunqid', 'srcofdata', 'updatedate', 'effective', 'expire', 'country', 'state', 'county', 'addcode', 'muni'],
             'paramkeylist': ['PolygonGeometry', 'UniqueId', 'SourceofData', 'DateUpdated', 'EffectiveDate', 'ExpirationDate', 'Country', 'State', 'County', 'AdditionalCode', 'IncorporatedMunicipalityName'],
             'mandatory_fields': ['UNIQUEID', 'SOURCEOFDATA', 'DATEUPDATED', 'COUNTRY', 'STATE', 'COUNTY', 'INCORPORATEDMUNICIPALITYNAME']},

        'countyboundary':
            {'fields': ['wkb_geometry', 'srcunqid', 'srcofdata', 'updatedate', 'effective', 'expire', 'country', 'state', 'county'],
            'paramkeylist': ['PolygonGeometry', 'UniqueId', 'SourceofData', 'DateUpdated', 'EffectiveDate', 'ExpirationDate', 'Country', 'State', 'County'],
            'mandatory_fields': ['UNIQUEID', 'SOURCEOFDATA', 'DATEUPDATED', 'COUNTRY', 'STATE', 'COUNTY']},

        'stateboundary':
            {'fields': ['wkb_geometry', 'srcunqid', 'srcofdata', ' updatedate', 'effective', 'expire', 'country', 'state'],
             'paramkeylist': ['PolygonGeometry', 'UniqueId', 'SourceofData', 'DateUpdated', 'EffectiveDate', 'ExpirationDate', 'Country', 'State'],
             'mandatory_fields': ['UNIQUEID', 'SOURCEOFDATA', 'DATEUPDATED', 'COUNTRY', 'STATE']},

        'uninccommboundary':
            {'fields': ['wkb_geometry', 'srcunqid', 'srcofdata', 'updatedate', 'effective', 'expire', 'country', 'state', 'county', 'addcode', 'uninccomm'],
             'paramkeylist': ['PolygonGeometry', 'UniqueId', 'SourceofData', 'DateUpdated', 'EffectiveDate', 'ExpirationDate', 'Country', 'State', 'County', 'AdditionalCode', 'UnincorporatedCommunityName'],
             'mandatory_fields': ['UNIQUEID', 'SOURCEOFDATA', 'DATEUPDATED', 'COUNTRY', 'STATE', 'COUNTY', 'UNINCORPORATEDCOMMUNITYNAME']},

        'ssap':
            {'fields': ['wkb_geometry', 'srcunqid', 'srcofdata', 'updatedate', 'effective', 'expire,country', 'state', 'county', 'addcode', 'incmuni', 'uninccomm', 'nbrhdcomm', 'premod', 'predir', 'pretype', 'pretypesep', 'strname', 'posttype', 'postdir', 'postmod', 'addnumpre', 'addnum', 'addnumsuf', 'milepost', 'esn', 'postcomm', 'zipcode', 'building', 'floor', 'unit', 'room', 'seat', 'landmark', 'location', 'placetype', 'adddatauri'],
             'paramkeylist': ['PointGeometry', 'UniqueId', 'SourceofData', 'DateUpdated', 'EffectiveDate', 'ExpirationDate', 'Country', 'State', 'County', 'AdditionalCode', 'IncorporatedMunicipalityName',  'UnincorporatedCommunityName', 'NeighborhoodCommunityName', 'StreetNamePreModifier',  'StreetNamePreDirectional', 'StreetNamePreType', 'StreetNamePreTypeSeparator', 'StreetName',  'StreetNamePostType', 'StreetNamePostDirectional', 'StreetNamePostModifier',  'AddressNumberPrefix', 'AddressNumber', 'AddressNumberSuffix', 'Milepost', 'ESN', 'PostalCommunityName', 'PostalCode', 'Building', 'Floor', 'Unit', 'Room', 'Seat', 'CompleteLandmarkName', 'AdditionalLocationInformation', 'PlaceType', 'AdditionalLocationDataURI'],
             'mandatory_fields': ['UNIQUEID', 'SOURCEOFDATA', 'DATEUPDATED', 'COUNTRY', 'STATE', 'COUNTY', 'INCORPORATEDMUNICIPALITYNAME', 'ESN', 'POSTALCODE']},
        'serviceboundary':
            {'fields': ['wkb_geometry', 'srcunqid', 'srcofdata', 'updatedate', 'effective', 'expire', 'country', 'state',   'county', 'agencyid', 'routeuri', 'serviceurn', 'servicenum', 'vcarduri', 'displayname'],
             'paramkeylist': ['PolygonGeometry', 'UniqueId', 'SourceofData', 'DateUpdated', 'EffectiveDate', 'ExpirationDate', 'Country', 'State', 'County', 'AgencyId', 'ServiceURI', 'ServiceURN', 'ServiceNumber', 'AgencyVCardURI', 'DisplayName'],
             'mandatory_fields': ['UNIQUEID', 'SOURCEOFDATA', 'DATEUPDATED', 'COUNTRY', 'STATE', 'COUNTY', 'AGENCYID', 'SERVICEURI', 'SERVICEURN', 'AGENCYVCARDURI', 'DISPLAYNAME']}
}

    return table_mapping[table_name]
