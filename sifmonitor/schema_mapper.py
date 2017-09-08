skip_fields = ['RightToAddressNumber', 'StreetNamePreModifier', 'StreetNamePreTypeSeparator', 'LeftFromAddressNumber',
               'StreetNamePreType', 'LeftAddressNumberPrefix', 'ParityLeft', 'StreetNamePostDirectional',
               'RightFromAddressNumber', 'ValidationLeft', 'Milepost', 'AddressNumberSuffix', 'AddressNumberPrefix',
               'LeftAddressNumberSuffix', 'RightAddressNumberPrefix', 'RightAddressNumberSuffix', 'AddressNumber',
               'StreetNamePostType', 'LeftToAddressNumber', 'ParityRight', 'ValidationRight', 'StreetNamePostModifier',
               'StreetName', 'StreetNamePreDirectional']


def get_table_fields(table_name):
    # fields are database fields
    # paramkeylist is from XML
    # mandatory_fields are the mandatory fields in that particular table
    table_mapping = {
        'roadcenterline':
            {'fields': ['wkb_geometry', 'gcunqid', 'srcofdata', 'premod', 'predir', 'pretype', 'pretypesep', 'strname',
                        'posttype', 'postdir', 'postmod', 'addrngprel', 'addrngprer', 'fromaddl', 'fromaddr', 'toaddl',
                        'toaddr', 'parityl', 'parityr', 'updatedate', 'effective', 'expire', 'countryl', 'countryr',
                        'statel', 'stater', 'countyl', 'countyr', 'addcodel', 'addcoder', 'incmunil', 'incmunir',
                        'uninccomml', 'uninccommr', 'nbrhdcomml', 'nbrhdcommr', 'roadclass', 'speedlimit', 'oneway',
                        'postcomml', 'postcommr', 'zipcodel', 'zipcoder', 'esnl', 'esnr'],
             'paramkeylist': ['LineGeometry', 'UniqueId', 'SourceofData', 'StreetNamePreModifier',
                              'StreetNamePreDirectional', 'StreetNamePreType', 'StreetNamePreTypeSeparator',
                              'StreetName', 'StreetNamePostType', 'StreetNamePostDirectional', 'StreetNamePostModifier',
                              'LeftAddressNumberPrefix', 'RightAddressNumberPrefix', 'LeftFromAddressNumber',
                              'RightFromAddressNumber', 'LeftToAddressNumber', 'RightToAddressNumber', 'ParityLeft',
                              'ParityRight', 'DateUpdated', 'EffectiveDate', 'ExpirationDate', 'CountryLeft',
                              'CountryRight', 'StateLeft', 'StateRight', 'CountyLeft', 'CountyRight',
                              'AdditionalCodeLeft', 'AdditionalCodeRight', 'IncorporatedMunicipalityNameLeft',
                              'IncorporatedMunicipalityNameRight', 'UnincorporatedCommunityNameRight',
                              'UnincorporatedCommunityNameLeft', 'NeighborhoodCommunityNameRight',
                              'NeighborhoodCommunityNameLeft', 'RoadClass', 'SpeedLimit', 'OneWay',
                              'PostalCommunityNameLeft', 'PostalCommunityNameRight', 'PostalCodeLeft',
                              'PostalCodeRight', 'ESNLeft', 'ESNRight'],
             'mandatory_fields': ['UniqueId', 'SourceofData', 'StreetName', 'LeftFromAddressNumber',
                                  'RightFromAddressNumber', 'LeftToAddressNumber', 'RightToAddressNumber', 'ParityLeft',
                                  'ParityRight', 'DateUpdated', 'CountryLeft', 'CountryRight', 'StateRight',
                                  'StateLeft', 'CountyLeft', 'CountyRight', 'IncorporatedMunicipalityNameLeft',
                                  'IncorporatedMunicipalityNameRight', 'RoadClass', 'ESNLeft', 'ESNRight']},

        'incmunicipalboundary':
            {'fields': ['wkb_geometry', 'gcunqid', 'srcofdata', 'updatedate', 'effective', 'expire', 'country', 'state',
                        'county', 'addcode', 'muni'],
             'paramkeylist': ['PolygonGeometry', 'UniqueId', 'SourceofData', 'DateUpdated', 'EffectiveDate',
                              'ExpirationDate', 'Country', 'State', 'County', 'AdditionalCode',
                              'IncorporatedMunicipalityName'],
             'mandatory_fields': ['UniqueId', 'SourceofData', 'DateUpdated', 'Country', 'State', 'County',
                                  'IncorporatedMunicipalityName']},

        'countyboundary':
            {'fields': ['wkb_geometry', 'gcunqid', 'srcofdata', 'updatedate', 'effective', 'expire', 'country', 'state',
                        'county'],
             'paramkeylist': ['PolygonGeometry', 'UniqueId', 'SourceofData', 'DateUpdated', 'EffectiveDate',
                              'ExpirationDate', 'Country', 'State', 'County'],
             'mandatory_fields': ['UniqueId', 'SourceofData', 'DateUpdated', 'Country', 'State', 'County']},

        'stateboundary':
            {'fields': ['wkb_geometry', 'gcunqid', 'srcofdata', ' updatedate', 'effective', 'expire', 'country',
                        'state'],
             'paramkeylist': ['PolygonGeometry', 'UniqueId', 'SourceofData', 'DateUpdated', 'EffectiveDate',
                              'ExpirationDate', 'Country', 'State'],
             'mandatory_fields': ['UniqueId', 'SourceofData', 'DateUpdated', 'Country', 'State']},

        'uninccommboundary':
            {'fields': ['wkb_geometry', 'gcunqid', 'srcofdata', 'updatedate', 'effective', 'expire', 'country', 'state',
                        'county', 'addcode', 'uninccomm'],
             'paramkeylist': ['PolygonGeometry', 'UniqueId', 'SourceofData', 'DateUpdated', 'EffectiveDate',
                              'ExpirationDate', 'Country', 'State', 'County', 'AdditionalCode',
                              'UnincorporatedCommunityName'],
             'mandatory_fields': ['UniqueId', 'SourceofData', 'DateUpdated', 'Country', 'State', 'County',
                                  'UnincorporatedCommunityName']},

        'ssap':
            {'fields': ['wkb_geometry', 'gcunqid', 'srcofdata', 'updatedate', 'effective', 'expire,country', 'state',
                        'county', 'addcode', 'incmuni', 'uninccomm', 'nbrhdcomm', 'premod', 'predir', 'pretype',
                        'pretypesep', 'strname', 'posttype', 'postdir', 'postmod', 'addnumpre', 'addnum', 'addnumsuf',
                        'milepost', 'esn', 'postcomm', 'zipcode', 'building', 'floor', 'unit', 'room', 'seat',
                        'landmark', 'location', 'placetype', 'adddatauri'],
             'paramkeylist': ['PointGeometry', 'UniqueId', 'SourceofData', 'DateUpdated', 'EffectiveDate',
                              'ExpirationDate', 'Country', 'State', 'County', 'AdditionalCode',
                              'IncorporatedMunicipalityName', 'UnincorporatedCommunityName',
                              'NeighborhoodCommunityName', 'StreetNamePreModifier', 'StreetNamePreDirectional',
                              'StreetNamePreType', 'StreetNamePreTypeSeparator', 'StreetName', 'StreetNamePostType',
                              'StreetNamePostDirectional', 'StreetNamePostModifier', 'AddressNumberPrefix',
                              'AddressNumber', 'AddressNumberSuffix', 'Milepost', 'ESN', 'PostalCommunityName',
                              'PostalCode', 'Building', 'Floor', 'Unit', 'Room', 'Seat', 'CompleteLandmarkName',
                              'AdditionalLocationInformation', 'PlaceType', 'AdditionalLocationDataURI'],
             'mandatory_fields': ['UniqueId', 'SourceofData', 'DateUpdated', 'Country', 'State', 'County',
                                  'IncorporatedMunicipalityName', 'ESN', 'PostalCode']},
        'serviceboundary':
            {'fields': ['wkb_geometry', 'gcunqid', 'srcofdata', 'updatedate', 'effective', 'expire', 'country', 'state',
                        'county', 'agencyid', 'routeuri', 'serviceurn', 'servicenum', 'vcarduri', 'displayname'],
             'paramkeylist': ['PolygonGeometry', 'UniqueId', 'SourceofData', 'DateUpdated', 'EffectiveDate',
                              'ExpirationDate', 'Country', 'State', 'County', 'AgencyId', 'ServiceURI', 'ServiceURN',
                              'ServiceNumber', 'AgencyVCardURI', 'DisplayName'],
             'mandatory_fields': ['UniqueId', 'SourceofData', 'DateUpdated', 'Country', 'State', 'County', 'AgencyId',
                                  'ServiceURI', 'ServiceURN', 'AgencyVCardURI', 'DisplayName']}
    }

    return table_mapping[table_name]
