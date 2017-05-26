
import sqlalchemy
from sqlalchemy import *
import json
import settings

CREDENTIAL_FILE = 'connection2.json'

class DB:
    def __init__(self, database):
        """Python constructor to initialize the object"""
        self.database = database
        self.credentials = settings.read_json(json_file=CREDENTIAL_FILE).get(self.database)

    # Insert/Update the table
    def transaction_sql(self, sql, action_statement):
        engine = self.connect() 
        try:
            with engine.connect() as con:
                con.execute(sql)
                print(action_statement)
        except Exception as error:
            print(error)
            
    def get_centerline():
        pass
       
    def get_replicationfeeds(self):
        engine = self.connect() 
        try:
            with engine.connect() as con:
                sql = "Select id, name, uri, refreshrate, lastupdateprocesed From replicationfeeds;"
                res = con.execute(sql)
                feed = res.fetchone()
                return feed
        except Exception as error:
            print(error)
        
    
    def update_last_processed(self, timestamp_update_date, previous_time_stamp):
        """Update the replicationFeed updatelastprocessed based on XML"""
        engine = self.connect()
        try:
            with engine.connect() as con:
                sql = """update replicationfeeds SET lastupdateprocesed = '{}' WHERE lastupdateprocesed = '{}'; """.format(timestamp_update_date,     
        previous_time_stamp)
                con.execute(sql)
                print("UPDATE SUCCESS: replicationfeed `lastupdateprocesed` values was updated from `{}` to `{}`".format(previous_time_stamp,        timestamp_update_date)) 
        except Exception as error:
            print(error)
    
    
    def get_service_url_layer_mapping(transaction_data):
        table_name = "serviceurnlayermapping"
        key = transaction_data['Insert'][('Insert', 'ServiceBoundary')].get('ServiceURN')
        engine = self.connect()
        try:
            with engine.connect() as con:
                sql = "select layername from {} where serviceurn = '{}';".format(table_name, key)
                res = con.execute(sql)
                result = res.fetchone()[0] # result will vbe police | fire | sheriiff
                return result
        except Exception as error:
            print(error)

    def connect(self):
        '''Returns a connection and a metadata object'''
        # We connect with the help of the PostgreSQL URL
        # postgresql://username:yourpasswd@localhost:5432/databasename
        url = 'postgresql://{}:{}@{}:{}/{}'
        
        user = self.credentials.get('user')
        password = self.credentials.get('password')
        host = self.credentials.get('host')
        port = self.credentials.get('port')
        db = self.credentials.get('db')
        
        url = url.format(user, password, host, port, db)
        engine = create_engine(url)
        # The return value of create_engine() is our connection object
        return engine


