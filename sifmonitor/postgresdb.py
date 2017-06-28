
import sqlalchemy
from sqlalchemy import *
from sqlalchemy.orm import sessionmaker, scoped_session
import json
import settings

CREDENTIAL_FILE = 'connection.json'


class DB:
    def __init__(self, database):
        """Python constructor to initialize the object"""
        self.database = database
        self.credentials = settings.read_json(json_file=CREDENTIAL_FILE).get(self.database)

    # Insert/Update the table
    def transaction_sql(self, sql, action_statement, commit_flag=None):
        engine = self.connect() 
        #connection = engine.connect()
        #trans = connection.begin()
        try:
            #connection.execute(sql)
            Session = sessionmaker()
            sess = Session(bind=engine)
            sess.execute(sql)
            sess.commit()

            # If commit_flag is set then only commit
            if commit_flag:
                sess.commit()
            print(action_statement)
        
        except Exception as error:
            print(error)
            #trans.rollback()
        # finally:
        #    connection.close()
        # return trans


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


    def update_last_processed(self, timestamp_update_date, previous_time_stamp, item_id):
        """Update the replicationFeed updatelastprocessed based on XML"""
        engine = self.connect()
        try:
            with engine.connect() as con:
                sql = """update replicationfeeds SET lastupdateprocesed = '{}' WHERE lastupdateprocesed = '{}' AND id = '{}'; """.format(timestamp_update_date, previous_time_stamp, item_id)
                con.execute(sql)
                print("UPDATE SUCCESS: replicationfeed `lastupdateprocesed` values was updated from `{}` to `{}`".format(previous_time_stamp,        timestamp_update_date)) 
        except Exception as error:
            print(error)

    
    def get_service_urn_layer_mapping(self, transaction_data):
        table_name = "serviceurnlayermapping"
        transaction_type = list(transaction_data.keys())[0]
        table_map_type = list(transaction_data[transaction_type].keys())[0]
        key = transaction_data[transaction_type][table_map_type].get('ServiceURN')
        engine = self.connect()
        try:
            with engine.connect() as con:
                sql = "select layername from {} where serviceurn = '{}';".format(table_name, key)
                res = con.execute(sql)
                result = res.fetchone()[0]  # result will vbe police | fire | sheriff
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


