import json
from datetime import datetime

from numpy.linalg.tests.test_linalg import a
from sqlalchemy import *
from sqlalchemy.orm import sessionmaker, scoped_session

import settings
import transaction_mapper
from logger_settings import *

CREDENTIAL_FILE = 'connection.json'
PG_CREDENTIAL_FILE = 'pg_connection.json'
transactions_file = 'transactions_file.txt'
xml_log_history = "provisioninghistory.txt"
table_names = ['roadcenterline', 'ssap'] #@TODO: Put in all tablenames

def read_transactions():
    try:
        with open(transactions_file, 'r') as fp:
            data = fp.readlines()
        return data
    except IOError as ie:
        logger.error(ie)


def get_databases():
    try:
        with open(CREDENTIAL_FILE, 'r') as fp:
            data = json.load(fp)
            databases = list(data.keys())
        return databases
    except IOError as ie:
        logger.error(ie)


class DB:
    def __init__(self):
        """Python constructor to initialize the object"""
        pass
        # self.database = database
        # self.credentials = settings.read_json(json_file=CREDENTIAL_FILE).get(self.database)

    # Insert/Update the table
    # def transaction_sql(self, sql, action_statement, commit_flag=None):
    def transaction_sql(self):
        """
        Execute SQL command for the table
        :return: None
        """
        # Transaction execution logic only for postgres DB
        databases = get_databases()
        for database in databases:
            credentials = settings.read_json(settings.CREDENTIAL_FILE).get(database)
            engine = self.connect(credentials)
            connection = engine.connect()
            trans = connection.begin()
            # Read transactions from transaction.txt file
            #transactions = read_transactions()
            # transactions = transaction_mapper.SQL_STATEMENTS
            transactions = "".join(transaction_mapper.TRANSACTION_RESULTS['sql'])
            try:
                # execute the transactions
                connection.execute(transactions)
                # logger.info("DB {} :: {}".format(database, transactions))
                trans.commit()

            except Exception as error:
                # if we have any connection failure  
                trans.rollback()
                logger.error(error)
                exit()
            finally:
                connection.close()
                self.log_modification_history()

    # Get the replicationfeed values from the replicationfeed table in postgres
    def get_replicationfeeds(self):
        credentials = settings.read_json(settings.PG_CREDENTIAL_FILE).get('postgres')
        engine = self.connect(credentials)
        try:
            with engine.connect() as con:
                sql = "Select id, name, uri, refreshrate, lastupdateprocesed From replicationfeeds;"
                res = con.execute(sql)
                feed = res.fetchone()
                if feed:
                    return feed
                else:
                    logger.error("No data retrieved from replicationfeed table!!!")
                    exit()
        except Exception as error:
            logger.error(error)
            exit()

    def update_last_processed(self, timestamp_update_date, previous_time_stamp, item_id):
        """Update the replicationFeed updatelastprocessed based on XML"""
        credentials = settings.read_json(settings.PG_CREDENTIAL_FILE).get('postgres')
        engine = self.connect(credentials)
        try:
            with engine.connect() as con:
                sql = """update replicationfeeds SET lastupdateprocesed = '{}' WHERE lastupdateprocesed = '{}' AND id = '{}'; """.format(
                    timestamp_update_date, previous_time_stamp, item_id)
                con.execute(sql)
                logger.info(
                    "UPDATE SUCCESS: replicationfeed `lastupdateprocesed` values was updated from `{}` to `{}`".format(
                        previous_time_stamp, timestamp_update_date))
        except Exception as error:
            logger.error(error)
            exit()

    def get_service_urn_layer_mapping(self, transaction_data):
        credentials = settings.read_json(settings.PG_CREDENTIAL_FILE).get('postgres')
        table_name = "serviceurnlayermapping"
        transaction_type = list(transaction_data.keys())[0]
        table_map_type = list(transaction_data[transaction_type].keys())[0]
        key = transaction_data[transaction_type][table_map_type].get('ServiceURN')
        engine = self.connect(credentials)
        try:
            with engine.connect() as con:
                sql = "select layername from {} where serviceurn = '{}';".format(table_name, key)
                res = con.execute(sql)
                result = res.fetchone()[0]  # result will vbe police | fire | sheriff
                return result
        except Exception as error:
            logger.error(error)
            exit()

    def connect(self, credentials):
        """Returns a connection and a metadata object"""
        # We connect with the help of the PostgreSQL URL
        # postgresql://username:yourpasswd@localhost:5432/databasenameopen(transactions_file, 'w').close()
        url = 'postgresql://{}:{}@{}:{}/{}'
        user = credentials.get('user')
        password = credentials.get('password')
        host = credentials.get('host')
        port = credentials.get('port')
        db = credentials.get('db')
        url = url.format(user, password, host, port, db)
        try:
            engine = create_engine(url)
            # The return value of create_engine() is our connection object
            return engine
        except Exception as error:
            logger.error(error)
            # open(transactions_file, 'w').close()
            exit()

    def log_modification_history(self):
        """
        Log the modification of xml entires in a history table for tracing back the changes
        :return: None
        """
        credentials = settings.read_json(settings.PG_CREDENTIAL_FILE).get('postgres')
        modified_tables = list(set((table_name for table_name in transaction_mapper.TRANSACTION_RESULTS.keys() if table_name in table_names)))
        #list(set(transaction_mapper.TRANSACTION_RESULTS.keys())))# - set(['sql']))
        engine = self.connect(credentials)
        sql_statements = []
        for table_name in modified_tables:
            sql = """Insert into provisioninghistory(table_name, modified_count, last_modified) values('{}', {}, '{}'); """.format(table_name, transaction_mapper.TRANSACTION_RESULTS[table_name], datetime.now())
            sql_statements.append(sql)
        sql_statements = "".join(sql_statements)
        
        # Write to a temporary file for future references
        with open(xml_log_history, 'a+') as fp:
            fp.writelines(sql_statements)
        # Write to a Database
        try:
            with engine.connect() as con:
                con.execute(sql_statements)
                logger.info("Inserted the modifications for tables successfully into provisioning history table!!")
        except Exception as error:
            logger.error(error)
            exit()

