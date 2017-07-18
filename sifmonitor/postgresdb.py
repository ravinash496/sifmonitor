import json
import os
from datetime import datetime
# import serviceurnlayermapping
import psycopg2
# from numpy.linalg.tests.test_linalg import a
import sqlalchemy
from datetime import datetime
from numpy.linalg.tests.test_linalg import a
from sqlalchemy import *
from sqlalchemy.orm import sessionmaker, scoped_session
import settings
import transaction_mapper
from logger_settings import *

CREDENTIAL_FILE = 'connection.json'
xml_log_history = "provisioninghistory.json"
PG_CREDENTIAL_FILE = 'pg_connection.json'
transactions_file = 'transactions_file.txt'


def read_transactions():
    try:
        with open(transactions_file, 'r') as fp:
            data = fp.readlines()
        return data
    except IOError as ie:
        logger.error(ie)


def get_databases():
    """Get credentials from connection.json file
    :return: databases
    """
    try:
        with open(CREDENTIAL_FILE, 'r') as fp:
            data = json.load(fp)
            databases = list(data.keys())
        return databases
    except IOError as ie:
        logger.error(ie)


def execute_sql(sql, fetch=None):
    """Execute any general sequence statements and if fetch parameter is provided it returns the output"""
    db = DB()
    credentials = settings.read_json(settings.CREDENTIAL_FILE).get('srgis')
    engine = db.connect(credentials)
    try:
        with engine.connect() as con:
            res = con.execute(sql)
            if fetch:
                result = res.fetchall()
                return result
    except Exception as error:
        logger.error(error)
        exit()


class DB:
    def __init__(self):
        """Python constructor to initialize the object"""
        pass
        # self.database = database
        # self.credentials = settings.read_json(json_file=CREDENTIAL_FILE).get(self.database)

    def get_nextval(self, next_val_sql):
        """ Get the nextval from sequence for ogc_fid field from public sequences"""
        credentials = settings.read_json(settings.CREDENTIAL_FILE).get('srgis')
        engine = self.connect(credentials)
        try:
            with engine.connect() as con:
                res = con.execute(next_val_sql)
                result = res.fetchall()
                return result[0][0]

        except Exception as error:
            print(error)

    def get_all_table_names(self, schema_name):
        sql = """SELECT table_name FROM information_schema.tables
                 WHERE table_schema='{}';""".format(schema_name)
        res = execute_sql(sql, fetch=True)
        tables = [r[0] for r in res]
        return tables

    # Insert/Update the table
    # def transaction_sql(self, sql, action_statement, commit_flag=None):

    def transaction_sql(self):
        """ Execute SQL command for the table
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
            transactions = "".join(transaction_mapper.TRANSACTION_RESULTS['sql'])
            try:
                # execute the transactions
                logger.info("Connecting to postgresDB to execute the transactions in provisioning schema")
                connection.execute(transactions)
                trans.commit()
                self.log_modification_history()
            
            except sqlalchemy.exc.IntegrityError as pse:
                logger.error(":: Duplicate key value violates unique constraint, gcunqid already exists!!!")
                os.remove(settings.application_flag)
                exit()
            
            except Exception as error:
                # if we have any connection failure
                trans.rollback()
                transaction_mapper.TRANSACTION_RESULTS = {}
                logger.error(error)
                os.remove(settings.application_flag)
                exit()
            finally:
                connection.close()

    # Get the replicationfeed values from the replicationfeed table in postgres
    def get_replicationfeeds(self):
        credentials = settings.read_json(settings.PG_CREDENTIAL_FILE).get('postgres')
        engine = self.connect(credentials)
        try:
            with engine.connect() as con:
                sql = "Select id, name, uri, refreshrate, lastupdateprocesed From public.replicationfeeds;"
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
        databases = get_databases()
        for database in databases:
            credentials = settings.read_json(settings.CREDENTIAL_FILE).get(database)
            engine = self.connect(credentials)
            try:
                with engine.connect() as con:
                    sql = """update public.replicationfeeds SET lastupdateprocesed = '{}' WHERE lastupdateprocesed = '{}' AND id = '{}'; """.format(
                        timestamp_update_date, previous_time_stamp, item_id)
                    con.execute(sql)
                    logger.info(
                        "UPDATE SUCCESS: replicationfeed `lastupdateprocesed` values was updated from `{}` to `{}`".format(
                            previous_time_stamp, timestamp_update_date))
            except Exception as error:
                logger.error(error)
                exit()

    def get_service_urn_layer_mapping(self, transaction_data):
        """Get the service urn layer mapping for a specififed table/service"""
        credentials = settings.read_json(settings.PG_CREDENTIAL_FILE).get('postgres')
        table_name = "serviceurnlayermapping"
        transaction_type = list(transaction_data.keys())[0]
        table_map_type = list(transaction_data[transaction_type].keys())[0]
        key = transaction_data[transaction_type][table_map_type].get('ServiceURN')
        engine = self.connect(credentials)
        try:
            with engine.connect() as con:
                sql = "select layername from public.{} where serviceurn = '{}';".format(table_name, key)
                res = con.execute(sql)
                result = res.fetchone()[0]  # result will vbe police | fire | sheriff
                return result
        except Exception as error:
            logger.error(error)

    def connect(self, credentials):
        """Returns a connection and a metadata object"""
        # We connect with the help of the PostgreSQL URL
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
            exit()

    def log_modification_history(self):
        """Log the modification of xml entires in a history table and provisioninghistory.json file for tracing back the changes
        :return: None
        """
        db = DB()
        table_names = db.get_all_table_names("provisioning")
        databases = get_databases()
        for database in databases:
            credentials = settings.read_json(settings.CREDENTIAL_FILE).get(database)
            modified_tables = list(set((table_name for table_name in transaction_mapper.TRANSACTION_RESULTS.keys() if table_name in table_names)))
            sql_statements = []
            for table_name in modified_tables:
                sql = """Insert into public.provisioninghistory(table_name, modified_count, last_modified) values('{}', {}, '{}'); """.format(table_name, transaction_mapper.TRANSACTION_RESULTS[table_name], datetime.now())
                sql_statements.append(sql)
            sql_statements = "".join(sql_statements)

            # Write to a temporary json file for future references
            with open(xml_log_history, 'a+') as fp:
                json.dump(sql_statements, fp)
                fp.write("\n")

            # Write to a Database
            try:
                execute_sql(sql_statements)
                logger.info("Inserted the modifications for tables successfully into provisioning history table!!")
            except Exception as error:
                logger.error(error)
                exit()
