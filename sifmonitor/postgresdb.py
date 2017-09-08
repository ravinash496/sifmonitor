import json
import os
import time
from datetime import datetime
import psycopg2
import sqlalchemy
from sqlalchemy import *
from sqlalchemy.orm import sessionmaker, scoped_session

import settings
import transaction_mapper
from logger_settings import *

CREDENTIAL_FILE = 'connection.json'
xml_log_history = "provisioninghistory.json"


# def read_transactions():
#     try:
#         with open(transactions_file, 'r') as fp:
#             data = fp.readlines()
#         return data
#     except IOError as ie:
#         logger.error(ie)


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


# def execute_sql(sql, fetch=None):
#     """Execute any general sequence statements and if fetch parameter is provided it returns the output"""
#     db = DB()
#     databases = get_databases()
#     for database in databases:
#         credentials = settings.read_json(settings.CREDENTIAL_FILE).get(database)
#         engine = db.connect(credentials)
#         try:
#             with engine.connect() as con:
#                 res = con.execute(sql)
#                 if fetch:
#                     result = res.fetchall()
#                     return result
#         except Exception as error:
#             logger.error(error)
#             exit()


def execute_sql(sql, fetch=None):
    """Execute any general sequence statements and if fetch parameter is provided it returns the output"""
    exception_flag = False
    db = DB()
    databases = get_databases()
    for database in databases:
        credentials = settings.read_json(settings.CREDENTIAL_FILE).get(database)
        engine = db.connect(credentials)
        try:
            with engine.connect() as con:
                res = con.execute(sql)
                if fetch:
                    result = res.fetchall()
                    return result
        except Exception as error:
            exception_flag = True
            logger.error(error)
            return exception_flag


def retry_execute_sql(sql, databases=None, fetch=None, retry=None):
    """Execute any general sequence statements and if fetch parameter is provided it returns the output"""
    sql_exception_flag = False

    retry_exception_flag = False
    retry_exception_fail = False
    successful_dbs = []
    sql = """ALTER SCHEMA active RENAME TO bogus;
             ALTER SCHEMA provisioning RENAME TO active; 
             ALTER SCHEMA bogus RENAME TO provisioning;
         """

    db = DB()
    if not databases and not retry:
        databases = get_databases()
    for database in databases:
        credentials = settings.read_json(settings.CREDENTIAL_FILE).get(database)
        engine = db.connect(credentials)
        try:
            with engine.connect() as con:
                res = con.execute(sql)
                successful_dbs.append(database)
                if not retry:
                    logger.info("Running flip sql query!!")
                else:
                    logger.info("Re-Flipped back the db which were partially successful on Exception!")

                    retry_exception_flag = True
                if fetch:
                    result = res.fetchall()
                    return result
        except sqlalchemy.exc.DatabaseError as sqlerror:
            # time.sleep(2)
            logger.error(sqlerror)
            if retry:
                retry_exception_flag = True
            sql_exception_flag = True
    if retry and retry_exception_flag:
        logger.error("FLip SQL retry also failed!!! needs to be fixed!!!")
        return retry_exception_flag, []
    return sql_exception_flag, successful_dbs


class DB:
    def __init__(self):
        """Python constructor to initialize the object"""
        pass

    def get_lastval(self, last_val_sql):
        """ Get the lastval from sequence for ogc_fid field from public sequences"""
        databases = get_databases()
        for database in databases:
            credentials = settings.read_json(settings.CREDENTIAL_FILE).get(database)
            engine = self.connect(credentials)
            try:
                with engine.connect() as con:
                    res = con.execute(last_val_sql)
                    result = res.fetchall()
                    return result[0][0]

            except Exception as error:
                logger.error(error)
                os.remove(settings.application_flag)

    # get all table names from the given Schema
    def get_all_table_names(self, schema_name):
        sql = """SELECT table_name FROM information_schema.tables
                 WHERE table_schema='{}';""".format(schema_name)
        res = execute_sql(sql, fetch=True)
        tables = [r[0] for r in res]
        return tables

    # Insert/Update the table
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
            # Read transactions from transaction data structure
            transactions = "".join(transaction_mapper.TRANSACTION_RESULTS['sql'])
            try:
                # Execute the transactions
                logger.info("Connecting to postgresDB to execute the transactions in provisioning schema")
                connection.execute(transactions)
                trans.commit()
                self.log_modification_history()
            
            except sqlalchemy.exc.IntegrityError as pse:
                logger.error(":: Duplicate key value violates unique constraint, gcunqid already exists!!!")
                os.remove(settings.application_flag)
                exit()
            
            except Exception as error:
                # Rollback incase of any connection failure
                trans.rollback()
                transaction_mapper.TRANSACTION_RESULTS = {}
                logger.error(error)
                os.remove(settings.application_flag)
                exit()
            finally:
                connection.close()

    # Get the replicationfeed values from the replicationfeed table in postgres
    def get_replicationfeeds(self):
        credentials = settings.read_json(settings.CREDENTIAL_FILE).get('srgis')
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

    # Update the replicationFeed updatelastprocessed based on root_updatetimestamp in XML
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
