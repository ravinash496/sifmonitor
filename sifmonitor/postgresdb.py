import json
import os
import uuid
from datetime import datetime
import sqlalchemy
from sqlalchemy import *
import provision_aggregator
import settings
import transaction_mapper
from logger_settings import *
import pytz

xml_log_history = "provisioninghistory.json"
config = settings.read_config_file()


def get_databases(database_name=None):
    """Get credentials from .ini file
    :return: databases
    """
    try:
        if not database_name:
            data = eval(config["Database"]["dbs"])
        else:
            data = eval(config[database_name]["dbs"])
        databases = list(data.keys())
        return databases
    except IOError as ie:
        logger.error(ie)


def execute_sql(sql, fetch=None):
    """Execute any general sequence statements and if fetch parameter is provided it returns the output"""
    db = DB()
    databases = get_databases()
    for database in databases:
        credentials = eval(config["Database"]["dbs"])[database]
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


def retry_execute_sql(sql, databases=None, fetch=None, retry=None):
    """Execute any general sequence statements and if fetch parameter is provided it returns the output"""
    sql_exception_flag = False

    retry_exception_flag = False
    retry_exception_fail = False
    successful_dbs = []
    sql = """
            BEGIN;
            ALTER SCHEMA active RENAME TO bogus;
            ALTER SCHEMA {0} RENAME TO active;
            ALTER SCHEMA bogus RENAME TO {0};
            COMMIT;
        """.format(settings.target_schema)

    db = DB()
    if not databases and not retry:
        databases = get_databases()
    for database in databases:
        credentials = eval(config["Database"]["dbs"])[database]
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
            credentials = eval(config["Database"]["dbs"])[database]
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
        """
        Get all Table name from required schema
        :param schema_name: 
        :return: 
        """
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
            # credentials = settings.read_json(settings.CREDENTIAL_FILE).get(database)
            credentials = eval(config["Database"]["dbs"])[database]
            engine = self.connect(credentials)
            connection = engine.connect()
            trans = connection.begin()
            # Read transactions from transaction data structure
            if transaction_mapper.TRANSACTION_RESULTS.get('sql'):
                transactions = "".join(transaction_mapper.TRANSACTION_RESULTS['sql'])
            else:
                logger.info("INFO: No transaction SQL statements  to execute")
                exit()
            try:
                # Execute the transactions
                logger.info("Connecting to postgresDB to execute the transactions in provisioning schema")
                connection.execute(transactions)
                trans.commit()
            except sqlalchemy.exc.IntegrityError as pse:
                now = datetime.now(tz=pytz.utc)
                status = 'fail'
                message = 'Duplicate key value violates unique constraint, srcunqid already exists'
                self.log_modification_history(now, now, status, message)
                logger.error(" Duplicate key value violates unique constraint srcunqid already exists!!!")
                try:
                    os.remove(settings.application_flag)
                except OSError:
                    pass
                exit()

            except Exception as error:
                # Rollback in case of any connection failure
                trans.rollback()
                transaction_mapper.TRANSACTION_RESULTS = {}
                logger.error(error)
                try:
                    os.remove(settings.application_flag)
                except OSError:
                    pass
                exit()
            finally:
                connection.close()


    # Update the replicationFeed updatelastprocessed based on root_updatetimestamp in XML
    def update_last_processed(self, timestamp_update_date, previous_time_stamp, item_id):
        """Update the replicationFeed updatelastprocessed based on XML"""
        databases = get_databases()
        for database in databases:
            credentials = eval(config["Database"]["dbs"])[database]
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

    # update provisioning history table
    def log_modification_history(self, start_time, end_time, status="success", message=""):
        """Log the modification of xml entires in a history table and provisioninghistory.json file for tracing back the changes
        :return: None
        """
        db = DB()
        provisioning_type = 'Incremental_provisioning'
        unique_ID = uuid.uuid4()
        table_names = db.get_all_table_names(settings.target_schema)

        modified_tables = list(set((table_name for table_name in transaction_mapper.TRANSACTION_RESULTS.keys() if
                                        table_name in table_names)))

        if modified_tables:
            sql_statements = []
            for table_name in modified_tables:

                sql = """INSERT INTO public.provisioning_history(id, layer, load_type, row_count, start_time, end_time, status, messages) VALUES('{}','{}', '{}', '{}', '{}', '{}','{}','{}');""".format(unique_ID, table_name, provisioning_type, transaction_mapper.TRANSACTION_RESULTS[table_name], start_time, end_time, status, message)

                sql_statements.append(sql)
            sql_statements = "".join(sql_statements)

            # Write to a temporary json file for future references
            with open(xml_log_history, 'a+') as fp:
                json.dump(sql_statements, fp)
                fp.write("\n")

            # Write to a Database
            try:
                databases = get_databases("LoggingDB")
                for database in databases:
                    credentials = eval(config["LoggingDB"]["dbs"])[database]
                    engine = db.connect(credentials)

                    with engine.connect() as con:
                        res = con.execute(sql_statements)
                        logger.info("Inserted the modifications for tables successfully into provisioning history table!!")
            except Exception as error:
                logger.error(error)
                exit()
        else:
            logger.info("No Update in Feed")

