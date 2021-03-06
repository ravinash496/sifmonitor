from sqlalchemy import *
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql import select, or_
import os
import settings
from logger_settings import *


class MappingDiscoveryException(Exception):
    """
    Raised when something goes wrong in the process of discovering
    the service urn to table mappings.

    :param message: The exception message
    :type message:  ``str``
    :param nested: Nested exception, if any.
    :type nested: 
    """
    def __init__(self, message, nested=None):
        super().__init__(message)
        self._nested = nested


def get_serviceurn(tablename, engine):

    """

    Gets the service urn from a emergency services boundary table.
    :param tablename: The name of the ESB table
    :type tablename: ``str``
    :param engine: An instance of the database engine.
    :type engine:  :py:class:`sqlalchemy.engine.Engine`
    :return: ``str``
    """
    urn = None
    try:
        result = None
        tbl_metadata = MetaData(bind=engine)
        esb_table = Table(tablename, tbl_metadata, autoload=True)
        q = select([esb_table.c.serviceurn]).distinct()
        with engine.connect() as conn:
            result = conn.execute(q)
            rows = result.fetchall()
            if len(rows) > 1:
                logger.error('MappingDiscoveryException: Table {0} contained more than one service urn: {1}'.format(tablename, rows))
            urn = rows[0]['serviceurn']
    except SQLAlchemyError as ex:
        logger.error('MappingDiscoveryException: Failed to extract mapping for table {0}'.format(tablename), ex)
        try:
            os.remove(settings.application_flag)
            exit()
        except OSError:
            pass
    except MappingDiscoveryException as ex:
        logger.error('MappingDiscoveryException: {}'.format(ex))
        try:
            os.remove(settings.application_flag)
            exit()
        except OSError:
            pass
    return urn


def get_urn_table_mappings(engine):
    """
    Inspects the database and extracts the service urn to table mappings.
    :param engine: An instance of the database engine.
    :type engine:  :py:class:`sqlalchemy.engine.Engine`
    :return: A dictionary containing the service URNs as keys and associated table names as values
    :rtype: ``dict``
    """
    mappings = {}
    try:
        result = None
        metadata = MetaData(bind=engine, schema='information_schema')
        info_table = Table('tables', metadata, autoload=True)
        s = select([info_table.c.table_name]).where(
            or_(
                info_table.c.table_name.like('esb%'),
                info_table.c.table_name.like('aloc%')))
        with engine.connect() as conn:
            result = conn.execute(s)
            for row in result:
                tablename = row['table_name']
                urn = get_serviceurn(tablename, engine)
                mappings[urn] = tablename
            if not mappings:
                logger.error('MappingDiscoveryException: No service boundary tables were found in the database.')
    except SQLAlchemyError as ex:
        logger.error('MappingDiscoveryException: Encountered an error when attempting to discover the service boundary tables.', ex)
        try:
            os.remove(settings.application_flag)
            exit()
        except OSError:
            pass
    except MappingDiscoveryException as ex:
        logger.error('MappingDiscoveryException: {}'.format(ex))
        try:
            os.remove(settings.application_flag)
            exit()
        except OSError:
            pass
    return mappings
