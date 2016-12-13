"""This module contains the code that touches the database directly

All of the code in other modules interfaces with the database through the
classes and functions in this module."""
import logging

from typing import (
    Dict,
    List,
    Union
)

from sqlalchemy import create_engine
from sqlalchemy import engine as sqla_engine
from sqlalchemy_utils import database_exists
from sqlalchemy.sql import (
    Delete,
    Insert,
    Select,
    Update
)

from star_schema.utilities import pprint_sql

module_logger = logging.getLogger('app.' + __name__)


def create_sqlite_db(eng: sqla_engine) -> None:
    logger = module_logger.getChild('create_sqlite_db')
    try:
        from star_schema import md
        md.create_all(eng)
    except Exception as e:
        logger.debug(
            'Unable to create database; error {}'
            .format(e)
        )


def is_sqlite(con_str: str) -> bool:
    return 'sqlite' in con_str


def get_engine(con_str: str) -> sqla_engine:
    logger = module_logger.getChild('get_engine')
    if not con_str:
        err_msg = 'No connection string was provided'
        logger.error(err_msg)
        raise AttributeError(err_msg)
    try:
        engine = create_engine(con_str, echo=False)
        if not database_exists(engine.url):
            if is_sqlite(con_str):
                create_sqlite_db(eng=engine)
            else:
                logger.debug(
                    'The database at path {} could not be found.'
                    .format(con_str)
                )
        return engine
    except Exception as e:
        if is_sqlite(con_str):
            try:
                eng = create_engine(con_str, echo=False)
                create_sqlite_db(eng)
                return eng
            except:
                raise
        else:
            logger.debug(
                'The sqlachemy engine could not be instantiated; '
                'error {}'.format(e)
            )
            raise


class Transaction:
    def __init__(self, con_str: str) -> None:
        self.logger = module_logger.getChild('Transaction')

        self.connection = get_engine(con_str=con_str).connect()
        self.transaction = self.connection.begin()
        self.rows_added = 0
        self.rows_deleted = 0
        self.rows_updated = 0

    def execute(self, cmd: Union[Delete, Insert, Update]):
        self.logger.debug('execute:\n{}'.format(pprint_sql(cmd)))
        try:
            result = self.connection.execute(cmd)
            if type(cmd) == Delete:
                self.rows_deleted += 1
                return 0
            elif type(cmd) == Insert:
                self.rows_added += 1
                return result.inserted_primary_key[0]
            elif type(cmd) == Update:
                self.rows_updated += 1
                return 0
            return 0
        except:
            self.transaction.rollback()
            self.connection.close()
            raise

    def commit(self) -> Dict[str, int]:
        self.transaction.commit()
        self.connection.close()
        return {
            'rows_added':   self.rows_added,
            'rows_deleted': self.rows_deleted,
            'rows_updated': self.rows_updated
        }


def fetch(qry: Select, con_str: str) -> List[str]:
    logger = module_logger.getChild('fetch')
    try:
        engine = get_engine(con_str)
        con = engine.connect()
        logger.debug(pprint_sql(qry))
        return con.execute(qry).fetchall()
    except Exception as e:
        logger.error(
            'fetch: Unable to run the query {}; con_str: {}; error {}'
            .format(pprint_sql(qry), con_str, e)
        )
        raise
    finally:
        con.close()

