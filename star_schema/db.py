"""This module contains the code that touches the database directly

All of the code in other modules interfaces with the database through the
classes and functions in this module."""
from typing import (
    Dict,
    List,
    Union
)

from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists
from sqlalchemy.sql import (
    Delete,
    Insert,
    Select,
    Update
)

from star_schema.config import cfg, ConfigError
from logger import log_error


def get_engine():
    engine = create_engine(cfg.app.db_path, echo=False)
    if not database_exists(engine.url):
        if 'sqlite' in cfg.app.db_path:
            from star_schema import md
            md.create_all(engine)
        else:
            raise ConfigError('The database at path {} could not be found.'
                              .format(cfg.app.db_path))
    return engine

eng = get_engine()


class Transaction:
    def __init__(self) -> None:
        self.connection = eng.connect()
        self.transaction = self.connection.begin()
        self.rows_added = 0
        self.rows_deleted = 0
        self.rows_updated = 0

    def execute(self, cmd: Union[Delete, Insert, Update]):
        try:
            from sqlalchemy.dialects import sqlite
            print(cmd.compile(dialect=sqlite.dialect(), compile_kwargs={"literal_binds": True}))
        except Exception as e:
            print("Unable to compose sql statement: cmd {}; err {}"
                  .format(str(cmd), str(e)))
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


@log_error
def fetch(qry: Select) -> List[str]:
    con = eng.connect()
    try:
        from sqlalchemy.dialects import sqlite
        print(qry.compile(dialect=sqlite.dialect(), compile_kwargs={"literal_binds": True}))
        return con.execute(qry).fetchall()
    except:
        raise
    finally:
        con.close()

