
from typing import Generator, List

from sqlalchemy import create_engine
from sqlalchemy.sql import Delete, Insert, Update

from config import cfg
from logger import log_error

engine = create_engine(cfg.app.db_path, echo=False)


class Transaction:
    def __init__(self):
        self.connection = engine.connect()
        self.transaction = self.connection.begin()
        self.rows_added = 0
        self.rows_deleted = 0
        self.rows_updated = 0

    def execute(self, cmd):
        try:
            result = self.connection.execute(cmd)
            if type(cmd) == Delete:
                self.rows_deleted += 1
                return 0
            elif type(cmd) == Insert:
                self.rows_added += 1
                return result.inserted_primary_key
            elif type(cmd) == Update:
                self.rows_updated += 1
                return 0
            return 0
        except:
            self.transaction.rollback()
            self.connection.close()
            raise

    def commit(self):
        self.transaction.commit()
        self.connection.close()
        return {
            'rows_added': self.rows_added,
            'rows_deleted': self.rows_deleted,
            'rows_updated': self.rows_updated
        }


@log_error
def execute(cmd) -> int:
    con = engine.connect()
    try:
        result = con.execute(cmd)
        if type(cmd) == Delete:
            return 0
        elif type(cmd) == Insert:
            return result.inserted_primary_key
        elif type(cmd) == Update:
            return 0
        return 0
    except:
        raise
    finally:
        con.close()


@log_error
def fetch(qry: str) -> List[str]:
    con = engine.connect()
    try:
        return con.execute(qry).fetchall()
    except:
        raise
    finally:
        con.close()


@log_error
def iterrows(cmd) -> Generator:
    con = engine.connect()
    for row in con.execute(cmd):
        yield row
    con.close()

# if __name__ == '__main__':
#     with fetch('SELECT * FROM Products') as f:
#         print(f)
