import os
import random
import sqlite3

from faker import Faker

from utilities import rootdir

fake = Faker()


class SQLiteConnection:
    """Context manager that auto-commits and closes connection on exit."""

    def __init__(self, db_name, read_only=False):
        """Constructor"""
        self.db_name = db_name
        self.read_only = read_only

    def __enter__(self):
        """
        Open the database connection
        """
        fp = os.path.abspath(self.db_name)
        if self.read_only:
            self.conn = sqlite3.connect('file:/{}?mode=ro'.format(fp), uri=True)
        else:
            self.conn = sqlite3.connect(fp)
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Close the connection
        """
        if not self.read_only:
            self.conn.commit()
        self.conn.close()

def create_customers_table():
    with SQLiteConnection(db_path) as con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS dimCustomer (
                ID INTEGER PRIMARY KEY
                , CustomerName VARCHAR
                , ShippingAddress VARCHAR
            )
        """)


def create_sales_history_table():
    with SQLiteConnection(db_path) as con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS factSales (
                OrderID INTEGER PRIMARY KEY
                , CustomerID INTEGER
                , ProductID INTEGER
                , OrderDate DATETIME
                , ShippingDate DATETIME
                , SalesAmount NUMERIC(19, 2)
                , Paid BOOLEAN
            )
        """)


def delete_db():
    try:
        os.remove(os.path.join(rootdir(), 'test.db'))
        print('test.db deleted')
    except Exception as e:
        print(str(e))


def add_dummy_sales():
    with SQLiteConnection(db_path) as con:
        for _ in range(10000):
            con.execute(("""
                INSERT INTO factSales(
                  CustomerID
                  , ProductID
                  , OrderDate
                  , ShippingDate
                  , SalesAmount
                  , Paid
                ) VALUES (
                  '{customer_id}'
                  , '{product_id}'
                  , '{order_date}'
                  , '{shipping_date}'
                  , '{sales_amount}'
                  , '{paid}'
                )
            """).format(
                customer_id=random.randint(1, 1000)
                , product_id=random.randint(1, 10)
                , order_date=fake.date_time()
                , shipping_date=fake.date_time()
                , sales_amount=round(random.random() * 1000, 2)
                , paid=random.choice([0, 1])
            ))

def add_dummy_customers():
    with SQLiteConnection(db_path) as con:
        for _ in range(1000):
            con.execute(("""
                INSERT INTO dimCustomer(
                  CustomerName
                  , ShippingAddress
                ) VALUES (
                  '{customer_name}'
                  , '{shipping_address}'
                )
            """).format(
                customer_name=fake.name()
                , shipping_address=fake.address()
            ))


def create_products_table():
    with SQLiteConnection(db_path) as con:
        con.execute("""
            DROP TABLE IF EXISTS dimProduct
        """)
        con.execute("""
            CREATE TABLE dimProduct (
                ID INTEGER PRIMARY KEY
                , ProductName VARCHAR(100)
                , ProductCategory VARCHAR(40)
            )
        """)
        con.execute("""
            INSERT INTO dimProduct (
              ProductName
              , ProductCategory
            )
            VALUES
                ('Rain coat', 'Clothing')
                , ('Banannas', 'Groceries')
                , ('Shoes', 'Clothing')
                , ('Sony Walkman', 'Electronics')
                , ('Hair clips', 'Household items')
                , ('Toothpaste', 'Toiletries')
                , ('Bandaids', 'Medicine')
                , ('Oranges', 'Groceries')
                , ('Paper', 'Office supplies')
                , ('Levi Jeans', 'Clothing')
        """)


def setup():
    reset_db()


def teardown():
    delete_db()


def reset_db():
    delete_db()
    create_customers_table()
    create_sales_history_table()
    create_products_table()
    add_dummy_customers()
    add_dummy_sales()


if __name__ == '__main__':
    db_path = os.path.join(rootdir(), 'test.db')
    reset_db()
