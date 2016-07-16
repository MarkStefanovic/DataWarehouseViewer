import os
import random


from faker import Faker

from utilities import rootdir, SQLiteConnection

fake = Faker()


def create_table():
    with SQLiteConnection(db_path) as con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS SalesHistory (
                OrderID INTEGER PRIMARY KEY
                , CustomerID INTEGER
                , ProductID INTEGER
                , CustomerName VARCHAR
                , OrderDate DATETIME
                , ShippingDate DATETIME
                , ShippingAddress VARCHAR
                , SalesAmount NUMERIC(19, 2)
            )
        """)


def delete_db():
    try:
        os.remove(os.path.join(rootdir(), 'test.db'))
        print('test.db deleted')
    except Exception as e:
        print(str(e))


def drop_table():
    with SQLiteConnection(db_path) as con:
        con.execute("DROP TABLE IF EXISTS Customers;")


def insert_test_rows():
    with SQLiteConnection(db_path) as con:
        for _ in range(10000):
            con.execute(("""
                INSERT INTO SalesHistory(
                  CustomerID
                  , ProductID
                  , CustomerName
                  , OrderDate
                  , ShippingDate
                  , ShippingAddress
                  , SalesAmount
                )
                VALUES (
                  '{customer_id}'
                  , '{product_id}'
                  , '{customer_name}'
                  , '{order_date}'
                  , '{shipping_date}'
                  , '{shipping_address}'
                  , '{sales_amount}'
                )
            """).format(
                customer_id=random.randint(1, 999999)
                , product_id=random.randint(1, 999999)
                , customer_name=fake.name()
                , order_date=fake.date_time()
                , shipping_date=fake.date_time()
                , shipping_address=fake.address()
                , sales_amount=round(random.random() * 1000, 2)
            ))


def setup():
    reset_db()


def teardown():
    delete_db()


def reset_db():
    delete_db()
    create_table()
    insert_test_rows()


if __name__ == '__main__':
    db_path = os.path.join(rootdir(), 'test.db')
    reset_db()
