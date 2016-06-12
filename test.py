import sqlite3

def setup():
    con.execute("DROP TABLE IF EXISTS Customers")
    con.execute("""
        CREATE TABLE IF NOT EXISTS Customers (
            CustomerID INTEGER IDENTITY(1, 1)
            , FirstName VARCHAR(60)
            , LastName VARCHAR(60)
            , OrderDate DATE
        )
    """)
    con.commit()
    con.execute("""
    INSERT INTO Customers(FirstName, LastName, OrderDate)
    VALUES ('Mark', 'Stefanovic', '2016-01-31')
      , ('Tim', 'Jones', '2014-02-28')
      , ('Billy', 'Thorton', '2012-12-31')
      , ('Jill', 'Potter', Null)
    """)
    con.commit()

def teardown():
    # drop_table()
    con.close()


if __name__ == '__main__':
    con = sqlite3.connect('test.db')
    try:
        setup()
    finally:
        teardown()
