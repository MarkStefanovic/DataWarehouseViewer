import sqlite3

def setup():
    con.execute("DROP TABLE IF EXISTS Customers")
    con.execute("""
        CREATE TABLE IF NOT EXISTS Customers (
            CustomerID INTEGER IDENTITY(1, 1)
            , FirstName VARCHAR(60)
            , LastName VARCHAR(60)
            , OrderDate DATE
            , Amount NUMERIC(9, 2)
        )
    """)
    con.commit()
    for i in range(10000):
        con.execute(
            """
            INSERT INTO Customers(FirstName, LastName, OrderDate, Amount)
            VALUES ('Mark', 'Stefanovic', '2016-01-31', 1.46)
              , ('Tim', 'Jones', '2014-02-28', 2.73)
              , ('Billy', 'Thorton', '2012-12-31', 92.49)
              , ('Jill', 'Potter', Null, 0.13)
            """
        )
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
