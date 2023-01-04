# opencsv_insert_sql_db
Open a csv and insert into a sql database (example)

import pandas as pd
import pyodbc
import uuid

#import our customers.csv (obs: no dashes necessary, root folder)
customers = pd.read_csv("customers.csv")

df = pd.DataFrame(customers, columns=['name', 'phone', 'email', 'address'])


#verify if dataFrame has no rows
if len(df) == 0:
    sys.exit("DataFrame is empty")

# Script to create table Customer

#CREATE TABLE Customer (
#  _id int NOT NULL    IDENTITY    PRIMARY KEY,
#[Name] [nvarchar](300) NULL,
#[Phone] [nvarchar](300) NULL,
#[Email] [nvarchar](300) NULL,
#[Address] [nvarchar](300) NULL)

#function generic to connect a database
def connect_to_database(driver, server, db, uid, pwd):
        cnxn_str = (f"Driver={driver};" 
        f"Server={server};" 
        f"Database={db};" 
        f"UID={uid};" 
        f"PWD={pwd};")
        return cnxn_str

#invoke connect function from pyodbc
cnxn = pyodbc.connect(connect_to_database("ODBC Driver 17 for SQL Server", "localhost", "master", "sa", "reallyStrongPwd123"))
cursor = cnxn.cursor()

#improve insertion speed
cursor.fast_executemany = True

cursor.executemany(f"INSERT INTO Customer (Name, Phone, Email, Address) values(?,?,?,?)", df.values.tolist())

cnxn.commit()
cursor.close()  
