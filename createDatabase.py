# using tutorial from https://www.freecodecamp.org/news/connect-python-with-sql/

import sqlalchemy import create_engine
import pandas as pd
import os

def create_db_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name
        )
        print("MySQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")

    return connection

# This will call the function above and attempt to create a connection
connection = create_server_connection("localhost", "root", "My3qlP@ssword")

# def create_database(connection, query):
#     cursor = connection.cursor()
#     try:
#         cursor.execute(query)
#         print("Database created successfully")
#     except Error as err:
#         print(f"Error: '{err}'")
#
# # This is the SQL query that will create a database called school
# create_database_query = "CREATE DATABASE school"

# This will call the function that creates the database
# create_database(connection, create_database_query)

# This function takes strings and turns them into SQL queries
def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query successful")
    except Error as err:
        print(f"Error: '{err}'")

for dirpath, dirsInDirpath, filesInDirPath in os.walk("RawLM2Data"):
    for myfile in filesInDirPath:
        sqlQuery = "LOAD DATA INFILE %s INTO TABLE xxxx (col1,col2,...);" % os.path.join(dirpath, myfile)
        # execute the query here using your mysql connector.
        # I used string formatting to build the query, but you should use the safe placeholders provided by the mysql api instead of %s, to protect against SQL injections