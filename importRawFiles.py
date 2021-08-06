from sqlalchemy import create_engine
import pandas as pd
import pymysql
import os

# Create SQLAlchemy engine
engine = create_engine("mysql+pymysql://root:My3qlP%40ssword@localhost:3306/unionspending", echo=False)

# Create function to turn a .txt file into an SQL database using SQLAlchemy
def txtToSQL(filename):

      # This takes the filename string and strips the ends off it to make it more readable
      databaseName = filename.rsplit('_', 2)[0].split('_', 1)[1]

      # This converts the specified file into a Pandas dataframe
      # Some of the DOL .txt files have improperly separated columns, so I set to program to keep running in case of error
      df = pd.read_csv('RawLM2Data/' + filename, sep='|', engine='python', header=0, on_bad_lines='warn')

      # This converts the dataframe to an SQL table using the databaseName variable from above
      df.to_sql(databaseName, con=engine, index=False, chunksize=1000, if_exists='replace')

      # This confirms that the table uploaded correctly by printing the first record from the new table
      print(engine.execute("""
      SELECT * FROM %s
      LIMIT 1;
      """ % (databaseName)).fetchall()
      )

# Create loop to iterate through every file in the relativeDirectory folder and run the txtToSQL function for some
def fileIteration(relativeDirectory):
      for filename in os.listdir(relativeDirectory):

            # Need to exclude the metadata files and readme from the raw DOL data
            if "meta" in filename or "Readme" in filename:
                  continue
            else:
                  txtToSQL(filename)

# fileIteration('RawLM2Data/')
txtToSQL('lm_data_data_2019.txt')