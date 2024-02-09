import os
import pandas as pd
from sqlalchemy import create_engine, MetaData,insert, Table,Column, Integer, String, Float
import time
import pymysql


from dotenv import load_dotenv


load_dotenv()

from sqlalchemy import create_engine
from urllib.parse import quote
import pandas as pd
import os


class DatabaseHelper:
    def __init__(self):
        self.db_host = os.getenv('DB_HOST')
        self.db_user = os.getenv('DB_USERNAME')
        self.db_password = os.getenv('DB_PASS')
        self.db_name = os.getenv('DB_NAME')
        self.engine = None

    def __connect__(self):
        try:
            db_url = f'mysql+pymysql://{self.db_user}:{quote(self.db_password)}@{self.db_host}/{self.db_name}'
            print(db_url)
            self.engine = create_engine(db_url)
            print("Connected to the database!")
        except Exception as e:
            print(f"Failed to connect to the database: {str(e)}")

    def __disconnect__(self):
        if self.engine is not None:
            self.engine.dispose()
            print("Disconnected from the database!")
        else:
            print("No active database connection.")

    def fetch(self, sql):
        """Executes the SQL query and returns the result as a Pandas DataFrame."""
        try:
            self.__connect__()
            result = pd.read_sql_query(sql, self.engine)
            self.__disconnect__()
            return result
        except Exception as e:
            print(f"Failed to fetch data from the database: {str(e)}")
            return None
        
    
    def update(self, sql, parameters=None):
        """Executes the SQL update query and returns the number of rows affected."""
        try:
            self.__connect__()
            with self.engine.connect() as connection:
                result = connection.execute(sql, parameters)
                rows_affected = result.rowcount
                print(f"Rows affected: {rows_affected}")
            self.__disconnect__()
            return rows_affected
        except Exception as e:
            print(f"Failed to execute update query: {str(e)}")
            return None
        
    def insertData(self, insert_query, parameters=None):
        """Executes the SQL insert query and returns the last inserted row ID."""
        try:
            metadata = MetaData()

            outlet_ratings = Table(
                'outlet_ratings', metadata,
                Column('store_id', Integer),
                Column('aggregator', String),
                Column('rating', Float),
                Column('date', String),
                Column('created_at', String),
                Column('updated_at', String)
            )
            self.__connect__()
            with self.engine.connect() as connection:
                data_insert = insert(outlet_ratings)
                insert_query = data_insert.values(parameters)
                # print(insert_query)
                result = connection.execute(insert_query)
                connection.commit()
                last_inserted_id = result.lastrowid
                # print(f"Last inserted row ID: {last_inserted_id}")
            self.__disconnect__()
            return last_inserted_id
        except Exception as e:
            # print(f"Failed to execute insert query: {str(e)}")
            return None


        
