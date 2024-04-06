import json
import sqlalchemy as db
import pymysql
import pandas as pd
import psycopg2
import sys
import os
import boto3
import base64
from dotenv import load_dotenv

class Base:
    def __init__(self):
        load_dotenv()
        self.DRIVER =os.getenv('driver')
        self.SERVER =os.getenv('server')
        self.DATABASE = os.getenv('database')
        self.USERNAME = os.getenv('user_name')
        self.PASSWORD = os.getenv('password')
        self.url = os.getenv('url')
        self.DATABASE_CONNECTION = f'{self.DRIVER}://{self.USERNAME}:{self.PASSWORD}@{self.SERVER}/{self.DATABASE}'
        
        ## variables AWS
        self.awsId=os.getenv('awsId')
        self.awsSecret=os.getenv('awsSecret')
        self.bucket=os.getenv('bucket')
        self.regionName = os.getenv('regionName')        
        self.connect()
    
    def connect(self):
        try:
            self.engine = db.create_engine(self.DATABASE_CONNECTION)
            self.connection = self.engine.connect()
            print("Conexión exitosa db CRMWARE")
        except Exception as ex:
            print(ex)

    def reconnect(self):
        self.close()
        self.connect()
            
    def close(self):
        self.connection.close()

    def query(self, query):

        return pd.read_sql_query(query, self.connection)

    def connectAWS(self):
        try:
            S3Client = boto3.client(
                's3',
                region_name=self.regionName,
                aws_access_key_id=self.awsId,
                aws_secret_access_key=self.awsSecret
            )
            print("Conexión exitosa aws")
            return S3Client   
        except Exception as ex:
            print(ex)
        