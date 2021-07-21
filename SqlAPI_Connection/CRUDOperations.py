'''
@Author: Santanu Mohapatra
@Date: 19/07/2021
@Last Modified by: Santanu Mohapatra
@Last Modified Time: 20:30 PM
@Title: Python program for SQL_API Connections
'''

from azure.cosmos import CosmosClient, PartitionKey, exceptions
import logging
import json
import os

class CRUDOperations:
    '''
    Class: CRUDOperations
    Description: Class to perform Basic CRUD Operations on Cosmos DB SQL API
    Functions:
        dbConnection()
        create_database()
        create_container()
        insert_record()
        delete_record()
        read_record()
        drop_database()
    Variable: None
    '''
    def __init__(self):
        self.url = os.getenv('URI')
        self.key = os.getenv('KEY')
        self.dbConnection()
        self.create_database()
        self.create_container()

    def dbConnection(self):
        '''
        Description: Connection to Azure Cosmos DB
        Parameter: None
        Return: None
        '''
        try:
            self.client = CosmosClient(self.url, credential = self.key)
            logging.info("Connection Successful")
        
        except Exception:
            logging.error("Connection Unsuccessful")

    def create_database(self):
        '''
        Description: Create a Database
        Parameter: None
        Return: None
        '''
        try: 
            database_name = 'mydatabase'
            self.mydb = self.client.create_database(database_name)
            logging.info("Database Creation Successful")
        except exceptions.CosmosResourceExistsError:
            self.mydb = self.client.get_database_client(database_name)
            logging.info("Database Exists")
        except Exception:
            logging.error("Database Creation Unsuccessfull")

    def create_container(self):
        '''
        Description: Create a Container
        Parameter: None
        Return: None
        '''
        try: 
            container_name = 'to-do_list'
            self.myContainer = self.mydb.create_container(id = container_name, partition_key = PartitionKey(path="/category"))
            logging.info("Container Creation Successful")
        except exceptions.CosmosResourceExistsError:
            self.myContainer = self.mydb.get_container_client(container_name)
            logging.warning("Container Exists")
        except Exception:
            logging.error("Container Creation Unsuccessful")

    def insert_record(self):
        '''
        Description: Insert Records to the container
        Parameter: None
        Return: None
        '''
        try:
            for index in range(1, 10):
                self.myContainer.upsert_item(
                    {
                    'id': 'item{0}'.format(index),
                    'category': 'Personal',
                    'categorical_desc': 'Description {0}'.format(index)
                    }
                )
            logging.info("Insert Record Successful")
        except Exception:
            logging.exception("Insert Record Unsuccessful")

    def delete_record(self):
        '''
        Description: Delete a record in the container
        Parameter: None
        Return: None
        '''
        try:
            myQuery = "SELECT * FROM list l WHERE l.id = 'item1'"
            for item in self.myContainer.query_items(query = myQuery, enable_cross_partition_query = True):
                self.myContainer.delete_item(item, partition_key = 'Widget')
            logging.info("Delete Record Successful")
        except Exception:
            logging.exception("Delete Record Unsuccessful")

    def read_record(self):
        '''
        Description: Read a record from the container
        Parameter: None
        Return: None
        '''
        try:
            myQuery = "SELECT * FROM list l WHERE l.categorical_desc = 'Description 2'"
            for item in self.myContainer.query_items(query = myQuery, enable_cross_partition_query = True):
                print(json.dumps(item, indent = True))
            logging.info("Read Record Successful")
        except Exception:
            logging.error("Read Record Unsuccessful")
    
    def drop_database(self):
        '''
        Description: Delete the created Database
        Parameter: None
        Return: None
        '''
        try:
            self.client.delete_database("mydatabase")
            logging.info("Database Drop Successful")
        except Exception:
            logging.error("Drop Database Unsuccessful")

if __name__ == "__main__":
    obj = CRUDOperations()
    obj.insert_record()
    obj.delete_record()
    obj.read_record()
    obj.drop_database()