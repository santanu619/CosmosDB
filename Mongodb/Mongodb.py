'''
@Author: Santanu Mohapatra
@Date: 18/07/2021
@Last Modified by: Santanu Mohapatra
@Last Modified Time: 09:05 AM
@Title: Python program for cosmosdb.
'''

import os
import json
import pymongo
import logging

class cosmosDB:
    '''
    Class: cosmosDB
    Description: Class to perform Basic CRUD Operations on Cosmos DB MongoDB API
    Functions:
        dbConnection()
        insert_record()
        update_record()
        delete_record()
        read_collection()
        drop_collection()
    '''
    def __init__(self):
        self.conString = os.getenv('CONNECTION_STRING')
        self.dbConnection()

    def dbConnection(self):
        '''
        Description: Connection to Azure Cosmos DB
        Parameter: None
        Return: None
        '''
        try:
            self.client = pymongo.MongoClient(self.conString)
            self.db = self.client[os.getenv("mydb")]
            self.col = self.db[os.getenv("mycollection")]
            logging.info("Connection Successful")

        except Exception:
            logging.error("Connection Unsuccessful")
            
    def insert_record(self):
        '''
        Description: Insert Reccords from a json file
        Parameter: None
        Return: None
        '''
        try:
            if os.path.isfile("book.json"):
                with open("book.json", "r") as f:
                    entries = json.load(f)
                    listBooks = entries["book"]
                    self.col.insert_many(listBooks)
                    logging.info("Insertion Successful")
        except Exception:
            logging.error("Insertion Unsuccessful")

    def update_record(self):
        '''
        Description: Update a record in collections book
        Parameter: None
        Return: None
        '''
        try:
            search = {"bookid": "004"}
            setting = {"$set": {"type": "Fiction"}}
            self.col.update_one(search, setting)
            logging.info("Update Successful")
        except Exception:
            logging.error("Update Unsuccessful")

    def read_collection(self):
        '''
        Description: Read from collections and print the  records
        Parameter: None
        Return: None
        '''
        try:
            logging.info("Printing Records")
            for b in self.col.find():
                logging.info(b)
        except Exception:
            logging.error("Data Unread")

    def delete_record(self):
        '''
        Description: Delete a record in Collection book
        Parameter: None
        Return: None
        '''
        try:
            search = {"_id": 3}
            self.col.delete_one(search)
            logging.info("Delete Record Successful")
        except Exception:
            logging.error("Delete Record Unsuccessful")

    def drop_collection(self):
        '''
        Description: Drop the created collection in Azure Cosmos DB
        Parameter: None
        Return: None
        '''
        try:
            self.col.drop() 
            logging.info("Drop Collection Successful")
        except Exception:
            logging.error("Drop Collection Unsuccessful")

if __name__ == "__main__":
    obj = cosmosDB()
    obj.insert_record()
    obj.update_record()
    obj.delete_record()
    obj.read_collection()
    obj.drop_collection()