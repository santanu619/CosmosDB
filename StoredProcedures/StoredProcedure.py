'''
@Author: Santanu Mohapatra
@Date: 20/07/2021
@Last Modified by: Santanu Mohapatra
@Last Modified Time: 19:40 PM
@Title: Python program for Stored Procedures in Cosmosdb
'''

import uuid,os
import azure.cosmos.cosmos_client as cosmos_client
import logging

class StoredProcedure:

    def __init__(self):
            url = os.getenv('URL')
            key = os.getenv('KEY')
            self.database_name = os.getenv('DATABASENAME')
            self.container_name = os.getenv('CONTAINERNAME')
            self.client = cosmos_client.CosmosClient(url, key)
            self.database = self.client.get_database_client(self.database_name)
            self.container = self.database.get_container_client(self.container_name) 

    def create_container(self):
        '''
        description: Function to register the stored procedure
        Parameters: None
        Returns: None
        '''
        try:        
            with open('AddItem.js') as file:
                file_contents = file.read()

            sprocedure = {
                'id': 'AddItem',
                'serverScript': file_contents,
            }     
            self.create_sproc = self.container.scripts.create_stored_procedure(body=sprocedure)
        except Exception as e:    
            print("Already exists")


    def execute_container(self):  
        '''
        description: Function to execute the container in cosmosdb
        Parameters: None
        Return: None
        '''
        try:      
            new_id= str(uuid.uuid4())
            #Creating a document for a container with "id" as a partition key.
            id = input("Enter the id:")
            category = input("Enter the category:")
            description = input("Enter the Description:")
            
            new_item =   {
            "id": new_id, 
            "_id":id,
            "name":category,
            "description":description,
            "isComplete":False
            }
            self.container.scripts.execute_stored_procedure(sproc='AddItem',params=[[new_item]], partition_key=description) 
        except Exception as e:
            logging.error(e)    