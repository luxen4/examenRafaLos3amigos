import csv
import os
from pymongo import MongoClient

class MongoDBOperations:
    def __init__(self, database_name, collection_name, port,username=None, password=None):
        if username and password:
            self.client = MongoClient(f'mongodb://{username}:{password}@localhost:{{port}}/')
        else:
            self.client = MongoClient(f'mongodb://localhost:{port}/')
            
        self.db = self.client[database_name]
        self.collection = self.db[collection_name]

    
    def read_person(self, filter_criteria):
        result = self.collection.find(filter_criteria)
       # persons = [Person(**person) for person in result]
       # return persons

    def update_person(self, filter_criteria, update_data):
        result = self.collection.update_many(filter_criteria, {'$set': update_data})
        return result.modified_count

    def delete_person(self, filter_criteria):
        result = self.collection.delete_many(filter_criteria)
        return result.deleted_count
    
    def run_aggregation(self, pipeline):
        result = self.collection.aggregate(pipeline)
        return list(result)
     
    '''     
    def create_project(self, project: NewProject):
        result = self.collection.insert_one(project.__dict__)
        return result
              
    def create_team(self, team: NewTeam):
        result = self.collection.insert_one(team.__dict__)
        return result  '''
                
                
        
        
        
                
                
    '''    
    #read_csv_file("./resources/MongoDB/projects.csv")

    def read_csv_file(filename):
        with open(filename, 'r') as file:
            reader = csv.reader(file)
            for row in reader:
                print(row[2])      
    #read_csv_file("./resources/MongoDB/teams.csv")

    def read_csv_file(filename):
        with open(filename, 'r') as file:
            reader = csv.reader(file)
            for row in reader:
                print(row[2])  
    read_csv_file("./resources/MongoDB/works_in_team.csv")'''


    # Function to read and display the content of a CSV file
    def read_csv_file(filename):
        with open(filename, 'r') as file:
            reader = csv.reader(file)
            for row in reader:
                print(row[2])  
                
                
                
    '''
    def create_person(self, person: NewPerson):
        result = self.collection.insert_one(person.__dict__)
        return result
    '''