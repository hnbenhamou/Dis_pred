import pymongo as mongo
import os
from dotenv import load_dotenv


class DbConnect:
    def __init__(self):
        load_dotenv()
        mongo_uri = os.getenv("MONGO_URI")
        print(mongo_uri)
        if not mongo_uri:
            raise ValueError("MONGO_URI not found in .env file.")
        self.client = mongo.MongoClient(mongo_uri)
        print(self.client)
        self.db = self.client['errors']
        self.collection = self.db['errors']

    def insert_one(self, data):
        self.collection.insert_one(data)

    def insert_many(self, data):
        self.collection.insert_many(data)

    def find_one(self, query):
        return self.collection.find_one(query)

    def find_many(self, query):
        return self.collection.find(query)

    def delete_one(self, query):
        self.collection.delete_one(query)

    def delete_many(self, query):
        self.collection.delete_many(query)

    def update_one(self, query, data):
        self.collection.update_one(query, data)

    def update_many(self, query, data):
        self.collection.update_many(query, data)

    def drop(self):
        self.collection.drop()

    def count(self):
        return self.collection.count_documents()

    def close(self):
        self.client.close()

    def __del__(self):
        self.client.close()


if __name__ == "__main__":
    db = DbConnect()
    db.insert_one({'test': 'test'})
    print(db.find_one({'test': 'test'}))
    db.delete_one({'test': 'test'})
    print(db.find_one({'test': 'test'}))
    db.close()
