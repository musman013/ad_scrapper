from bson.objectid import ObjectId
from pymongo import MongoClient

class MongoDBConnectionClass():

    def connectToMongo(self):
        try:
            client = MongoClient('localhost:27017')
            # client = MongoClient(Constants.mongoDbIp)
            # print("***************Server info")
            # print(client.server_info(), type(client))
            db = client['adspy']
            return db
        except Exception as e:
            print(e)
            return ("collectionOfProjectNotFound")