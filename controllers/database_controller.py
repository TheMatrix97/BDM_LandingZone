import pymongo


class Database:
    _uri = "mongodb://localhost:27017"
    _database = None

    def __init__(self, database_name):
        self._initialize(database_name)

    def _initialize(self, database_name):
        client = pymongo.MongoClient(self._uri)
        self._database = client[database_name]

    def insert(self, collection, data):
        self._database[collection].insert(data)

    def find(self, collection, query):
        return self._database[collection].find(query)

    def find_one(self, collection, query):
        return self._database[collection].find_one(query)
