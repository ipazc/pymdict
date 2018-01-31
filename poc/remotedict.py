import pymongo
from bson import ObjectId
from pymongo import MongoClient


class MongoDict():

    def __init__(self, original_dict:ObjectId=None, mongo_host:str="localhost", mongo_port:int=27017):

        if original_dict is None:
            original_dict = ObjectId()

        self._client = MongoClient(host=mongo_host, port=mongo_port)
        self._storage = self._client['mongo_dicts']
        self._instance = self._storage[str(original_dict)]
        self._instance.create_index([('key', pymongo.TEXT)], name='key_index')

        self._original_dict = original_dict

    def __getitem__(self, item):
        if type(item) is dict:
            result = self._instance.find_one(item)
        else:
            result = self._instance.find_one({'key': item}, {'_id': 0, 'value':1})['value']

        return result

    def __setitem__(self, key, value):
        self._instance.replace_one({'key': key},  {'key': key, 'value': value}, upsert=True)

    def keys(self):
        return list(self.__iter__())

    def __str__(self):
        return "Mongo_Dict ({})".format(self._original_dict)

    def __repr__(self):
        return "Mongo_Dict ({})".format(self._original_dict)

    def values(self):
        return self._instance.find({}, {'_id':0, 'value': 1})

    def __iter__(self):
        for x in self._instance.find({}, {'_id':0, 'key': 1}):
            yield x['key']

    def __delitem__(self, key):
        self._instance.remove({'key': key})

    def items(self):
        for x in self._instance.find({}, {'_id':0, 'key': 1, 'value': 1}):
            yield x['key'], x['value']


m = MongoDict(original_dict=ObjectId('5a720122b9a7c0403a5b376c'), mongo_host="172.17.0.1", mongo_port=27015)

#m['example'] = "hi"
print(m['example'])

print(m.keys())

del m['example']
print(m.keys())
