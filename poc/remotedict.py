from contextlib import contextmanager

import pymongo
from bson import ObjectId
from pymongo import MongoClient, UpdateOne, DeleteOne
from pymongo.errors import ConnectionFailure, BulkWriteError

from poc.mongo_query_parser import MongoQueryParser


class MongoDict():

    def __init__(self, original_dict_id:str=str(ObjectId()), mongo_host:str="localhost", mongo_port:int=27017):

        try:
            self._client = MongoClient(host=mongo_host, port=mongo_port)
        except ConnectionFailure:
            raise ConnectionError("No connection to the remote dict backend. Ensure that a MongoDB backend is listening"
                                  " on {}:{}".format(mongo_host, mongo_port)) from None

        self._mongo_host = mongo_host
        self._mongo_port = mongo_port
        self._storage = self._client['mongo_dicts']
        self._instance = self._storage[original_dict_id]
        self._instance.create_index([('key', pymongo.TEXT)], name='key_index')

        self._original_dict_id = original_dict_id

    def __getitem__(self, item):
        result = self._instance.find_one({'key': item}, {'_id': 0, 'value':1})

        if result is not None:
            result = result['value']

        return result

    def __setitem__(self, key, value):
        self._instance.replace_one({'key': key},  {'key': key, 'value': value}, upsert=True)

    @contextmanager
    def bulk(self, buffer_size=100):
        m = MultiInsertMongoDict(self._original_dict_id, mongo_host=self._mongo_host, mongo_port=self._mongo_port,
                                 buffer_size=buffer_size)
        yield m
        m.commit()

    def keys(self):
        return list(self.__iter__())

    def __contains__(self, item):
        return self._instance.find_one({'key': item}) != None

    def __str__(self):
        return "Mongo_Dict ({})".format(self._original_dict_id)

    def __repr__(self):
        return "Mongo_Dict ({})".format(self._original_dict_id)

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

    def __call__(self, query:str, count_only:bool=False):
        m = MongoQueryParser()
        mongo_query = m.transform_request(query)
        mongo_cursor = self._instance.find(mongo_query)

        if count_only:
            return mongo_cursor.count()
        else:
            for result in mongo_cursor:
                yield result['key'], result['value'], result['_id']

    def __len__(self):
        return self._instance.find().count()

class MultiInsertMongoDict(MongoDict):
    def __init__(self, original_dict_id:str=str(ObjectId()), mongo_host:str="localhost", mongo_port:int=27017,
                 buffer_size=100):
        self._buffer_size = buffer_size
        MongoDict.__init__(self, original_dict_id, mongo_host, mongo_port)
        self._operations = []

    def __setitem__(self, key, value):
        self._operations.append(
            UpdateOne({"key": key}, {"$set": {"value": value}}, upsert=True)
        )

        if len(self._operations) > self._buffer_size:
            self.commit()

    def __delitem__(self, key):
        self._operations.append(
            DeleteOne({"key": key})
        )

        if len(self._operations) > self._buffer_size:
            self.commit()

    def commit(self):
        if len(self._operations) > 0:
            try:
                self._instance.bulk_write(self._operations, ordered=False)
                self._operations = []
            except BulkWriteError as ex:
                print("Could not write the bulk operations: {}".format(ex.details))
                raise

m = MongoDict(original_dict_id="a", mongo_host="172.17.0.1", mongo_port=27015)

with m.bulk() as m:
    m['example'] = {"hi": 5}
    m['example2'] = {"hi": 61}
    m['example4'] = {"hi": 5}

    del m['example']

print(m['example'])
print(m['example2'])
print(m['example4'])

for key, value, _ in m('value.hi >= 2 and key !% mple1'):
    print(key, value)
#print(m.keys())

#del m['example']
#print(m.keys())

print(len(m))
print(m)
print(m.keys())

print("example" in m)
print("example2" in m)