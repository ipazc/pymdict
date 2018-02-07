from contextlib import contextmanager
from time import sleep

import pymongo
from bson import ObjectId
from pymongo import MongoClient, UpdateOne, DeleteOne
from pymongo.errors import ConnectionFailure, BulkWriteError
from pymdict.mongo_query_parser import MongoQueryParser


# Special collection for tracking information of mongo dictionaries, like forks families.
___MONGO_DICT_META___ = "___MONGO_DICT_META___"


class BasicMongoDict():
    """
    Mongo dictionary class.

    Allows to use a dictionary back-ended in a MongoDB.
    It allows to read, write, modify, create bulk operations and perform queries in a simplified way.

    [] Examples of classic-dict behaviour:

        * Create a new dictionary in MongoDB:
            >>> dictionary = MongoDict()

        * Insert elements in the dictionary:
            >>> dictionary['element'] = "value"

        * Read elements from the dictionary:
            >>> element = dictionary['element']

        * Iterate over keys:
            >>> for key in dictionary:
            ...     print(key)

        * Iterate over keys and values:
            >>> for key, value in dictionary.items():
            ...     print("{}: {}".format(key, value))

        * Remove element:
            >>> del dictionary[key]

      [] Examples of advanced features of MongoDict:

        * bulk write (write multiple elements in a single mongo call):
            >>> with dictionary.bulk(buffer_size=100) as d:
            ...     for x in range(10000):
            ...         d['key{}'.format(x)] = x

        * bulk remove:
            >>> with dictionary.bulk(buffer_size=100) as d:
            ...     for x in range(10000):
            ...         del d['key{}'.format(x)]

        * Perform a query:

            >>> for key, value, id in dictionary('key = key5'):
            ...     print("{}: {}".format(key, value))

        MongoDict allows to perform queries on the dictionary with a custom simple-to-use syntax instead of a mongo
        query.
        The basic syntax follows the next structure:

            Operand1 operator Operand2

        operator is defined in two ways:
            first level operators: "and", "or"
            second level operators: "=", "!=", ">", "<", ">=", "<=", "eq", "!eq", "%", "in"

        ----

        For example:

            >>> dictionary['a'] = 5
            >>> dictionary['b'] = 9
            >>> dictionary['d'] = 100
            >>>
            >>> query = "value > 5 and value < 100"
            >>> for k, v,_ in dictionary(query):
            ...     print("{}: {}".format(k, v))
            b: 9

            operators can be nested in a complex way by using brackets:
            >>> query = "value > 5 and (value < 100 or value = 100)"
            >>> for k, v,_ in dictionary(query):
            ...     print("{}: {}".format(k, v))
            b: 9
            d: 100

        For more information about queries, visit this wiki page: [TODO]
    """

    def __init__(self, original_dict_id:str=str(ObjectId()), mongo_host:str="localhost", mongo_port:int=27017,
                 mongo_database="mongo_dicts", credentials:tuple=None):
        """
        Instantiates the dictionary back-ended in mongo
        :param original_dict_id: ID of the dictionary. This is going to be the name of the collection in the db.
                                 Reusing of an ID under a same mongo host, port and database leads to share the
                                 dictionary.
        :param mongo_host:  host of the mongodb
        :param mongo_port:  port of the mongodb
        :param mongo_database:  database name of the mongo db. By default it will use "mongo_dicts"
        :param credentials: tuple containing the user and password. Leave it as None if no credentials are required.
        """

        mongo_uri = "mongodb://"

        if credentials is not None and len(credentials) == 2:
            mongo_uri += "{}:{}@".format(credentials[0], credentials[1])

        mongo_uri += "{}:{}".format(mongo_host, mongo_port)

        try:
            self._client = MongoClient(mongo_uri)
        except ConnectionFailure:
            raise ConnectionError("No connection to the remote dict backend. Ensure that a MongoDB backend is listening"
                                  " on {}:{}".format(mongo_host, mongo_port)) from None

        self._mongo_host = mongo_host
        self._mongo_port = mongo_port
        self._mongo_database = mongo_database
        self._credentials = credentials
        self._storage = self._client[mongo_database]
        self._instance = self._storage[original_dict_id]
        self._instance.create_index([('key', pymongo.TEXT)], name='key_index')

        self._original_dict_id = original_dict_id

    def __getitem__(self, item):
        """
        Retrieves the value for a given item.
        :param item: item key to search for.
        :return: value for the specified item key.
        """

        if type(item) is tuple:
            item = item[0]
            extract_id = 1
        else:
            extract_id = 0

        result = self._instance.find_one({'key': item}, {'_id': extract_id, 'value':1})

        if result is None:
            raise KeyError(item)

        return result['value']

    def __setitem__(self, key, value):
        """
        Sets the value for a given key.
        :param key: item key to set.
        :param value: value to set for the item.
        """
        self._instance.replace_one({'key': key},  {'key': key, 'value': value}, upsert=True)

    @contextmanager
    def bulk(self, buffer_size=100):
        """
        Performs a bulk operation over the dictionary. It can be write and/or delete of elements.
        It is managed by a contextmanager. An example of use is:
            >>> with dictionary.bulk() as d:
            ...     d['k1'] = "v1"
            ...     d['k2'] = "v2"
            ...     d['k3'] = "v3"

        :param buffer_size: size of the buffer to set for the bulk operation. This size is the threshold at which
                    the changes are commited to the backend. The reason is that bulk operations are limited in size, so
                    they must be buffered.
        :return: context manager for the write and delete bulk operations.
        """
        m = BulkMongoDict(self._original_dict_id, mongo_host=self._mongo_host, mongo_port=self._mongo_port,
                          buffer_size=buffer_size)
        yield m
        m.commit()

    def keys(self):
        """
        Retrieves a list of keys. Should not use this method if the dictionary is extremely big. Instead,
        iterate over it.
        :return: list of keys.
        """
        return list(self.__iter__())

    def __contains__(self, item):
        """
        Checks whether an item is contained in the instance.
        :param item: key value to check if it is contained or not.
        :return: boolean flag indicating if the key is contained or not.
        """
        return self._instance.find_one({'key': item}) != None

    def __str__(self):
        return "Mongo_Dict ({})".format(self._original_dict_id)

    def __repr__(self):
        return "Mongo_Dict ({})".format(self._original_dict_id)

    def values(self):
        """
        Returns the values of the dictionary. It must be forcefully iterated.
        """
        for x in self._instance.find({}, {'_id':0, 'value': 1}):
            yield x['value']

    def __iter__(self):
        for x in self._instance.find({}, {'_id':0, 'key': 1}):
            yield x['key']

    def __delitem__(self, key):
        self._instance.remove({'key': key})

    def items(self):
        for x in self._instance.find({}, {'_id':0, 'key': 1, 'value': 1}):
            yield x['key'], x['value']

    def __call__(self, query:str, count_only:bool=False):
        """
        Performs a query over the dictionary. It uses a simple query.
        :param query: string query to perform
        :param count_only: if set to true, it will return the length of the query result.
        :return: iterator for the elements that satisfies the query, or the size of the elements set if count_only
        param is true.
        """

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

    def __del__(self):
        self._client.fsync()
        self._client.close()

    def last_element_id(self):
        return list(self._instance.find().sort("_id", pymongo.DESCENDING).limit(1))[0]['_id']

class MongoDict(BasicMongoDict):
    """
    This dictionary is special: keeps track of itself in a special collection ___MONGO_DICT_META___
    """
    def __init__(self, original_dict_id:str=str(ObjectId()), mongo_host:str="localhost", mongo_port:int=27017,
                 mongo_database="mongo_dicts", credentials:tuple=None, version=None):
        BasicMongoDict.__init__(self, original_dict_id=original_dict_id, mongo_host=mongo_host, mongo_port=mongo_port,
                 mongo_database=mongo_database, credentials=credentials)

        self._version = version
        self._load_version(version)

    def _load_version(self, version):
        dict_meta = BasicMongoDict(original_dict_id=___MONGO_DICT_META___, mongo_host=self._mongo_host,
                                   mongo_port=self._mongo_port,
                                   mongo_database=self._mongo_database, credentials=self._credentials)

        metadata = dict_meta[self._original_dict_id]

        if version is None:
            # we pick last version
            if len(metadata['version']) == 0:
                self._version = 0
            else:
                self._version = metadata['version'][-1]

        self._instance = self._storage[self._original_dict_id+"v{}".format(self._version)]
        self._instance.create_index([('key', pymongo.TEXT)], name='key_index')

    def _save_dict_meta(self):
        dict_meta = BasicMongoDict(original_dict_id=___MONGO_DICT_META___, mongo_host=self._mongo_host,
                                   mongo_port=self._mongo_port,
                                   mongo_database=self._mongo_database, credentials=self._credentials)

    def _thread_checker(self):

        while not self._exit:
            if
            sleep(1)


class ForkedMongoDict(MongoDict):
    """
    Dictionary that allows to work with an existing dict without overriding it.
    """

    def __init__(self, father:BasicMongoDict, original_dict_id:str=str(ObjectId()), mongo_host:str="localhost", mongo_port:int=27017,
                 mongo_database="mongo_dicts", credentials:tuple=None):
        MongoDict.__init__(self, original_dict_id=original_dict_id, mongo_host=mongo_host, mongo_port=mongo_port,
                           mongo_database=mongo_database, credentials=credentials)
        self.fork_father = father

    def __contains__(self, item):
        """
        Checks whether an item is contained in the instance.
        :param item: key value to check if it is contained or not.
        :return: boolean flag indicating if the key is contained or not.
        """

        i = self._instance.find_one({'key': item})

        if i is None:
            return item in self.fork_father
        else:
            return "___removed" in i

    def __str__(self):
        return "Mongo_Dict ({}) -- father: {}".format(self._original_dict_id, self.fork_father)

    def __repr__(self):
        return "Mongo_Dict ({}) -- father: {}".format(self._original_dict_id, self.fork_father)

    def __len__(self):
        size = self._instance.find({'$and':
                                        [{'_id': {'$gt': self.fork_father.last_element_id()}},
                                         {'___removed': {'$ne': 1}}]}).count()
        return len(self.fork_father) + size

    def __getitem__(self, item):
        try:
            result = MongoDict.__getitem__(self, item)
        except KeyError:
            try:
                result = self.fork_father[item]
            except KeyError as e:
                raise e from None

        if result is None or (result is not None and '___removed' in result):
            raise KeyError(item)

        return result

    def __delitem__(self, key):
        self._instance.replace_one({'key': key}, {'key': key, 'value': None, '___removed': 1}, upsert=True)

    @contextmanager
    def bulk(self, buffer_size=100):
        """
        Performs a bulk operation over the dictionary. It can be write and/or delete of elements.
        It is managed by a contextmanager. An example of use is:
            >>> with dictionary.bulk() as d:
            ...     d['k1'] = "v1"
            ...     d['k2'] = "v2"
            ...     d['k3'] = "v3"

        :param buffer_size: size of the buffer to set for the bulk operation. This size is the threshold at which
                    the changes are commited to the backend. The reason is that bulk operations are limited in size, so
                    they must be buffered.
        :return: context manager for the write and delete bulk operations.
        """
        m = BulkMongoDictForked(original_dict_id=self._original_dict_id, mongo_host=self._mongo_host,
                                mongo_port=self._mongo_port, mongo_database=self._mongo_database,
                                credentials=self._credentials, buffer_size=buffer_size)
        yield m
        m.commit()


class BulkMongoDict(MongoDict):
    """
    Dictionary that allows bulk operations on a mongo dictionary.
    """
    def __init__(self, original_dict_id:str=str(ObjectId()), mongo_host:str="localhost", mongo_port:int=27017,
                 mongo_database="mongo_dicts", credentials:tuple=None, buffer_size=100):
        self._buffer_size = buffer_size
        MongoDict.__init__(self, original_dict_id=original_dict_id, mongo_host=mongo_host,
                           mongo_port=mongo_port, mongo_database=mongo_database, credentials=credentials)
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

class BulkMongoDictForked(BulkMongoDict):

    def __delitem__(self, key):
        self._operations.append(
            UpdateOne({"key": key}, {"$set": {"value": None, "___removed": 1}}, upsert=True)
        )

        if len(self._operations) > self._buffer_size:
            self.commit()

