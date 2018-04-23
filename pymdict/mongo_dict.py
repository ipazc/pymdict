#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# MIT License
#
# Copyright (c) 2018 Iván de Paz Centeno
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

from contextlib import contextmanager
from functools import partial
from threading import Thread, Lock
from time import sleep

import pymongo
from bson import ObjectId
from pymongo import MongoClient, UpdateOne, DeleteOne, InsertOne
from pymongo.errors import ConnectionFailure, BulkWriteError
from pymongo.periodic_executor import PeriodicExecutor

from pymdict.mongo_query_parser import MongoQueryParser


# Special collection for tracking information of mongo dictionaries, like forks families and versioning.
___MONGO_DICT_META___ = "___MONGO_DICT_META___"


class BasicMongoDict:
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

    def __init__(self, original_dict_id:str=None, mongo_host:str="192.168.2.115", mongo_port:int=27017,
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

        if original_dict_id is None:
            original_dict_id = str(ObjectId())

        mongo_uri = "mongodb://"

        if credentials is not None and len(credentials) == 2:
            mongo_uri += "{}:{}@".format(credentials[0], credentials[1])

        mongo_uri += "{}:{}/{}".format(mongo_host, mongo_port, mongo_database)

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
            result = self._instance.find_one({'key': item})

            if result is None:
                raise KeyError(item)
        else:

            result = self._instance.find_one({'key': item}, {'_id': 0, 'value': 1})
            if result is None:
                raise KeyError(item)

            result = result['value']

        return result

    def __setitem__(self, key, value):
        """
        Sets the value for a given key.
        :param key: item key to set.
        :param value: value to set for the item.
        """
        self._on_modified_callback()
        self._instance.replace_one({'key': key},  {'key': key, 'value': value}, upsert=True)

    @contextmanager
    def bulk(self, buffer_size: int = 500, do_upserts: bool = True):
        """
        Performs a bulk operation over the dictionary. It can be write and/or delete of elements.
        It is managed by a contextmanager. An example of use is:
            >>> with dictionary.bulk() as d:
            ...     d['k1'] = "v1"
            ...     d['k2'] = "v2"
            ...     d['k3'] = "v3"

        Note that by default the bulk is wrapping upsert operations. Upsert operations are not suitable
        if all the elements operated into the dict are going to be new elements. If this is the case, a boost of
        performance can be achieved by setting do_upserts flag to false.

        Benchmark
        =========

        for this piece of code:

        >>> benchmark = MongoDict("benchmark")

        >>> with benchmark.bulk(buffer_size=1000) as benchmark:
        ...    for x in range(5000):
        ...        benchmark['foo{}'.format(x)] = {'foo': 'bar', 'id': x}

        the time execution is between 18 and 27 seconds.

        If do_upserts flag is set to False, the time execution is reduced to 0.53 seconds.

        :param buffer_size: size of the buffer to set for the bulk operation. This size is the threshold at which
                    the changes are commited to the backend. The reason is that bulk operations are limited in size, so
                    they must be buffered.

        :param do_upserts: specifies if upserts should be the default operation. It will behave like a
                    normal dictionary, but appending lot of new elements is notably slower.
                    If during the bulk, it is ensured that all the elements are new, set this parameter
                    to false to speed up the operation.

        :return: context manager for the write and delete bulk operations.
        """
        m = BulkMongoDict(self._original_dict_id, mongo_host=self._mongo_host, mongo_port=self._mongo_port,
                          buffer_size=buffer_size, do_upserts=do_upserts)
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
        return self._instance.find_one({'key': item}) is not None

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
        self._on_modified_callback()
        result = self._instance.remove({'key': key})

    def items(self):
        for x in self._instance.find({}, {'_id':0, 'key': 1, 'value': 1}):
            yield x['key'], x['value']

    def __call__(self, query: str, count_only: bool=False):
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
            yield mongo_cursor.count()
        else:
            for result in mongo_cursor:
                yield result['key'], result['value'], result['_id']

    def __len__(self):
        return self._instance.find().count()

    def __del__(self):
        try:
            self._client.fsync()
        except pymongo.errors.OperationFailure:
            pass
        self._client.close()

    def last_element_id(self):
        last_elements = list(self._instance.find().sort("_id", pymongo.DESCENDING).limit(1))

        result = last_elements[0]['_id'] if len(last_elements) > 0 else None
        return result

    def _on_modified_callback(self):
        pass

    def _drop(self):
        self._instance.drop()

    def get_my_id(self):
        return self._original_dict_id

    def update(self, ext_dict):
        with self.bulk() as b:
            for key, value in ext_dict.items():
                b[key] = value


class MongoDict(BasicMongoDict):
    """
    This dictionary is special: keeps track of itself in a special collection ___MONGO_DICT_META___
    """
    def __init__(self, original_dict_id:str=None, mongo_host:str="localhost", mongo_port:int=27017,
                 mongo_database="mongo_dicts", credentials:tuple=None, version=None, allow_morph:bool=True,
                 immutable_version=False):
        BasicMongoDict.__init__(self, original_dict_id=original_dict_id, mongo_host=mongo_host, mongo_port=mongo_port,
                 mongo_database=mongo_database, credentials=credentials)

        self._allow_morph = allow_morph
        self._version = version
        self._immutable_version = immutable_version
        self._load_version(version)
        self._thread_lock = Lock()
        self._update_required = False

        if not immutable_version:
            self._executor = PeriodicExecutor(1, 1, target=self._update_thread_checker)
            self._executor.open()

    def _on_modified_callback(self):

        dict_meta = BasicMongoDict(original_dict_id=___MONGO_DICT_META___, mongo_host=self._mongo_host,
                                   mongo_port=self._mongo_port,
                                   mongo_database=self._mongo_database, credentials=self._credentials)

        metadata = dict_meta[self._original_dict_id]
        metadata.update({'modified': True})
        dict_meta[self._original_dict_id] = metadata

    def _load_version(self, version=None):
        """
        Loads a specific version of this dict.
        :param version: version to load (int number from 0 to N). If None specified, it will load the latest version.
        """

        dict_meta = BasicMongoDict(original_dict_id=___MONGO_DICT_META___, mongo_host=self._mongo_host,
                                   mongo_port=self._mongo_port,
                                   mongo_database=self._mongo_database, credentials=self._credentials)

        try:
            metadata = dict_meta[self._original_dict_id]
        except KeyError:
            metadata = {'version': [], 'modified': True}
            dict_meta[self._original_dict_id] = metadata

        if version is None:
            self._update_required = False

            # we pick last version
            if len(metadata['version']) == 0:
                self._version = 0
            else:
                self._version = metadata['version'][-1]

        else:
            self._version = version

        if self._version > 0:
            self._morph_into_fork(MongoDict(original_dict_id=self._original_dict_id, mongo_host=self._mongo_host,
                                            mongo_port=self._mongo_port, mongo_database=self._mongo_database,
                                            credentials=self._credentials, version=self._version-1))
        elif 'ancestor_fork' in metadata:
            self._morph_into_fork(MongoDict(original_dict_id=metadata['ancestor_fork'], mongo_host=self._mongo_host,
                                            mongo_port=self._mongo_port, mongo_database=self._mongo_database,
                                            credentials=self._credentials, version=metadata['ancestor_version']))

    def _update_thread_checker(self):
        dict_meta = BasicMongoDict(original_dict_id=___MONGO_DICT_META___, mongo_host=self._mongo_host,
                                   mongo_port=self._mongo_port,
                                   mongo_database=self._mongo_database, credentials=self._credentials)

        if self._immutable_version or not self._allow_morph:
            return False

        try:
            versions = dict_meta[self._original_dict_id]['version']
            if len(versions) > 0 and versions[-1] != self._version:
                with self._thread_lock:
                    self._update_required = True

        except KeyError:
            pass

        return True

    def _morph_into_fork(self, father):
        """
        Morphs this dictionary into a fork dictionary class, if it is not already one.
        :param father: father of this fork.
        :return:
        """
        if self._allow_morph:

            if type(self) is not ForkedMongoDict:
                self.__class__ = ForkedMongoDict

                # Now we map relevant functions
                self.__contains__ = partial(ForkedMongoDict.__contains__, self)
                self.__str__ = partial(ForkedMongoDict.__str__, self)
                self.__repr__ = partial(ForkedMongoDict.__repr__, self)
                self.__len__ = partial(ForkedMongoDict.__len__, self)
                self.__getitem__ = partial(ForkedMongoDict.__getitem__, self)
                self.__delitem__ = partial(ForkedMongoDict.__delitem__, self)
                self.__call__ = partial(ForkedMongoDict.__call__, self)
                self.__iter__ = partial(ForkedMongoDict.__iter__, self)

                self.items = partial(ForkedMongoDict.items, self)
                self.bulk = partial(ForkedMongoDict.bulk, self)
                self._fork_father = father

            else:
                self._fork_father = father

            self._fork_father._immutable_version = True

        self._instance = self._storage[self._original_dict_id+("v{}".format(self._version) if self._version > 0 else "")]
        self._instance.create_index([('key', pymongo.TEXT)], name='key_index')

    def fork(self, new_id=None):
        self._update_from_latest()

        if new_id is None:
            new_id = str(ObjectId())

        if new_id == self._original_dict_id:
            raise Exception("Fork cannot override father's ID")

        dict_meta = BasicMongoDict(original_dict_id=___MONGO_DICT_META___, mongo_host=self._mongo_host,
                                   mongo_port=self._mongo_port,
                                   mongo_database=self._mongo_database, credentials=self._credentials)

        metadata = dict_meta[self._original_dict_id]

        if metadata['modified']:
            metadata['version'].append(self._version + 1)
            metadata['modified'] = False
            dict_meta[self._original_dict_id] = metadata

        result = ForkedMongoDict(self, new_id, mongo_host=self._mongo_host, mongo_port=self._mongo_port,
                                 mongo_database=self._mongo_database, credentials=self._credentials)

        # Reload the latest version
        self._load_version()

        return result

    @contextmanager
    def bulk(self, buffer_size: int=500, do_upserts: bool=True):
        """
        Performs a bulk operation over the dictionary. It can be write and/or delete of elements.
        It is managed by a contextmanager. An example of use is:
            >>> with dictionary.bulk() as d:
            ...     d['k1'] = "v1"
            ...     d['k2'] = "v2"
            ...     d['k3'] = "v3"

        Note that by default the bulk is wrapping upsert operations. Upsert operations are not suitable
        if all the elements operated into the dict are going to be new elements. If this is the case, a boost of
        performance can be achieved by setting do_upserts flag to false.

        Benchmark
        =========

        for this piece of code:

        >>> benchmark = MongoDict("benchmark")

        >>> with benchmark.bulk(buffer_size=1000) as benchmark:
        ...    for x in range(5000):
        ...        benchmark['foo{}'.format(x)] = {'foo': 'bar', 'id': x}

        the time execution is between 18 and 27 seconds.

        If do_upserts flag is set to False, the time execution is reduced to 0.53 seconds.

        :param buffer_size: size of the buffer to set for the bulk operation. This size is the threshold at which
                    the changes are commited to the backend. The reason is that bulk operations are limited in size, so
                    they must be buffered.

        :param do_upserts: specifies if upserts should be the default operation. It will behave like a
                    normal dictionary, but appending lot of new elements is notably slower.
                    If during the bulk, it is ensured that all the elements are new, set this parameter
                    to false to speed up the operation.

        :return: context manager for the write and delete bulk operations.
        """
        self._update_from_latest()
        m = BulkMongoDict(self._original_dict_id, mongo_host=self._mongo_host, mongo_port=self._mongo_port,
                          buffer_size=buffer_size, version=self._version, do_upserts=do_upserts)
        yield m
        m.commit()

    def _update_from_latest(self, force_update=False):
        """
        Updates self if required (because of a modification or whatever)
        :return:
        """
        if not self._immutable_version and (self._update_required or force_update):
            self._load_version()

    def __eq__(self, other):
        self._update_from_latest(force_update=True)
        other._update_from_latest(force_update=True)

        return hasattr(other, '_original_dict_id') and hasattr(other, '_version') and \
               self._original_dict_id == other._original_dict_id and self._version == other._version

    def __setitem__(self, key, value):
        self._update_from_latest()
        return BasicMongoDict.__setitem__(self, key, value)

    def __getitem__(self, item):
        self._update_from_latest()
        return BasicMongoDict.__getitem__(self, item)

    def __delitem__(self, key):
        self._update_from_latest()
        return BasicMongoDict.__delitem__(self, key)

    def __contains__(self, item):
        self._update_from_latest()
        return BasicMongoDict.__contains__(self, item)

    def __len__(self):
        self._update_from_latest()
        return BasicMongoDict.__len__(self)

    def keys(self):
        self._update_from_latest()
        return BasicMongoDict.keys(self)

    def values(self):
        self._update_from_latest()
        return BasicMongoDict.values(self)

    def items(self):
        self._update_from_latest()
        return BasicMongoDict.items(self)

    def __iter__(self):
        self._update_from_latest()
        return BasicMongoDict.__iter__(self)

    def __call__(self, *args, **kwargs):
        self._update_from_latest()
        return BasicMongoDict.__call__(self, *args, **kwargs)


class ForkedMongoDict(MongoDict):
    """
    Dictionary that allows to work with an existing dict without overriding it.
    """

    def __init__(self, father:MongoDict, original_dict_id:str=None, mongo_host:str="localhost", mongo_port:int=27017,
                 mongo_database="mongo_dicts", credentials:tuple=None, version=None):
        MongoDict.__init__(self, original_dict_id=original_dict_id, mongo_host=mongo_host, mongo_port=mongo_port,
                           mongo_database=mongo_database, credentials=credentials, version=version)
        self._fork_father = MongoDict(father._original_dict_id, version=father._version, mongo_host=father._mongo_host,
                                      mongo_port=father._mongo_port, mongo_database=father._mongo_database,
                                      credentials=father._credentials, immutable_version=True)
        dict_meta = BasicMongoDict(original_dict_id=___MONGO_DICT_META___, mongo_host=self._mongo_host,
                                   mongo_port=self._mongo_port,
                                   mongo_database=self._mongo_database, credentials=self._credentials)

        metadata = dict_meta[self._original_dict_id]
        metadata['ancestor_fork'] = father._original_dict_id
        metadata['ancestor_version'] = father._version
        dict_meta[self._original_dict_id] = metadata

    def __contains__(self, item):
        """
        Checks whether an item is contained in the instance.
        :param item: key value to check if it is contained or not.
        :return: boolean flag indicating if the key is contained or not.
        """
        self._update_from_latest()

        i = self._instance.find_one({'key': item})

        if i is None:
            return item in self._fork_father
        else:
            return "___removed" not in i

    def keys(self):
        self._update_from_latest()
        current_keys = list(self._instance.find())
        removed_keys = set([k['key'] for k in current_keys if '___removed' in k])

        father_keys = self._fork_father.keys()
        total_keys = set(father_keys + [k['key'] for k in current_keys])
        return [r for r in total_keys if r not in removed_keys]

    def __str__(self):
        return "Mongo_Dict ({}) -- father: {} (v{})".format(self._original_dict_id, self._fork_father, self._fork_father._version)

    def __repr__(self):
        return "Mongo_Dict ({}) -- father: {} (v{})".format(self._original_dict_id, self._fork_father, self._fork_father._version)

    def __len__(self):
        self._update_from_latest()
        new_elements = self._instance.find({'___removed': None}).count()
        removed_elements = self._instance.find({'___removed': 1}).count()

        return max(0, len(self._fork_father) - removed_elements) + new_elements

    def __getitem__(self, item):
        self._update_from_latest()
        try:
            result = MongoDict.__getitem__(self, (item, ) if type(item) is not tuple else item)
        except KeyError:
            try:
                result = self._fork_father[(item, ) if type(item) is not tuple else item]
            except KeyError as e:
                raise e from None

        if result is None or (result is not None and '___removed' in result):
            raise KeyError(item)

        if type(item) is not tuple:
            result = result['value']

        return result

    def __delitem__(self, key):
        self._update_from_latest()
        self._on_modified_callback()
        self._instance.replace_one({'key': key}, {'key': key, 'value': None, '___removed': 1}, upsert=True)

    @contextmanager
    def bulk(self, buffer_size=500, do_upserts: bool=True):
        """
        Performs a bulk operation over the dictionary. It can be write and/or delete of elements.
        It is managed by a contextmanager. An example of use is:
            >>> with dictionary.bulk() as d:
            ...     d['k1'] = "v1"
            ...     d['k2'] = "v2"
            ...     d['k3'] = "v3"

        Note that by default the bulk is wrapping upsert operations. Upsert operations are not suitable
        if all the elements operated into the dict are new elements. If this is the case, a boost of
        performance can be achieved by setting do_upserts parameter to false.

        Benchmark
        =========

        for this piece of code:

        >>> benchmark = MongoDict("benchmark")

        >>> with benchmark.bulk(buffer_size=1000) as benchmark:
        ...    for x in range(5000):
        ...        benchmark['value7{}'.format(x)] = {'a': 'val', 'id': x}

        the time execution is between 18 and 27 seconds.

        If do_upserts flag is set to False, the time execution is reduced to 0.53 seconds.


        :param buffer_size: size of the buffer to set for the bulk operation. This size is the threshold at which
                    the changes are commited to the backend. The reason is that bulk operations are limited in size, so
                    they must be buffered.
        :param do_upserts: specifies if upserts should be the default operation. It will behave like a
                    normal dictionary, but appending lot of new elements is notably slower.
                    If during the bulk, it is ensured that all the elements are new, set this parameter
                    to false to speed up the operation.

        :return: context manager for the write and delete bulk operations.
        """
        m = BulkMongoDictForked(original_dict_id=self._original_dict_id, mongo_host=self._mongo_host,
                                mongo_port=self._mongo_port, mongo_database=self._mongo_database,
                                credentials=self._credentials, buffer_size=buffer_size, version=self._version,
                                do_upserts=do_upserts)
        yield m
        self._on_modified_callback()
        m.commit()

    def items(self):
        self._update_from_latest()

        for key, value, _id in self(""):
            yield key, value

    def __iter__(self):
        self._update_from_latest()

        for key, _, _ in self(""):
            yield key

    def __call__(self, query:str, count_only:bool=False):
        """
        Performs a query over the dictionary. It uses a simple query.
        :param query: string query to perform
        :param count_only: if set to true, it will return the length of the query result.
        :return: iterator for the elements that satisfies the query, or the size of the elements set if count_only
        param is true.
        """
        self._update_from_latest()

        RETRIEVE_BUFFER = 500
        # We perform a call this way we run through each element:

        m = MongoQueryParser()
        mongo_query = m.transform_request(query)
        mongo_query_for_removed = {'___removed': 1}
        mongo_query_for_edited_added = {'$and': [mongo_query, {'___removed': None}]}

        mongo_cursor_removed = self._instance.find(mongo_query_for_removed, {'key': 1, '_id': 0})
        mongo_cursor_edited = self._instance.find(mongo_query_for_edited_added)

        if count_only:
            yield max(0, self._fork_father(query, count_only=True) - mongo_cursor_removed.count()) + mongo_cursor_edited.count()
        else:

            mongo_used = set()

            # First we yield the edited and added
            for result in mongo_cursor_edited:
                mongo_used.add(result['key'])
                yield result['key'], result['value'], result['_id']

                # Meanwhile we get RETRIEVE_BUFFER of removed elements
                for index, removed in zip(range(RETRIEVE_BUFFER), mongo_cursor_removed):
                    mongo_used.add(removed['key'])

            # Let's add the rest of the removed elements (if any yet) to the set
            for removed in mongo_cursor_removed:
                mongo_used.add(removed['key'])

            # Then we yield from the father's if it is not in the used list,
            mongo_cursor = self._fork_father(query)

            for key, value, _id in mongo_cursor:
                if key not in mongo_used:
                    yield key, value, _id


class BulkMongoDict(MongoDict):
    """
    Dictionary that allows bulk operations on a mongo dictionary.
    """
    def __init__(self, original_dict_id: str=None, mongo_host: str="localhost", mongo_port: int=27017,
                 mongo_database: str="mongo_dicts", credentials: tuple=None, buffer_size: int=100,
                 version: int=None, do_upserts: bool=True):
        MongoDict.__init__(self, original_dict_id=original_dict_id, mongo_host=mongo_host,
                           mongo_port=mongo_port, mongo_database=mongo_database, credentials=credentials,
                           version=version, allow_morph=False, immutable_version=True)
        self._buffer_size = buffer_size
        self._operations = []
        self.do_upserts = do_upserts

    def __setitem__(self, key, value):

        if self.do_upserts:
            operation = UpdateOne({"key": key}, {"$set": {"value": value}}, upsert=True)
        else:
            operation = InsertOne({"key": key, "value": value})

        self._operations.append(operation)

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
                self._on_modified_callback()
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


class DictDropper:
    """
    Warning: this is a class capable of removing dictionaries! it might leave the dict-system in an inconsistent state!
    use it carefully!

    Why can it leave it in an inconsistent state?
    =============================================
    Dictionaries that fork from other dictionaries do not clone the content. Instead, they refer to the content of the
    original dictionary. If an original dictionary is removed and several forks are pointing to it, the forks will
    probably lose part of its content.
    """
    def __init__(self, mongo_host:str="localhost", mongo_port:int=27017,
                 mongo_database="mongo_dicts", credentials:tuple=None):
        self._mongo_host = mongo_host
        self._mongo_port = mongo_port
        self._mongo_database = mongo_database
        self._credentials = credentials

    def drop_dict(self, dict_id, remove_all_versions=True):
        dict_meta = BasicMongoDict(original_dict_id=___MONGO_DICT_META___, mongo_host=self._mongo_host,
                                   mongo_port=self._mongo_port,
                                   mongo_database=self._mongo_database, credentials=self._credentials)

        metadata = dict_meta[dict_id]

        versions = metadata['version']

        if remove_all_versions:
            for version in versions:
                BasicMongoDict(original_dict_id="{}v{}".format(dict_id, version), mongo_host=self._mongo_host,
                               mongo_port=self._mongo_port, mongo_database=self._mongo_database,
                               credentials=self._credentials)._drop()

            BasicMongoDict(original_dict_id=dict_id, mongo_host=self._mongo_host,
                           mongo_port=self._mongo_port,
                           mongo_database=self._mongo_database, credentials=self._credentials)._drop()
            del dict_meta[dict_id]
        else:
            MongoDict(original_dict_id=dict_id, mongo_host=self._mongo_host, mongo_port=self._mongo_port,
                      mongo_database=self._mongo_database, credentials=self._credentials)._drop()

            if len(metadata['version']) > 0:
                metadata['version'] = metadata['version'][:-1]
                dict_meta[dict_id] = metadata
            else:
                del dict_meta[dict_id]

        return len(versions) + 1
