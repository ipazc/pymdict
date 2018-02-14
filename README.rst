PyMDict
#######

.. image:: https://badge.fury.io/py/pymdict.svg
    :target: https://badge.fury.io/py/pymdict
.. image:: https://travis-ci.org/ipazc/pymdict.svg?branch=master
    :target: https://travis-ci.org/ipazc/pymdict
.. image:: https://coveralls.io/repos/github/ipazc/pymdict/badge.svg?branch=master
    :target: https://coveralls.io/github/ipazc/pymdict?branch=master


Advanced Python Mongo Dict.
A Python dictionary based on a MongoDB. It allows to treat a collection as a dictionary in Python, with extensive capabilities, like allowing basic queries, bulk operations and versioning (forks).

INSTALLATION
############

Currently it is only supported Python3.4 onwards. It can be installed through pip:

.. code:: bash

    $ pip3 install pymdict


USAGE
#####

In order to run, a MongoDB server is required.

A Mongo-based dictionary can be instantiated as follows:

.. code:: python

    >>> from pymdict.mongo_dict import MongoDict
    >>>
    >>> m = MongoDict("custom_id", mongo_host="localhost", mongo_port=27017)


Once `m` is instantiated, it can be used as a normal dictionary.

.. code:: python

    >>> m["key"] = "value"
    >>> m[44] = "value2"
    >>> m["key2"] = "value3"
    >>> m["number"] = 44

    >>> print(list(m.keys()))
    ["key", 44, "key2, "number"]

    >>> for key, value in m.items():
    ...     print("{}: {}".format(key, value))
    key: value
    44: value2
    key2: value3
    number: 44


In addition, there are advanced functionalities like queries:

.. code:: python

    >>> for key, value, _ in m('key % ey'):
    ...     print("{}: {}".format(key, value))
    key: value
    key2: value3

    >>> for key, value, _ in m('key % ey or value = 44'):
    ...     print("{}: {}".format(key, value))
    key: value
    key2: value3
    number: 44

Queries also support to query with sub-dict elements:

.. code:: python

    >>> m["first"] = {"example": 44}
    >>> m["second"] = {"example": 45}
    >>> m["third"] = {"example": 46}

    >>> for key, value, _ in m('value.example > 44 and value.example < 46'):
    ...     print("{}: {}".format(key, value))
    second: 45

(TODO: Check the wiki page for more information about the query syntax)

Note that all the stores and removals are stored within a MongoDB. This means for each addition,edit and removal there is at least one connection to the MongoDB backend. In order to optimize it, a bulk operation can be used to wrap such amount of operations in a single connection:

.. code:: python

    >>> with m.bulk(buffer_size=100) as m:
    ...     for x in range(2000):
    ...         m["key{}".format(x)] = {"example": x}

Also, a mongo dict can be forked without the need to copy its content. This is specially useful if the target dict is extremely big and a copy is wanted. Note that a fork is an immediate process, and it allows to override or remove elements without modifying an original dictionary. It is achieved by applying a versioning technique with the dictionaries and it is still in an experimental state.

(TODO: More information about forking and versioning in the wiki page)

.. code:: python

    >>> m['foo'] = "bar"
    >>> fork = m.fork()
    >>> print(fork['foo'])
    bar
    >>> fork['foo'] = "foo"
    >>> print(fork['foo'], m['foo'])
    foo bar
