import unittest
from time import sleep, time

from bson import ObjectId

from pymdict.mongo_dict import MongoDict, DictDropper, ForkedMongoDict

MONGO_HOST = "localhost"
MONGO_PORT = 27017


class CheckDict:
    
    def __init__(self, testcase_instance, dict_to_test):
        self._dict_to_test = dict_to_test
        self._testcase_instance = testcase_instance
        
    def test_len(self):
        d = self._dict_to_test

        self._testcase_instance.assertEqual(len(d), 0)
        d["1"] = 1
        self._testcase_instance.assertEqual(len(d), 1)

    def test_keys(self):
        d = self._dict_to_test

        self._testcase_instance.assertEqual(len(d), 0)
        d["1"] = 1
        self._testcase_instance.assertEqual(d.keys(), ["1"])

    def test_write_read(self):
        d = self._dict_to_test

        self._testcase_instance.assertEqual(len(d), 0)
        d["1"] = "my value"
        self._testcase_instance.assertEqual(d["1"], "my value")

        with self._testcase_instance.assertRaises(KeyError):
            d["5"]

    def test_write_read_complex_data(self):
        d = self._dict_to_test

        self._testcase_instance.assertEqual(len(d), 0)
        d["1"] = {"this": {"is": ["a very complex"], "piece": 33.4}, "of": b"data"}
        self._testcase_instance.assertEqual(d["1"], {"this": {"is": ["a very complex"], "piece": 33.4}, "of": b"data"})

    def test_contains(self):
        d = self._dict_to_test

        self._testcase_instance.assertEqual(len(d), 0)
        d["1"] = 22
        d["2"] = "foo"
        d["3"] = ["bar"]
        d[4] = ["bar"]

        self._testcase_instance.assertTrue("1" in d)
        self._testcase_instance.assertTrue("2" in d)
        self._testcase_instance.assertTrue("3" in d)
        self._testcase_instance.assertFalse("4" in d)
        self._testcase_instance.assertTrue(4 in d)

    def test_delete(self):
        d = self._dict_to_test

        self._testcase_instance.assertEqual(len(d), 0)
        d["1"] = 22
        d["2"] = "foo"
        d["3"] = ["bar"]

        self._testcase_instance.assertEqual(len(d), 3)

        self._testcase_instance.assertTrue("1" in d)
        self._testcase_instance.assertTrue("2" in d)

        del d["1"]

        self._testcase_instance.assertEqual(len(d), 2)

        self._testcase_instance.assertFalse("1" in d)
        self._testcase_instance.assertTrue("2" in d)

    def test_iter(self):
        d = self._dict_to_test

        d["1"] = 22
        d["2"] = "foo"
        d["3"] = ["bar"]

        keys = {"1", "2", "3"}

        for x in d:
            self._testcase_instance.assertIn(x, keys)

    def test_items(self):
        d = self._dict_to_test

        d["1"] = 22
        d["2"] = "foo"
        d["3"] = ["bar"]

        normal_dict = {
            "1": 22,
            "2": "foo",
            "3": ["bar"]
        }

        for key, value in d.items():
            self._testcase_instance.assertEqual(value, normal_dict[key])

    def test_update(self):
        d = self._dict_to_test

        self._testcase_instance.assertEqual(len(d), 0)

        normal_dict = {
            "1": 22,
            "2": "foo",
            "3": ["bar"],
            "4": 44,
            "5": 55
        }

        d.update(normal_dict)

        self._testcase_instance.assertEqual(len(d), 5)

        for key, value in normal_dict.items():
            self._testcase_instance.assertEqual(d[key], value)

    def test_query(self):
        d = self._dict_to_test

        normal_dict = {
            "1": 22,
            "2": "foo",
            "3": ["bar"]
        }

        for key, value, _id in d(""):
            self._testcase_instance.assertEqual(value, normal_dict[key])
            self._testcase_instance.assertEqual(type(_id), ObjectId)

        for key, value, _id in d("value = 22"):
            self._testcase_instance.assertEqual(key, "1")
            self._testcase_instance.assertEqual(value, normal_dict[key])

        for key, value, _id in d("value = 22"):
            self._testcase_instance.assertEqual(key, "1")
            self._testcase_instance.assertEqual(value, normal_dict[key])


class MongoDictTests(unittest.TestCase):

    def setUp(self):
        self.m = MongoDict(mongo_host=MONGO_HOST, mongo_port=MONGO_PORT)

    def test_mongo_dict_len(self):
        test = CheckDict(self, self.m)
        test.test_len()

    def test_mongo_dict_keys(self):
        test = CheckDict(self, self.m)
        test.test_keys()

    def test_mongo_dict_write_read(self):
        test = CheckDict(self, self.m)
        test.test_write_read()

    def test_mongo_dict_write_read_complex_data(self):
        test = CheckDict(self, self.m)
        test.test_write_read_complex_data()

    def test_mongo_dict_contains(self):
        test = CheckDict(self, self.m)
        test.test_contains()

    def test_mongo_dict_delete(self):
        test = CheckDict(self, self.m)
        test.test_delete()

    def test_iter(self):
        test = CheckDict(self, self.m)
        test.test_iter()

    def test_items(self):
        test = CheckDict(self, self.m)
        test.test_items()

    def test_update(self):
        test = CheckDict(self, self.m)
        test.test_update()

    def test_query(self):
        test = CheckDict(self, self.m)
        test.test_query()

    def tearDown(self):
        self.dropper = DictDropper(mongo_host=MONGO_HOST, mongo_port=MONGO_PORT)
        self.dropper.drop_dict(self.m.get_my_id())


class ForkedMongoDictTests(unittest.TestCase):

    def setUp(self):
        self.original = MongoDict(mongo_host=MONGO_HOST, mongo_port=MONGO_PORT)
        self.fork = self.original.fork()

    def test_mongo_dict_len(self):
        test = CheckDict(self, self.fork)
        test.test_len()

    def test_mongo_dict_keys(self):
        test = CheckDict(self, self.fork)
        test.test_keys()

    def test_mongo_dict_write_read(self):
        test = CheckDict(self, self.fork)
        test.test_write_read()

    def test_mongo_dict_write_read_complex_data(self):
        test = CheckDict(self, self.fork)
        test.test_write_read_complex_data()

    def test_mongo_dict_contains(self):
        test = CheckDict(self, self.fork)
        test.test_contains()

    def test_mongo_dict_delete(self):
        test = CheckDict(self, self.fork)
        test.test_delete()

    def test_iter(self):
        test = CheckDict(self, self.fork)
        test.test_iter()

    def test_items(self):
        test = CheckDict(self, self.fork)
        test.test_items()

    def test_update(self):
        test = CheckDict(self, self.fork)
        test.test_update()

    def test_query(self):
        test = CheckDict(self, self.fork)
        test.test_query()

    def tearDown(self):
        self.dropper = DictDropper(mongo_host=MONGO_HOST, mongo_port=MONGO_PORT)
        self.dropper.drop_dict(self.original.get_my_id())
        self.dropper.drop_dict(self.fork.get_my_id())


class ForkedMongoDictTests2(unittest.TestCase):

    def setUp(self):
        self._drop()
        self.original = MongoDict("original", mongo_host=MONGO_HOST, mongo_port=MONGO_PORT)
        self.original["val1"] = 55
        self.original["val2"] = "hello"
        self.fork = self.original.fork("fork")
        del self.fork["val1"]
        del self.fork["val2"]

    def _assert_original_kept(self):
        self.assertEqual(len(self.original), 2)
        self.assertEqual(self.original['val1'], 55)
        self.assertEqual(self.original['val2'], "hello")

    def test_mongo_dict_len(self):
        test = CheckDict(self, self.fork)
        test.test_len()
        self._assert_original_kept()

    def test_mongo_dict_keys(self):
        test = CheckDict(self, self.fork)
        test.test_keys()
        self._assert_original_kept()

    def test_mongo_dict_write_read(self):
        test = CheckDict(self, self.fork)
        test.test_write_read()
        self._assert_original_kept()

    def test_mongo_dict_write_read_complex_data(self):
        test = CheckDict(self, self.fork)
        test.test_write_read_complex_data()
        self._assert_original_kept()

    def test_mongo_dict_contains(self):
        test = CheckDict(self, self.fork)
        test.test_contains()
        self._assert_original_kept()

    def test_mongo_dict_delete(self):
        test = CheckDict(self, self.fork)
        test.test_delete()
        self._assert_original_kept()

    def test_iter(self):
        test = CheckDict(self, self.fork)
        test.test_iter()
        self._assert_original_kept()

    def test_items(self):
        test = CheckDict(self, self.fork)
        test.test_items()
        self._assert_original_kept()

    def test_update(self):
        test = CheckDict(self, self.fork)
        test.test_update()
        self._assert_original_kept()

    def test_query(self):
        test = CheckDict(self, self.fork)
        test.test_query()
        self._assert_original_kept()

    def _drop(self):
        self.dropper = DictDropper(mongo_host=MONGO_HOST, mongo_port=MONGO_PORT)
        try:
            self.dropper.drop_dict("original")
        except KeyError:
            pass

        try:
            self.dropper.drop_dict("fork")
        except KeyError:
            pass

    def tearDown(self):
        self._drop()


class ForkedMongoDictTests3(unittest.TestCase):

    def setUp(self):
        self._drop_db()
        self.original = MongoDict("original", mongo_host=MONGO_HOST, mongo_port=MONGO_PORT)
        self.original["val1"] = 55
        self.original["val2"] = "hello"
        self.fork1 = self.original.fork("fork1")
        self.fork1["val3"] = 65
        self.fork2 = self.fork1.fork("fork2")
        del self.fork2["val1"]
        del self.fork2["val2"]
        del self.fork2["val3"]

    def _assert_original_kept(self):
        self.assertEqual(len(self.original), 2)
        self.assertEqual(self.original['val1'], 55)
        self.assertEqual(self.original['val2'], "hello")

        self.assertEqual(len(self.fork1), 3)
        self.assertEqual(self.fork1['val1'], 55)
        self.assertEqual(self.fork1['val2'], "hello")
        self.assertEqual(self.fork1['val3'], 65)

    def test_mongo_dict_len(self):
        test = CheckDict(self, self.fork2)
        test.test_len()
        self._assert_original_kept()

    def test_mongo_dict_keys(self):
        test = CheckDict(self, self.fork2)
        test.test_keys()
        self._assert_original_kept()

    def test_mongo_dict_write_read(self):
        test = CheckDict(self, self.fork2)
        test.test_write_read()
        self._assert_original_kept()

    def test_mongo_dict_write_read_complex_data(self):
        test = CheckDict(self, self.fork2)
        test.test_write_read_complex_data()
        self._assert_original_kept()

    def test_mongo_dict_contains(self):
        test = CheckDict(self, self.fork2)
        test.test_contains()
        self._assert_original_kept()

    def test_mongo_dict_delete(self):
        test = CheckDict(self, self.fork2)
        test.test_delete()
        self._assert_original_kept()

    def test_iter(self):
        test = CheckDict(self, self.fork2)
        test.test_iter()
        self._assert_original_kept()

    def test_items(self):
        test = CheckDict(self, self.fork2)
        test.test_items()
        self._assert_original_kept()

    def test_update(self):
        test = CheckDict(self, self.fork2)
        test.test_update()
        self._assert_original_kept()

    def test_query(self):
        test = CheckDict(self, self.fork2)
        test.test_query()
        self._assert_original_kept()

    def _drop_db(self):
        try:
            self.dropper = DictDropper(mongo_host=MONGO_HOST, mongo_port=MONGO_PORT)
            self.dropper.drop_dict("original")
            self.dropper.drop_dict("fork1")
            self.dropper.drop_dict("fork2")

        except:
            pass

    def tearDown(self):
        self._drop_db()


class ForkSituationsTest(unittest.TestCase):

    def setUp(self):
        self._drop_db()
        self.original = MongoDict("original", mongo_host=MONGO_HOST, mongo_port=MONGO_PORT)
        self.original2 = MongoDict("original", mongo_host=MONGO_HOST, mongo_port=MONGO_PORT)
        self.fork1 = self.original.fork("fork1")

    def test_similar_dicts(self):
        self.assertEqual(self.original, self.original2)

    def test_fork_reflected_on_originals(self):
        sleep(1.5)

        self.original2['foo'] = 'bar'
        self.assertEqual(self.original['foo'], 'bar')
        self.assertEqual(type(self.original), ForkedMongoDict)
        self.assertEqual(type(self.original2), ForkedMongoDict)

    def tearDown(self):
        self._drop_db()

    def _drop_db(self):
        try:
            self.dropper = DictDropper(mongo_host=MONGO_HOST, mongo_port=MONGO_PORT)
            self.dropper.drop_dict("original")
            self.dropper.drop_dict("fork1")

        except:
            pass


class BenchmarkTest(unittest.TestCase):

    def setUp(self):
        self._drop_db()
        self.benchmark = MongoDict("benchmark", mongo_host=MONGO_HOST, mongo_port=MONGO_PORT)

    def test_bulk_upsert_vs_non_upsert(self):

        start_upsert = time()

        with self.benchmark.bulk(do_upserts=True) as benchmark:
            for x in range(2000):
                benchmark["value_{}".format(x)] = {'upsert': True, 'id': x}

        stop_upsert = time()
        upsert_total_time = stop_upsert - start_upsert

        with self.benchmark.bulk(do_upserts=False) as benchmark:
            for x in range(2000):
                benchmark["value2_{}".format(x)] = {'upsert': False, 'id': x}

        insert_total_time = time() - stop_upsert
        self.assertLess(insert_total_time, upsert_total_time)

    def tearDown(self):
        self._drop_db()

    def _drop_db(self):
        try:
            self.dropper = DictDropper(mongo_host=MONGO_HOST, mongo_port=MONGO_PORT)
            self.dropper.drop_dict("benchmark")

        except:
            pass


if __name__ == '__main__':
    unittest.main()
