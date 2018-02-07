import re
import unittest

from pymdict.mongo_query_parser import MongoQueryParser


class MongoQueryParserTests(unittest.TestCase):

    def test_mongo_simple_query(self):
        m = MongoQueryParser()

        self.assertEqual(m.transform_request("val.hola = 33"), {'val.hola': 33.0})
        self.assertEqual(m.transform_request("val.hola eq 33"), {'val.hola': '33'})
        self.assertEqual(m.transform_request("val.hola > 33"), {'val.hola': {'$gt': 33.0}})
        self.assertEqual(m.transform_request("val.hola < 33"), {'val.hola': {'$lt': 33.0}})
        self.assertEqual(m.transform_request("val.hola >= 33"), {'val.hola': {'$gt': 32.0}})
        self.assertEqual(m.transform_request("val.hola <= 33"), {'val.hola': {'$lt': 34.0}})
        self.assertEqual(m.transform_request("val.hola != 33"), {'val.hola': {'$ne': 33.0}})
        self.assertEqual(m.transform_request("val.hola !eq 33"), {'val.hola': {'$ne': '33'}})
        self.assertEqual(m.transform_request("val.hola in ['33', '44']"), {'val.hola': {'$in': ['33', '44']}})
        self.assertEqual(m.transform_request("val.hola in [33, 44]"), {'val.hola': {'$in': [33, 44]}})
        self.assertEqual(m.transform_request("val.hola in ['hello jeje', '24 44']"), {'val.hola': {'$in': ['hello jeje', '24 44']}})
        self.assertEqual(m.transform_request("val.hola % thisregex"), {'val.hola': {'$regex': 'thisregex'}})
        self.assertEqual(m.transform_request("val.hola !% thisregex"), {'val.hola': {'$not': re.compile('thisregex')}})

    def test_mongo_complex_query(self):
        m = MongoQueryParser()

        self.assertEqual(m.transform_request("val.hola = 22 and val.pepe > 44"), {'$and': [{'val.hola': 22.0}, {'val.pepe': {'$gt': 44.0} }]})
        self.assertEqual(m.transform_request("val.hola = 22 and (val.pepe > 44 or val.juan < 44)"), {'$and': [{'val.hola': 22.0}, {'$or': [{'val.pepe': {'$gt': 44.0}}, {'val.juan': {'$lt': 44.0}}]}]})
        self.assertEqual(m.transform_request("(val.hola = 22 and val.pepe > 44) or val.juan < 44"), {'$or': [{'$and':[{'val.hola': 22.0}, {'val.pepe': {'$gt': 44.0}}]}, {'val.juan': {'$lt': 44.0}}]})


if __name__ == '__main__':
    unittest.main()
