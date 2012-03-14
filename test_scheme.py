import unittest
import scheme_mongo
class TestScheme(unittest.TestCase):
    def runTest(self):
        mongo_uri = "mongodb://localhost/test.in"
        wrapper = scheme_mongo.open(mongo_uri)
        assert wrapper
        for result in wrapper:
            print result

if __name__ == '__main__':
    unittest.main()
