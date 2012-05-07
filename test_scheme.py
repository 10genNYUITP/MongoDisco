from disco.util import kvgroup
import unittest
import mongodb_input

'''
File: test_input.py
Description: Tests the new scheme - scheme_mongodb created. When provided with an inputURI, this test attempts to read the data at the URI by making use of the wrapper returned.
Author/s: NYU ITP team
'''

class TestScheme(unittest.TestCase):
    def runTest(self):
        mongo_uri = "mongodb://localhost/test.modforty"
        wrapper = mongodb_input.open(mongo_uri)
        assert wrapper
        for result in wrapper:
            print result
            for key in result:
                print result[key]
            assert result
            break

if __name__ == '__main__':
    unittest.main()
