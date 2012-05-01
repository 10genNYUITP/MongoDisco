from app import MongoSplitter as MS
import logging
config = {
        "db_name": "test",
        "collection_name": "in",
        "splitSize": 1, #MB
        "inputURI": "mongodb://localhost/test.in",
        "createInputSplits": False,
        "splitKey": {'_id' : 1}, }



def test_single_split():
    splits = MS.calculate_splits(config)
    print splits
    assert splits

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.DEBUG)
    test_single_split()
