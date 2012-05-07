from app import MongoSplitter as MS
import logging
config = {
        "db_name": "test",
        "collection_name": "in",
        "split_size": 1, #MB
        "input_uri": "mongodb://localhost/test.in",
        "create_input_splits": False,
        "split_key": {'_id' : 1}, }



def test_single_split():
    splits = MS.calculate_splits(config)
    print splits
    assert splits

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.DEBUG)
    test_single_split()
