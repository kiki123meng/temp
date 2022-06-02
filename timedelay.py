import time
from bson.objectid import ObjectId
from pymongo import MongoClient, DESCENDING
from pymongo.cursor import CursorType
from pymongo.errors import AutoReconnect

if __name__ == '__main__':
    oplog = MongoClient("mongodb://localhost:27017/?directConnection=true").local.oplog.rs
    curtime_stamp = oplog.find().sort('$natural', DESCENDING).limit(-1).next()['wall']
    #print(curtime_stamp)
    while True:
        search_arguments = {}
        search_arguments['filter'] = {'op':'i','wall': {'$gt': curtime_stamp}, 'ns':{'$regex':'[0-9]+_TradeFlow.(TradeRecord|OrderRecord)'}}
        search_arguments['cursor_type'] = CursorType.TAILABLE_AWAIT
        search_arguments['oplog_replay'] = True

        cursor = oplog.find(**search_arguments)
        
        try:
            while cursor.alive:
                for doc in cursor:
                    raw_data = doc['o'] 
                    data_id = raw_data['_id']
                    src_gen_time = (data_id.generation_time).replace(tzinfo=None)
                    dst_gen_time = doc['wall']
                    delay = (dst_gen_time - src_gen_time).total_seconds()
                    print(delay)  
                time.sleep(0.1)
        except AutoReconnect:
            time.sleep(0.1)
