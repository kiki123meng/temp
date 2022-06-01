import time
from bson.objectid import ObjectId
from pymongo import MongoClient, DESCENDING
from pymongo.cursor import CursorType
from pymongo.errors import AutoReconnect

if __name__ == '__main__':
    oplog = MongoClient("mongodb://localhost:27017/?directConnection=true").local.oplog.rs
    # todo: consider rename stamp, more explicit and intutive
    curtime_stamp = oplog.find().sort('$natural', ASCENDING).limit(1).next()['wall']

    # todo: 过滤掉过去的数据，只计算当前时间点往后的oplog延时
    while True:
        # todo: what meaing kw? make variable names more explicit and intutive
        search_arguments = {}
        search_argumentssearch_arguments['filter'] = {'op':'i','wall': {'$gt': curtime_stamp}, 'ns':{'$regex':'[0-9]+_TradeFlow.(TradeRecord|OrderRecord)'}}
        search_arguments['cursor_type'] = CursorType.TAILABLE_AWAIT
        search_arguments['oplog_replay'] = True

        cursor = oplog.find(**search_arguments)
        
        try:
            while cursor.alive:
                for doc in cursor:
                    # todo: rename stamp to raw_data 
                    raw_data = doc['o'] 
                    # todo: rename cur
                    data_id = stamp['_id']
                    src_gen_time = (cur.generation_time).replace(tzinfo=None)
                    dst_gen_time = doc['wall']
                    delay = (dst_gen_time - src_gen_time).total_seconds()
                    print(delay)  # todo something with doc.
                time.sleep(0.1)
        except AutoReconnect:
            time.sleep(0.1)
