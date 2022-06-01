import time
from bson.objectid import ObjectId
from pymongo import MongoClient, ASCENDING
from pymongo.cursor import CursorType
from pymongo.errors import AutoReconnect

if __name__ == '__main__':
    oplog = MongoClient("mongodb://localhost:27017/?directConnection=true").local.oplog.rs
    # todo: consider rename stamp, more explicit and intutive
    stamp = oplog.find().sort('$natural', ASCENDING).limit(-1).next()['wall']

    # todo: 过滤掉过去的数据，只计算当前时间点往后的oplog延时
    while True:
        # todo: what meaing kw? make variable names more explicit and intutive
        kw = {}
        kw['filter'] = {'op':'i','wall': {'$gt': stamp}, 'ns':{'$regex':'[0-9]+_TradeFlow.(TradeRecord|OrderRecord)'}}
        kw['cursor_type'] = CursorType.TAILABLE_AWAIT
        kw['oplog_replay'] = True

        cursor = oplog.find(**kw)
        
        try:
            while cursor.alive:
                for doc in cursor:
                    # todo: rename stamp to raw_data 
                    stamp = doc['o'] 
                    # todo: rename cur
                    cur = stamp['_id']
                    src_gen_time = (cur.generation_time).replace(tzinfo=None)
                    dst_gen_time = doc['wall']
                    delay = (dst_gen_time - src_gen_time).total_seconds()
                    print(delay)  # todo something with doc.
                time.sleep(0.1)
        except AutoReconnect:
            time.sleep(0.1)
