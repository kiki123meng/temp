from time import sleep
import time
from bson.objectid import ObjectId
import datetime
from pymongo import MongoClient, ASCENDING
from pymongo.cursor import CursorType
from pymongo.errors import AutoReconnect

# Time to wait for data or connection.
_SLEEP = 1.0

def time2id(from_datetime=None, time_delta=None):
    if from_datetime is None or not isinstance(from_datetime, datetime.datetime):
        from_datetime = datetime.datetime.now()  # 时间元组
       # print(from_datetime)
    if time_delta:  # time_delta 是datetime.timedelta 类型,可以进行时间的加减运算
        from_datetime = from_datetime + time_delta
 
    return ObjectId.from_datetime(from_datetime)
 
 
def id2time(object_id):
    timeStamp = int(object_id[:8], 16)
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timeStamp))

if __name__ == '__main__':
    oplog = MongoClient("mongodb://localhost:27017/?directConnection=true").local.oplog.rs
    stamp = oplog.find().sort('$natural', ASCENDING).limit(-1).next()['wall']
#   print(stamp)
#   print("你哈嘎嘎哈哈哈哈哈哈哈哈哈")
    #print(ObjectId("62958e6b9863e55c5be7c8b2").getTimestamp())
   # a = new ObjectId("62958e6b9863e55c5be7c8b2")
    #print(a.getTimestamp())
    while True:
        kw = {}

        kw['filter'] = {'op':'i','wall': {'$gt': stamp},'ns':'test.col'}
        kw['cursor_type'] = CursorType.TAILABLE_AWAIT
        kw['oplog_replay'] = True

        cursor = oplog.find(**kw)
        
        try:
            while cursor.alive:
                for doc in cursor:
                    stamp = doc['o']
                    cur = stamp['_id']
                    #print(time.mktime(cur.generation_time.timetuple))
                    #print(stamp)

                    print(id2time(str(cur)))
                    print(doc['wall'])
                    #print(doc)  # Do something with doc.
                    #print(doc['ts'])
                sleep(_SLEEP)

        except AutoReconnect:
            sleep(_SLEEP)