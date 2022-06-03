import time
from bson.objectid import ObjectId
from pymongo import MongoClient, DESCENDING
from pymongo.cursor import CursorType
from pymongo.errors import AutoReconnect
from prometheus_client import Counter, Gauge, Summary
from prometheus_client import start_http_server

if __name__ == '__main__':
    oplog = MongoClient("mongodb://localhost:27017/?directConnection=true").local.oplog.rs
    start_timestamp = oplog.find().sort('$natural', DESCENDING).limit(-1).next()['wall']
    print(start_timestamp)

    start_http_server(8081)
    # todo: 整理命名和描述
    c = Counter('record_counter', 'Description of counter', ['src_host_name', 'collection'])

    # todo: 整理命名和描述
    g = Gauge('record_latency', 'Description of gauge', ['src_host_name', 'collection'])

    # todo: 整理命名和描述
    s = Summary('record_latency_summary', 'xx', ['src_host_name', 'collection'])

    while True:
        search_arguments = {}
        search_arguments['filter'] = {'op': 'i', 'wall': {'$gt': start_timestamp},
                                      'ns': {'$regex': '[0-9]+_TradeFlow.(TradeRecord|OrderRecord)'}}
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
                    collection_name = doc['ns'].split('.')[1]

                    # todo: 整理命名和描述
                    c.labels(raw_data['src_host_name'], collection_name).inc()
                    g.labels(raw_data['src_host_name'], collection_name).set(delay)
                    s.labels(raw_data['src_host_name'], collection_name).observe(delay)

                time.sleep(0.1)
        except AutoReconnect:
            time.sleep(0.1)
