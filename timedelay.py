import time
import click
from bson.objectid import ObjectId
from pymongo import MongoClient, DESCENDING
from pymongo.cursor import CursorType
from pymongo.errors import AutoReconnect
from prometheus_client import CollectorRegistry, Counter, Gauge, Summary, Histogram
from prometheus_client import start_http_server

@click.command
@click.option('--mongo-url', default='mongodb://localhost:27017', help='mongodb://user:password@ip:port')
@click.option('--prometheus-exporter-port', default=8081, help='use for prometheus exporter http port')
def main(mongo_url: str, prometheus_exporter_port: int):
    print(mongo_url, prometheus_exporter_port)
    oplog = MongoClient(f'{mongo_url}/?directConnection=true').local.oplog.rs
    start_timestamp = oplog.find().sort('$natural', DESCENDING).limit(-1).next()['wall']
    # print(start_timestamp)

    start_http_server(prometheus_exporter_port)

    # todo: consider rename c, more intuitive
    c = Counter('documents_total', '统计documents总数', ['src_host_name', 'account_id', 'collection'])

    # todo: consider rename g, more intuitive
    g = Gauge('everydocument_latency', '监听document延时', ['src_host_name', 'account_id', 'collection'])

    # todo: consider rename s, more intuitive
    s = Summary('record_latency_summary', '监听document延时分位', ['src_host_name', 'account_id', 'collection'])

    # todo: 根据待办中的要求，设置bucket, 在构造函数中增加buckets的赋值，具体可以看Histogram构造函数中的解释
    # todo: consider rename h, more intuitive
    h = Histogram('record_latency_histogram','监听document延时直方图', ['src_host_name', 'account_id', 'collection'])

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

                    """
                    1. 对于OrderRecord，document的结构为
                    {
                        "_id": xxxxx
                        "order": {
                            "account_id": "xxxx",
                            "other": "xxxx"
                        }
                        "src_host_name": "xxxxx"   
                    }
                    todo: 因此获取account_id应使用raw_data['order']['account_id'] !!!!!!!

                    2. 对于TradeRecord，document的结构为
                    {
                        "_id": xxxxx
                        "account_id": "xxxx",
                        "other": "xxxx"
                        "src_host_name": "xxxxx"   
                    }
                    直接使用raw_data['account_id']没有问题
                    """
                    
                    c.labels(raw_data['src_host_name'], raw_data['account_id'], collection_name).inc()
                    g.labels(raw_data['src_host_name'], raw_data['account_id'], collection_name).set(delay)
                    s.labels(raw_data['src_host_name'], raw_data['account_id'], collection_name).observe(delay)
                    h.labels(raw_data['src_host_name'], raw_data['account_id'], collection_name).observe(delay)

                time.sleep(0.1)
        except AutoReconnect:
            time.sleep(0.1)

if __name__ == '__main__':
    main()