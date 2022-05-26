#引入模块，创建链接
from pymongo import MongoClient

client = MongoClient()
#本地默认端口的话就这么写。不然可能需要改动

#本地端口有所调整：
client = MongoClient('mongodb://localhost:28017/')

#指定数据库
db = client.local

#指定集合，oplog.rs是存放日志的集合
collection = db.oplog.rs

#findone()返回单个结果，find()返回一个生成器对象
#对于find，比如说result = collection.find({'age':{'$gt':20}}),https://www.cnblogs.com/huang-yc/p/10453275.html
