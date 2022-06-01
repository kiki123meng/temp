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
#对于find,https://www.cnblogs.com/huang-yc/p/10453275.html
result = collection.find({'age':{'$gt':20}})

#关于oplog中一些字段的含义，
#op: the write operation that should be applied to the slave. n indicates a no-op, this is just an informational message.
#ns: the database and collection affected by this operation. Since this is a no-op, this field is left blank.
#o: the actual document representing the op. Since this is a no-op, this field is pretty useless._id这个字段就在这里，这里也有很多i,n,c等
#i:insert;u:updata;d:delete;c:db cmd;n:空操作
#wall。毫秒粒度墙上时钟。
#需要注意的是，o字段的格式与其他字段不同，o字段是bson.D的结构（数组嵌套字段）

#关于将数据写入mongodb,主要是insert(),insertMany()方法
#db.collection.insertOne(
#  <document>,
#    {
#      writeConcern: <document>
#  }
#)
#_id是主键，对应objectid("...")