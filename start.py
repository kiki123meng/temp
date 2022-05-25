#启动
docker run -itd -p 28017:27017 mongo --replSet rs

#install mongosh
# https://www.mongodb.com/docs/mongodb-shell/install/

#inti
mongosh "mongodb://localhost:28017"
rs.initiate()

#find _id
use local
db.oplog.rs.find()

#运行python路径有点问题
python /workspace/temp/pymongotest.py

