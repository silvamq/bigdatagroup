from pyspark import SparkContext
from pyspark.streaming import StreamingContext

import pymongo

sc = SparkContext(appName="SparkStreamingKafka")
ssc = StreamingContext(sc, 30)

lines = ssc.socketTextStream("tcp", 9999)

pairs = lines.map(lambda l: ("U", l))
concat = pairs.reduceByKey(lambda x, y: x + ' ' + y)

def sebdMongo(msg):

    myclient = pymongo.MongoClient("mongodb://mongo:27017/")
    mydb = myclient["mydatabase"]
    mycol = mydb["message"]
    x = mycol.insert_one({"msg":msg})
    return msg

countw = concat.map(lambda l: sebdMongo(l[1]))
countw.pprint()

ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate
