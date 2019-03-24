from pyspark import SparkContext
from pyspark.streaming import StreamingContext

from kafka import KafkaProducer

sc = SparkContext(appName="SparkStreamingKafka")
ssc = StreamingContext(sc, 10)

lines = ssc.socketTextStream("tcp", 9999)

pairs = lines.map(lambda l: ("U", l))
concat = pairs.reduceByKey(lambda x, y: x + ' ' + y)

def sendKafka(msg):
    producer = KafkaProducer(bootstrap_servers='kafka:9092')
    producer.send('test_topic', msg.encode())
    return msg

countw = concat.map(lambda l: sendKafka(l[1]))
countw.pprint()

ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate
