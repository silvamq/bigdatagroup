
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

sc = SparkContext(appName="PythonSparkStreamingKafka")
ssc = StreamingContext(sc,10)

lines = KafkaUtils.createStream(ssc, 'zookeeper:2181', 'spark-streaming', {'topic_in':1})

pairs = lines.map(lambda l: ("U", l[1]))
concat = pairs.reduceByKey(lambda x, y: x + ' ' + y)

concat.pprint()

ssc.start()
ssc.awaitTermination()
