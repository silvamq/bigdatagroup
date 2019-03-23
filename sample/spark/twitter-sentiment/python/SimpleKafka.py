
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

sc = SparkContext(appName="PythonSparkStreamingKafka")
ssc = StreamingContext(sc,60)

kafkaStream = KafkaUtils.createStream(ssc, 'kafka:9292', 'spark-streaming', {'test':1})

lines = kafkaStream.map(lambda x: x[1])
lines.pprint()

ssc.start()
ssc.awaitTermination()
