from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc = SparkContext(appName="SparkStreamingKafka")
ssc = StreamingContext(sc, 5)

lines = ssc.socketTextStream("localhost", 9999)

words = lines.flatMap(lambda line: line.split(" "))

pairs = words.map(lambda word: (word, 1))
wordCounts = pairs.reduceByKey(lambda x, y: x + y)

wordCounts.pprint()

ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate
