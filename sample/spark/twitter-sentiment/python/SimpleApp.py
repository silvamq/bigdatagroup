from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("app")
sc = SparkContext(conf=conf)

rdd = sc.textFile('file:///spark-2.4.0-bin-hadoop2.7/README.md')

print(rdd.collect())

sc.stop()
