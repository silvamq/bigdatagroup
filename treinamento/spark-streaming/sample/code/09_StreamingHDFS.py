
# coding: utf-8

# # Spark Streaming
#Para Spark Streaming será necessario executar na linhas de comando.

#     Execute o programa 09_StreamingHDFS.py com comando abaixo:
#     $ spark-submit --master spark://master:7077 09_StreamingHDFS.py
#     Obs: necessario estar na pasta bin dentro do diretorio do spark

# Import SparkContext do modulo pyspark
from  pyspark import SparkContext

# Import StreamingContext do modulo pyspark.streaming
from pyspark.streaming import StreamingContext

# Inicia SparkContext localmente com 3 nucleos
# Os arquitvos poderam ser processados em 3 nodes paralelos
#sc = SparkContext("local[3]")
sc = SparkContext(appName="WordCount")

# Cria StreamingContext no SparkContext coma a janela de 3 segundos
ssc = StreamingContext(sc, 3)

# Cria um DStream que smonitora o HDFS, folder /user/cloudera/streaming/
lines = ssc.textFileStream("/user/cloudera/streaming/")

# Divide cada linha em palavras
words = lines.flatMap(lambda line: line.split(" "))

# Conta cada palavra em cada lote
pairs = words.map(lambda word: (word, 1))
wordCounts = pairs.reduceByKey(lambda x, y: x + y)

# Imprima os dez primeiros elementos de cada RDD gerado neste DStream para o console
wordCounts.pprint()

# Inicia o servico
ssc.start()
# Aguarde o servico terminar
ssc.awaitTermination()
