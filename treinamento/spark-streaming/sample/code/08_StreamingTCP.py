
# coding: utf-8

# # Spark Streaming
#Para Spark Streaming será necessario executar na linhas de comando.

# 1 - Crie o arquivo streaming.py com codigo abaixo
# 2 - Crie um linester na por 9999 com comando abaixo:
#     $ nc -lk 9999
# 3 - Execute o programa 08_StreamingTCP.py com comando abaixo:
#     $ spark-submit --master spark://master:7077 08_StreamingTCP.py

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

# Cria um DStream que se conecta a url localhost e porta 9999
lines = ssc.socketTextStream("localhost", 9999)
#lines = ssc.textFileStream("/user/cloudera/streaming/")

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
