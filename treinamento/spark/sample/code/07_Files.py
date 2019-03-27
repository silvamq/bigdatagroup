
# coding: utf-8

# # Spark Files
#Agora vamos tranalhar com alguns tipos de arquivos que
# spark tem compatibilidade naturalmente.
#
# Comando:
#     spark-submit --master spark://master:7077 07_Files.py

# Import SparkContext do modulo pyspark
from  pyspark import SparkContext

# Import SQLContext do modulo pyspark.sql
from  pyspark.sql import SQLContext

# Inicia SparkContext localmente com 3 nucleos
# Os arquitvos poderam ser processados em 3 nodes paralelos
sc = SparkContext(appName='07_Files')

# Cria SQLContext no SparkContext
sqlContext = SQLContext(sc)

# Comando para ler arquivo TXT e converter em um RDD
rdd = sc.textFile("/code/data/message.txt")

# Imprime os dados do DataFrame como tabela
rdd.collect()

# Le arquivo "json" e cria o DataFrame
df = sqlContext.read.json("/code/data/pessoas.json")

# Imprime os dados do DataFrame como tabela
df.show()

# Convert arquivo "json" para parquet
df.write.parquet("pessoas.parquet")

# Le arquivo "parquet" e cria o DataFrame Novo
dfparq = sqlContext.read.parquet("pessoas.parquet")

# Le o Schema do DataFrame novo
dfparq.printSchema()

# Imprime os dados do DataFrame como tabela
dfparq.show()

# e arquivo "csv" considerando a primeira linha como header e cria o DataFrame
#dfcsv = sqlContext.read.format("csv").option("header", "true").load("pessoas.csv")

# Imprime os dados do DataFrame como tabela
#dfcsv.show()
