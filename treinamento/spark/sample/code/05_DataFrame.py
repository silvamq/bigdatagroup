
# coding: utf-8

# # Spark Dataframe
#Utilizando as estrutura de Datframe voce pode ler arquivos como parquet,
# json e csv de forma simples e toda estrutora de colunas
# são preservadas e acessivel.
#
# Comando:
#     spark-submit --master spark://master:7077 05_DataFrame.py

# Import SparkContext do modulo pyspark
#from  pyspark import SparkContext

# Import SQLContext do modulo pyspark.sql
from  pyspark.sql import SQLContext

# Inicia SparkContext localmente com 3 nucleos
# Os arquitvos poderam ser processados em 3 nodes paralelos
sc = SparkContext(appName='05_DataFrame')

# Cria SQLContext no SparkContext
sqlContext = SQLContext(sc)

# Le arquivo "json" e cria o DataFrame
df = sqlContext.read.json("/code/data/pessoas.json")

# Imprime os dados do Dataframe em forma de tabela
df.show()

# Imprime Schema do Dataframe
df.printSchema()

# Imprime apenas a coluna nome
df.select("name").show()

# Imprime todos redistros com odeade maior de 23
df.filter(df["age"] > 23).show()

# Imprime agrupamento por idade
df.groupBy("age").count().show()
