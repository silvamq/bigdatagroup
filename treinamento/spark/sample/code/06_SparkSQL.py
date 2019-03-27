
# coding: utf-8

# # Spark SQL
#Utilizando tabelas registardo no SQLCOntext voce pode executar
# queris de forma simples e gerar nosvos DataFrames a partir instrucoes SQL.
#
# Comando:
#     spark-submit --master spark://master:7077 06_SparkSQL.py

# Import SparkContext do modulo pyspark
#from  pyspark import SparkContext

# Import SQLContext do modulo pyspark.sql
from  pyspark.sql import SQLContext

# Inicia SparkContext localmente com 3 nucleos
# Os arquitvos poderam ser processados em 3 nodes paralelos
sc = SparkContext(appName='06_SparkSQL')

# Cria SQLContext no SparkContext
sqlContext = SQLContext(sc)

# Le arquivo "json" e cria o DataFrame
df = sqlContext.read.json("/code/data/pessoas.json")

# Registra a tabela para sqlContext
df.registerTempTable("pessoas")

# Cria um Dataframe com todos os dados
sqlDF = sqlContext.sql("SELECT * FROM pessoas")

# Imprime os dados do Dataframe em forma de tabela
sqlDF.show()

# Imprime apenas a coluna nome
sqlContext.sql("SELECT name FROM pessoas").show()

# Imprime todos redistros com odeade maior de 23
sqlContext.sql("SELECT * FROM pessoas where age > 23").show()

# Imprime agrupamento por idade
sqlContext.sql("SELECT age, count(1) FROM pessoas group by age").show()
