
# coding: utf-8

# # Iniciamos com comando de map e flatmap
# Primeramente vamos carregar um arquivo em um RDD e trataremos com map
# e flatmap para vizualizar suas diferencas
#
# Comando:
#     spark-submit --master spark://master:7077 04_WordCount.py

# Import SparkContext do modulo pyspark
from  pyspark import SparkContext

# Inicia SparkContext localmente com 3 nucleos
# Os arquitvos poderam ser processados em 3 nodes paralelos
sc = SparkContext(appName='04_WordCount')

# Comando para ler arquivo TXT e converter em um RDD
rdd = sc.textFile("/code/data/message.txt")

# Comando tranforma todas palavras em tupla chave o nome da palavra e com valor 1
# as tuplas neste comando estao repetidas ainda
words = rdd.flatMap(lambda line: line.split()).map(lambda word: (word, 1))

# Imprime 3 primeras linhas/Array
words.take(3)

# reduce ira manter apenas umas chave
# e a cada chave processadas ele soma o valor atribuido e adiona na chave principal
wordCount = words.reduceByKey(lambda a, b: a+b)

# Imprime 3 primeras linhas/Array
wordCount.take(3)

# Imprime 3 primeras linhas/Array com filtro das palabras com 2 ou mais repeticao
wordCount.filter(lambda l : l[1] >= 2).take(3)
