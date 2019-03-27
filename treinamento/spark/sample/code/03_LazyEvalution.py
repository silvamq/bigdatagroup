
# coding: utf-8

# # Iniciamos com comando de map e flatmap

# Primeramente vamos carregar um arquivo em um RDD e trataremos com map
# e flatmap para vizualizar suas diferencas
#
# Comando:
#     spark-submit --master spark://master:7077 03_LazyEvalution.py

# Import SparkContext do modulo pyspark
from  pyspark import SparkContext

# Inicia SparkContext localmente com 3 nucleos
# Os arquitvos poderam ser processados em 3 nodes paralelos
sc = SparkContext(appName='03_LazyEvalution')

# Comando para ler arquivo TXT e converter em um RDD
rdd = sc.textFile("/code/data/message.txt")

# O "Take" imprime cada item do array gerado no rdd
# No caso o paramentro 3 ira imprimir as 3 primeiras linhas do arquivo tranformado em RDD
rdd.take(3)

# Com comando "map" vamos processar linha a linha do arquivo
# O comando split irá tranforma cada linha em um array de palavras
words = rdd.map(lambda l: l.split(' '))

# Imprime 3 primeras linhas/Array do arquivo tratado
words.take(3)

# Com comando "flatMap" é paracedo com map", ele processa linhas a linhas
# mas tranforma cada item do array em uma item do RDD
wordsFlat = rdd.flatMap(lambda l: l.split(' '))

# Imprime 3 primeras linhas/Array do arquivo tratado
wordsFlat.take(3)

wordsFilter = wordsFlat.filter(lambda l: l == 'curso')

# Imprime 3 primeras linhas/Array do arquivo tratado
wordsFilter.take(3)
