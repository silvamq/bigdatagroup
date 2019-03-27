# coding: utf-8

# # Meu Primeiro RDD
# Inciaremos criando um Array simples na linguagem
# Python e apos tranformoremos este Array em RDD
# Comando:
#     spark-submit --master spark://master:7077 01_FirstRDD.py

# Import SparkContext do modulo pyspark
from  pyspark import SparkContext

# Inicia SparkContext localmente com 3 nucleos
# Os arquitvos poderam ser processados em 3 nodes paralelos
sc = SparkContext(appName="01_FirstRDD")

# Cria um array com 10 posicoes [0,1,2,3,4,5,6,7,8,9]
nums = range(0, 10)

# Imprime o Array para visualizacao
print (nums)

# tranforma o array em um rdd distribuido em 3 partes
rdd = sc.parallelize(nums, 3)

# verifica numero de particoes
rdd.getNumPartitions()

# Imprime as particoes em forma de Array
rdd.glom().collect()

# Sava cada particao com os dados corrependentes
rdd.saveAsTextFile('/code/output/exerc01')
