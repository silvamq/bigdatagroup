
# coding: utf-8

# # Gerenciando Partições Spark com Coalesce e Repartition
#
# O algoritmo de "repartition" faz uma mistura completa de dados
# e distribui igualmente os dados entre as partições.
# Ele não tenta minimizar o movimento de dados como o algoritmo de "coalesce".
#
# O algoritmo de "coalesce" dimuir as partioes movendo de forma otimizada
# para manter os dados originais intactos e apenas adicionando os partições
# movidas. Com "coalesce" não é possivel aumanter o numero de partições,
# para isso é necessario utilizar o "repartition"
#
# Comando:
#     spark-submit --master spark://master:7077 02_Partition.py

# Import SparkContext do modulo pyspark
from  pyspark import SparkContext

# Inicia SparkContext localmente com 3 nucleos
# Os arquitvos poderam ser processados em 3 nodes paralelos
sc = SparkContext(appName='02_Partition')

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

# Redimenciona as partitoes para 2
rddReapt2 = rdd.repartition(2)

# Imprime as particoes em forma de Array
rddReapt2.glom().collect()

# Redimenciona as partitoes para 1
rddReapt1 = rdd.repartition(1)

# Imprime as particoes em forma de Array
rddReapt1.glom().collect()

# Redimenciona as partitoes para 1
rddReapt3 = rddReapt1.repartition(3)

# Imprime as particoes em forma de Array
rddReapt3.glom().collect()

# Redimenciona as partitoes para 1
rddCoal2 = rddReapt3.coalesce(2)

# Imprime as particoes em forma de Array
rddCoal2.glom().collect()
