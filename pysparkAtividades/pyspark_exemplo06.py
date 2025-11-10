from pyspark import SparkContext

sc = SparkContext("local", "ExemploMapNumeros")

# RDD Original
numeros_rdd = sc.parallelize([1, 2, 3, 4, 5])
# Visualmente: [1, 2, 3, 4, 5]

# Aplicando a transformação map para calcular o quadrado de cada número
quadrados_rdd = numeros_rdd.map(lambda numero: numero * numero)
# Visualmente, o plano agora é: [1*1, 2*2, 3*3, 4*4, 5*5]
# NADA FOI EXECUTADO AINDA!

# Agora, chamamos uma ação para executar o plano e ver o resultado
resultado = quadrados_rdd.collect()
# A computação acontece aqui!

print(f"RDD Original: {numeros_rdd.collect()}")
print(f"RDD após o map: {resultado}")
# Saída:
# RDD Original: [1, 2, 3, 4, 5]
# RDD após o map: [1, 4, 9, 16, 25]

sc.stop()