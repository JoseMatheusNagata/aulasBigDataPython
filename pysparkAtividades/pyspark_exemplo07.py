from pyspark import SparkContext

# Inicializa o SparkContext
sc = SparkContext("local", "ExemploParallelize")

versao = sc.version # versão
print(f"Versão do Spark em execução: {(versao)}")
num_particoes = sc.defaultParallelism # número de partições
print(f"Partições: {(num_particoes)}\n")

# 1. Cria uma coleção de dados local (uma lista Python)
dados_locais = ["maçã", "banana", "laranja", "uva", "morango", "abacaxi"]

# 2. Usa parallelize para criar um RDD a partir da lista
# O Spark divide esses dados para processamento em paralelo
frutas_rdd = sc.parallelize(dados_locais)

# 3. Agora você pode usar as operações de RDD
total_de_frutas = frutas_rdd.count()
print(f"O RDD foi criado com sucesso e contém {total_de_frutas} frutas.")

# Visualizando o RDD (trazendo os dados de volta para o driver)
print(f"Conteúdo do RDD: {frutas_rdd.collect()}")

# Exemplo de transformação
frutas_plural_rdd = frutas_rdd.map(lambda fruta: fruta + "s")
print(f"Frutas no plural: {frutas_plural_rdd.collect()}")

sc.stop()