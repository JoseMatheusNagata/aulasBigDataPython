# Importa a classe SparkContext do módulo pyspark
from pyspark import SparkContext

# Cria uma instância do SparkContext.
# "local" indica que o Spark será executado em modo local na sua máquina.
# "ExemploRDD" é o nome da nossa aplicação.
sc = SparkContext("local", "ExemploRDD")

# 1. Criação de um RDD
# Utilizamos o método parallelize para criar um RDD a partir de uma lista de números.
# Este RDD representa uma coleção distribuída de dados.
numeros_rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])

# a) Mapeamento (map): Multiplica cada elemento do RDD por 2.
# A função lambda é aplicada a cada elemento do 'numeros_rdd'.
# O resultado é um novo RDD chamado 'numeros_multiplicados_rdd'.
numeros_multiplicados_rdd = numeros_rdd.map(lambda x: x * 2)

# b) Filtro (filter): Filtra o RDD para manter apenas os números pares.
# A função lambda retorna True para os elementos que devem ser mantidos.
# O resultado é um novo RDD chamado 'numeros_pares_rdd'.
numeros_pares_rdd = numeros_multiplicados_rdd.filter(lambda x: x % 2 == 0)

# 3. Ação no RDD
# Uma ação dispara a execução de todas as transformações anteriores.

# a) Coleta (collect): Traz todos os elementos do RDD para o nó driver (sua máquina local).
resultado = numeros_pares_rdd.collect()

# Exibe o resultado final
print("O resultado final após as transformações é:")
print(resultado)

# Outra ação comum: count() - para contar o número de elementos no RDD
contagem = numeros_pares_rdd.count()
print(f"A contagem de elementos no RDD final é: {contagem}")

# Encerra a sessão do SparkContext
sc.stop()