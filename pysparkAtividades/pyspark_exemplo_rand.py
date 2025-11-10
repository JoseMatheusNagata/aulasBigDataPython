from pyspark.sql import SparkSession
# Importa as funções de Big Data que geram números aleatórios
from pyspark.sql.functions import col, rand, floor, count

# 1. Inicializa a SparkSession
spark = SparkSession.builder \
    .appName("SimulacaoDados") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Define o número de lances a simular (simulação de grande volume)
NUM_LANCES = 1000000

# --- 2. Geração Distribuída de Números Aleatórios (Dados) ---

# Passo A: Cria 1 milhão de linhas, numeradas de 0 a 999.999
# 'range()' é a maneira mais eficiente do Spark de criar grandes volumes de linhas.
df_lances = spark.range(NUM_LANCES).withColumnRenamed("id", "lance_id")

# Passo B: Gera 3 colunas de dados aleatórios (D1, D2, D3)
# rand() gera números decimais entre [0, 1).
# Multiplicamos por 6 e somamos 1 para obter o intervalo [1, 6].
# floor() converte o decimal para o inteiro mais baixo.
df_simulacao = df_lances.withColumn("D1", floor(rand() * 6 + 1)) \
                        .withColumn("D2", floor(rand() * 6 + 1)) \
                        .withColumn("D3", floor(rand() * 6 + 1))

print("--- DataFrame de Simulação (1 milhão de linhas) ---")
# Mostra apenas 10 lances, mas a tabela tem 1.000.000 de linhas
df_simulacao.show(10)

# --- 3. Transformação e Agregação (Regras do Jogo) ---

# Passo C: Calcula o Resultado do Lance (D1 + D2 + D3)
df_resultados = df_simulacao.withColumn("Resultado_Total",
                                       col("D1") + col("D2") + col("D3"))

# Passo D: Agregação - Conta a frequência de cada Resultado_Total
# Esta é a etapa final de Big Data, onde o Spark soma os resultados em paralelo.
df_frequencia = df_resultados.groupBy("Resultado_Total") \
                             .agg(count("*").alias("Total_Ocorrencias")) \
                             .orderBy(col("Resultado_Total").asc())

# Ação: Exibir o insight do jogo
print("\n--- Frequência Distribuída dos Resultados (3d6) ---")
# O resultado mais comum deve ser 10 ou 11 (o pico da curva em forma de sino)
df_frequencia.show()

# Encerra a SparkSession
spark.stop()