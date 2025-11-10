from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col

# 1. Inicializa a SparkSession
# Este é o ponto de partida. Ele configura o ambiente Spark.
spark = SparkSession.builder \
    .appName("Salário Médio Distribuído") \
    .master("local[*]") \
    .getOrCreate()

# Reduz o nível de log para manter o console limpo (opcional, mas recomendado)
spark.sparkContext.setLogLevel("ERROR")

# --- 2. Criação do DataFrame Simples (A Tabela de Dados) ---

# Dados de exemplo: Nome, Departamento e Salário
dados = [
    ("Alice", "Vendas", 50000),
    ("Bob", "TI", 75000),
    ("Catarina", "Vendas", 60000),
    ("David", "TI", 80000),
    ("Eva", "Vendas", 55000),
    ("Fábio", "RH", 45000),
]

# Cria o DataFrame (tabela distribuída)
colunas = ["Nome", "Departamento", "Salario"]
df = spark.createDataFrame(dados, colunas)

print("--- DataFrame Inicial (Dados Distribuídos) ---")
df.printSchema() # Mostra o tipo de dado de cada coluna
df.show()        # Mostra as primeiras linhas da tabela

# --- 3. Agregação Básica Distribuída ---

# O PySpark divide esta operação entre os "workers" (processadores)

# Tarefa: Calcular o Salário Médio por Departamento.

df_media_salarial = df.groupBy("Departamento") \
                       .agg(avg("Salario").alias("Salario_Medio")) \
                       .orderBy(col("Salario_Medio").desc())

# Ação: Exibir o resultado
print("\n--- Resultado: Salário Médio por Departamento ---")
df_media_salarial.show()

# Encerra a SparkSession
# É vital para liberar recursos do sistema.
spark.stop()