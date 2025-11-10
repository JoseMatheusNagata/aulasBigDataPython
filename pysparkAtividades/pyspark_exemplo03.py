from pyspark.sql import SparkSession

# 1. Iniciar uma SparkSession
# 'local[*]' usa todos os núcleos disponíveis na máquina local
spark = SparkSession.builder \
    .appName("ExemploSimplesPySpark") \
    .master("local[*]") \
    .getOrCreate()

# 2. Criar dados de exemplo
dados = [
    ("Galbas", 53),
    ("Douglas", 56),
    ("Valmir", 35),
    ("Cristina", 25)
]

# Definir os nomes das colunas
colunas = ["Nome", "Idade"]

# 3. Criar o DataFrame
df = spark.createDataFrame(dados, colunas)

# 4. Exibir o DataFrame original
print("DataFrame Original:")
df.show()

# 5. Operação Simples: Filtrar pessoas com mais de 25 anos
df_filtrado = df.filter(df["Idade"] > 25)

# 6. Exibir o DataFrame Filtrado
print("DataFrame Filtrado (Idade > 25):")
df_filtrado.show()

# 7. Parar a SparkSession
spark.stop()