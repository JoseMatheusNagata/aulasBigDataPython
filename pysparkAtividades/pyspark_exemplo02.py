from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# 1. Iniciar uma SparkSession
spark = SparkSession.builder \
    .appName("ExemploColunaDerivadaPySpark") \
    .master("local[*]") \
    .getOrCreate()

# 2. Criar dados de exemplo
dados = [
    ("Galbas", 85),
    ("Douglas", 42),
    ("Milton", 91),
    ("Marina", 68),
    ("Flavia", 55)
]

# Definir os nomes das colunas
colunas = ["Aluno", "Pontuacao"]

# 3. Criar o DataFrame
df = spark.createDataFrame(dados, colunas)

# 4. Exibir o DataFrame original
print("DataFrame Original:")
df.show()

# 5. Operação Simples: Criar uma coluna 'Status'
# Se a pontuação for >= 70, o status é 'Aprovado', caso contrário, é 'Reprovado'.
df_final = df.withColumn(
    "Status",
    when(col("Pontuacao") >= 70, "Aprovado")
    .otherwise("Reprovado")
)

# 6. Exibir o resultado com a nova coluna
print("DataFrame com Coluna Derivada 'Status':")
df_final.show()

# 7. Parar a SparkSession
spark.stop()