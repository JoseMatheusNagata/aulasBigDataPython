from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

# 1. Iniciar uma SparkSession
spark = SparkSession.builder \
    .appName("ExemploAgregacaoPySpark") \
    .master("local[*]") \
    .getOrCreate()

# 2. Criar dados de exemplo
dados = [
    ("Galbas", "Vendas"),
    ("Leonardo", "Marketing"),
    ("Alfredo", "Vendas"),
    ("Roselene", "TI"),
    ("Giovana", "Marketing"),
    ("João paulo", "Vendas")
]

# Definir os nomes das colunas
colunas = ["Nome", "Departamento"]

# 3. Criar o DataFrame
df = spark.createDataFrame(dados, colunas)

# 4. Exibir o DataFrame original
print("DataFrame Original:")
df.show()

# 5. Operação Simples: Contar o número de funcionários por Departamento
# Agrupa pelo 'Departamento' e, em seguida, conta as linhas em cada grupo.
df_agregado = df.groupBy("Departamento").agg(count(col("Nome")).alias("TotalFuncionarios"))

# 6. Exibir o resultado da agregação
print("Contagem de Funcionários por Departamento:")
df_agregado.show()

# 7. Parar a SparkSession
spark.stop()