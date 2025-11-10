from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

# 1. Inicializar a Spark Session
# A SparkSession é o ponto de entrada para usar a funcionalidade do Spark
spark = SparkSession.builder \
    .appName("Análise Prontuário Paciente") \
    .getOrCreate()

# 2. Criar dados de amostra (simulação de registros de pacientes)
# Um DataFrame é uma coleção distribuída de dados organizada em colunas nomeadas.
dados = [
    (1, "Maria Silva", 35, "Feminino", "Diabetes"),
    (2, "João Santos", 62, "Masculino", "Hipertensão"),
    (3, "Ana Costa", 48, "Feminino", "Diabetes"),
    (4, "Pedro Lima", 71, "Masculino", "Doença Cardíaca"),
    (5, "Sofia Alves", 35, "Feminino", "Diabetes"),
    (6, "Carlos Melo", 55, "Masculino", "Hipertensão"),
    (7, "Luisa Fernandes", 29, "Feminino", "Asma"),
    (8, "Rui Gomes", 62, "Masculino", "Hipertensão")
]

# Definir o esquema (nomes e tipos de colunas)
colunas = ["id_paciente", "nome", "idade", "genero", "condicao_medica"]

# Criar o DataFrame
df_pacientes = spark.createDataFrame(dados, colunas)

# 3. Mostrar os dados originais
print("--- DataFrame Original de Pacientes ---")
df_pacientes.show()

# 4. Exemplo de Análise: Contar o número de pacientes por Condição Médica
# Usamos a operação `groupBy` para agrupar por 'condicao_medica' e `count` para contar os registros em cada grupo.
contagem_por_condicao = df_pacientes.groupBy("condicao_medica").count()

# 5. Mostrar o resultado da contagem
print("--- Contagem de Pacientes por Condição Médica ---")
contagem_por_condicao.show()

# 6. Exemplo de Filtragem: Pacientes com "Diabetes" e com menos de 50 anos
# Usamos `filter` para selecionar linhas que satisfazem as condições.
diabetes_jovens = df_pacientes.filter(
    (col("condicao_medica") == "Diabetes") & (col("idade") < 50)
).select("nome", "idade", "genero") # Selecionamos apenas as colunas de interesse

# 7. Mostrar o resultado da filtragem
print("--- Pacientes com Diabetes (menos de 50 anos) ---")
diabetes_jovens.show()

# 8. Parar a Spark Session
spark.stop()