from pyspark import SparkContext

try:
    sc = SparkContext("local", "ExemploBasicoSC")
    print("SparkContext iniciado com sucesso!")

    dados = [
        "Apache Spark é um mecanismo de análise unificado",
        "Ele fornece processamento de dados em larga escala",
        "map e collect são conceitos fundamentais",
        "Este é um exemplo simples"
    ]
    
    dados_rdd = sc.parallelize(dados)
    contagem_rdd = dados_rdd.map(lambda frase: (frase, len(frase.split())))
    
    print("\nExecutando a ação 'collect()' para obter os resultados...")
    resultados = contagem_rdd.collect()
   
    print("\n--- Resultados Finais ---")
    for frase, contagem in resultados:
        print(f'A frase "{frase}" contém {contagem} palavras.')

finally:
    sc.stop()
    print("\nSparkContext parado.")