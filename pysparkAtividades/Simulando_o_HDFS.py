#Dados de entrada (simulando o HDFS)

dados_texto = [
    "o gato corre",
    "o cão corre, corre",
    "o gato e o cão"
]

print("--- 1. As Frases (O 'Big Data' no HDFS) ---")
for frase in dados_texto:
    print(f"-> {frase}")
print("------------------------------------------\n")

#A Fase MAP (A Atribuição de Tarefas)
#Vamos transformar cada frase em uma lista de todas as suas palavras (limpando vírgulas).

def mapear_palavras(frases):
    """
    Simula o MAP: Pega a frase e transforma em uma lista de palavras.
    Cada palavra é uma tarefa para ser contada.
    """
    todas_as_palavras = []

    for frase in frases:
        # Pega as palavras, remove vírgulas e coloca em minúsculas
        palavras = frase.lower().replace(',', '').split()
        todas_as_palavras.extend(palavras)

    return todas_as_palavras

palavras_isoladas = mapear_palavras(dados_texto)

print("--- 2. Resultado do MAP (Todas as Palavras Isoladas) ---")
# Mostra apenas as 10 primeiras para não ser confuso:
print(palavras_isoladas)
print(f"Total de Palavras encontradas: {len(palavras_isoladas)}")

#A Fase REDUCE (A Contagem Final)

def contar_palavras(palavras):
    """
    Simula o REDUCE: Agrupa todas as palavras e faz a contagem final.
    """
    contagem_final = {}

    for palavra in palavras:
        # Se a palavra já estiver no dicionário, soma 1; caso contrário, começa em 1.
        if palavra in contagem_final:
            contagem_final[palavra] += 1
        else:
            contagem_final[palavra] = 1

    return contagem_final

# Executa a contagem
resultado_final = contar_palavras(palavras_isoladas)

print("\n--- 3. Resultado do REDUCE (A Contagem Final) ---")
for palavra, contagem in resultado_final.items():
    print(f"'{palavra}': {contagem}")
print("--------------------------------------------------")
