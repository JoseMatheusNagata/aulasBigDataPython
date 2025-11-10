# Dados de entrada: (disciplina, nota)
notas = [
    ("Matemática", 8.5),
    ("História", 7.0),
    ("Matemática", 9.0),
    ("Física", 7.5),
    ("História", 6.5),
    ("Matemática", 7.5)
]

def mapear(registro):
    """
    Transforma (disciplina, nota) em (disciplina, (nota, 1)).
    O '1' representa a contagem de uma nota.
    """
    disciplina, nota = registro
    return (disciplina, (nota, 1))

# Aplicando Map
resultados_map = list(map(mapear, notas))
print("Resultado do Map:")
print(resultados_map)
# Saída: [('Matemática', (8.5, 1)), ('História', (7.0, 1)), ('Matemática', (9.0, 1)), ...]

# Agrupando os valores por chave (disciplina)
agrupado = {}
for disciplina, valor in resultados_map:
    if disciplina not in agrupado:
        agrupado[disciplina] = []
    agrupado[disciplina].append(valor)

print("\nResultado do Agrupamento (Shuffle):")
for disciplina, lista_valores in agrupado.items():
    print(f"{disciplina}: {lista_valores}")
# Saída (Exemplo para Matemática): {'Matemática': [(8.5, 1), (9.0, 1), (7.5, 1)], ...}

def reduzir(lista_valores):
    """
    Soma as notas e as contagens e retorna a média calculada (sem formatação ainda).
    """
    soma_notas = 0
    contagem_total = 0

    # Agrega a soma e a contagem
    for nota, contagem in lista_valores:
        soma_notas += nota
        contagem_total += contagem

    # Calcula a média (o valor bruto, ainda não arredondado)
    media = soma_notas / contagem_total if contagem_total > 0 else 0

    return media

# Aplicando Reduce e Formatando o Resultado Final
resultados_brutos = {
    disciplina: reduzir(lista_valores)
    for disciplina, lista_valores in agrupado.items()
}

# --- NOVO PASSO: FORMATAR COM 2 CASAS DECIMAIS ---
resultados_finais_formatados = {
    disciplina: round(media, 2)  # Usa round() para arredondar para 2 casas
    for disciplina, media in resultados_brutos.items()
}

print("\nResultado Final (Reduce - Média por Disciplina Formatada):")
print(resultados_finais_formatados)

