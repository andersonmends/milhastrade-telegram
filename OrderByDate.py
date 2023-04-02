import json

with open('dados-ready.json', 'r') as f:
    data = json.load(f)

# ordena os objetos pela data em ordem crescente
data = sorted(data, key=lambda x: x['date'])

# cria uma nova lista sem objetos com campos nulos
new_data = []
for item in data:
    if all(item.values()):  # verifica se todos os valores do objeto são diferentes de nulo
        new_data.append(item)

with open('dados-ordenados.json', 'w') as f:
    json.dump(new_data, f, indent=2)
