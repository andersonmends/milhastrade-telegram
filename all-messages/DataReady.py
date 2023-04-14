import json

# passo 4 - deixa os dados pronto, ou seja, cria no json objetos unicos com o valor máximo
# do array das cias, deixanod assim mais clean

with open('dados-processados.json', 'r') as f:
    data = json.load(f)

new_data = []
for item in data:
    new_item = {"date": item["date"], "name": item.get("name")}
    for company in ["Latam", "Gol", "Azul"]:
        values = item.get(company, [])
        if values:
            max_value = max(values)
            new_item[company] = max_value
    new_data.append(new_item)

with open('dados-ready.json', 'w') as f:
    json.dump(new_data, f, indent=2)
