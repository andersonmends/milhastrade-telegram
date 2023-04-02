import json

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
