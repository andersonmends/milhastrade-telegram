import json

# passo 3 - processa os dados do arquivo dados-limpos de forma que agora eles 
# fiquem estruturado em json com chave e valor, com data, nome da empresa de 
# compras e o nome das cias com seus resptivos valores em um array

with open('dados-limpos.json', 'r') as f:
    data = json.load(f)

new_data = []
for item in data:
    new_item = {"date": item["date"]}
    message = item["message"]
    i = 0
    while i < len(message):
        name = message[i]
        i += 1
        if name in ["HM", "Max"]:
            new_item["name"] = name
        elif name in ["Latam", "Gol", "Azul"]:
            data_array = []
            while i < len(message) and not message[i] in ["Latam", "Gol", "Azul", "HM", "Max"]:
                data_array.append(float(message[i].replace(",", ".")))
                i += 1
            new_item[name] = data_array
        else:
            i += 1
    new_data.append(new_item)

with open('dados-processados.json', 'w') as f:
    json.dump(new_data, f, indent=2)
