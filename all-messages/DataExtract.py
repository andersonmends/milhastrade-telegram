import json
import re

# passo 2 - extrai os dados relevantes (HM, Max, Latam, Gol e Azul) do arquivo retirado do
# canal do telegran e coloca em outro arquivo json com o nome dados limpos

# Abrir o arquivo JSON
with open('channel-messages.json') as f:
    data = json.load(f)

# Lista para armazenar as mensagens modificadas
mensagens_modificadas = []

# Iterar sobre os objetos do JSON
for item in data:
    # Extrair a mensagem do objeto
    message = item['message']
    # Substituir padrões de texto indesejados
    message = re.sub(r"de .*? até ", "", str(message))
    # Extrair somente as palavras "Gol", "Azul" e "Latam" e valores em dinheiro
    message = re.findall(r"(HM|Max|Gol|Azul|Latam|[0-9]+,[0-9]+)", message)
    # Adicionar a mensagem modificada à lista
    mensagens_modificadas.append({'date': item['data'], 'message': message})

# Salvar as mensagens modificadas em um novo arquivo JSON
with open('dados-limpos.json', 'w') as f:
    json.dump(mensagens_modificadas, f)
