import csv

import requests

base_url = 'https://apigw-proxy.capes.gov.br/data/online/ppg'

params = {'query': 'uf:(BA)', 'page': 0, 'size': 50}

all_data = []

while True:
    response = requests.get(base_url, params=params)

    data = response.json()

    if data['content']:
        all_data.extend(data['content'])
        params['page'] += 1
    else:
        break

fields = [
    'grau',
    'nome',
    'codigo',
    'conceito',
    'situacao',
    'siglaIes',
    'modalidade',
    'idPrograma',
    'idModalidade',
    'programaEmRede',
    'idAreaAvaliacao',
    'nomeAreaAvaliacao',
    'idAreaConhecimento',
    'nomeAreaConhecimento',
    'idModalidadeEnsino',
    'nomeModalidadeEnsino',
    'idGrandeAreaConhecimento',
    'nomeGrandeAreaConhecimento',
]

with open('graduate_program.csv', 'w', newline='', encoding='utf-8') as file:
    writer = csv.DictWriter(file, fieldnames=fields)
    writer.writeheader()
    writer.writerows(all_data)
