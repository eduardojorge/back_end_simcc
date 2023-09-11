
Passo I: Baixar Curriculos

Soap_lattes.py

Passo II: Apache Hop

config\projects\Jade-Extrator-Hop\metadata\dataset\

Copiar os curriculos para pasta XML
Copiar a planilha sucupira qualis para a pasta csv

Passo III: Atualizar Fotos 

lattes10.py

Passo IV: Atualizar Produção, Dicionário e Frequência de Termos

SimccBD_population_v1.py
UPDATE bibliographic_production SET YEAR_=YEAR::INTEGER;

Passo V: Gerar CSV para Painel

SimccBD_population_v1.py

Passo IV: Rodar o Rest Flask 

server.py

