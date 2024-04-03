# README

## Passo I: Baixar Currículos

Execute o script `Soap_lattes.py` para baixar os currículos.

## Passo II: Configuração do Apache Hop

1. Navegue até `config\projects\Jade-Extrator-Hop\metadata\dataset\`.
2. Copie os currículos baixados para a pasta XML.
3. Copie a planilha Sucupira Qualis para a pasta CSV.

## Passo III: Atualizar Fotos

Execute o script `lattes10.py` para atualizar as fotos.

## Passo IV: Atualizar Produção, Dicionário e Frequência de Termos

Execute o script `SimccBD_population_v1.py` e atualize a tabela `bibliographic_production` com o comando SQL `UPDATE bibliographic_production SET YEAR_=YEAR::INTEGER;`.

## Passo V: Gerar CSV para Painel

Execute novamente o script `SimccBD_population_v1.py` para gerar o CSV necessário para o painel.

## Passo VI: Rodar o Servidor Flask

Execute o script `server.py` para iniciar o servidor REST Flask.
