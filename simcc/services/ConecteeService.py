import re
from datetime import datetime

import numpy as np
import pandas as pd
import pdfplumber

from simcc.repositories import ConecteeRepository
from simcc.schemas.Conectee import ResearcherData


def get_researcher_data(cpf: str, name: str) -> list[ResearcherData]:
    researcher = ConecteeRepository.get_researcher(cpf, name)
    if not researcher:
        return []
    return researcher


meses_pt = {
    'JAN': '01',
    'FEV': '02',
    'MAR': '03',
    'ABR': '04',
    'MAI': '05',
    'JUN': '06',
    'JUL': '07',
    'AGO': '08',
    'SET': '09',
    'OUT': '10',
    'NOV': '11',
    'DEZ': '12',
}


def corrigir_data(data_str):
    if not data_str:
        return None
    for mes_abreviado, mes_numerico in meses_pt.items():
        data_str = data_str.replace(mes_abreviado, mes_numerico)
    try:
        return pd.to_datetime(data_str, format='%d%m%Y', errors='coerce')
    except Exception as e:
        print(f'Erro ao converter data: {data_str} - {e}')
        return None


def consolidar_texto(texto):
    texto = re.sub(r'\s+', ' ', texto)
    return texto


def extrair_dados_pdf(caminho_pdf):  # noqa: PLR0914
    texto = ''

    with pdfplumber.open(caminho_pdf) as pdf:
        for pagina in pdf.pages:
            texto += pagina.extract_text()

    texto = consolidar_texto(texto)

    regex_nome_cpf = r'NOME:\s*([\w\s]+)\s+CPF\s*:\s*([\d\.\-]+)'
    match_nome_cpf = re.search(regex_nome_cpf, texto)

    nome = (
        match_nome_cpf.group(1).strip() if match_nome_cpf else 'NÃO ENCONTRADO'
    )
    cpf = match_nome_cpf.group(2).strip() if match_nome_cpf else 'NÃO ENCONTRADO'

    regex_posicionamentos = r'CLASSE:(\d+)\s+NIVEL:(\d+)\s+(\d{2}[A-Z]{3}\d{4})\s+A\s*(\d{2}[A-Z]{3}\d{4}|)'
    matches = re.findall(regex_posicionamentos, texto)

    dados = []
    for match in matches:
        classe, nivel, inicio, fim = match
        inicio_dt = corrigir_data(inicio)
        fim_dt = corrigir_data(fim) if fim else None

        nivel = int(nivel[-1])

        dados.append({
            'nome': nome,
            'cpf': cpf,
            'classe': int(classe),
            'nivel': nivel,
            'inicio': inicio_dt,
            'fim': fim_dt,
        })

    if not dados:
        print(f'⚠️ Nenhum dado encontrado no arquivo: {caminho_pdf}')
        return pd.DataFrame()

    df = pd.DataFrame(dados)

    df['inicio'] = pd.to_datetime(df['inicio'], errors='coerce')
    df['fim'] = pd.to_datetime(df['fim'], errors='coerce')

    df = df.drop_duplicates(subset=['classe', 'nivel'], keep='first')

    df['tempo_nivel'] = (df['fim'] - df['inicio']).dt.days

    intersticio_tabela = {
        '4': {1: 730, 2: 365},
        '5': {1: 730, 2: 730},
        '6': {1: 730, 2: 730, 3: 730, 4: 730},
        '7': {1: 730, 2: 730, 3: 730, 4: 730},
        '8': {1: 730},
    }

    def calcular_tempo_acumulado(row):
        if pd.isnull(row['fim']):
            tempo_nivel = (pd.Timestamp.now() - row['inicio']).days
        else:
            tempo_nivel = (row['fim'] - row['inicio']).days

        classe = str(row['classe'])
        nivel = row['nivel']

        intersticio = intersticio_tabela.get(classe, {}).get(nivel, 0)

        tempo_acumulado = tempo_nivel - intersticio
        return tempo_acumulado

    df['tempo_acumulado'] = df.apply(calcular_tempo_acumulado, axis=1)

    return df


def extract_data(path):
    try:
        researcher = extrair_dados_pdf(path)
        if not researcher.empty:
            researcher['arquivo'] = path
        else:
            return False
    except Exception:
        return False
    researcher['tempo_nivel'] = researcher['tempo_nivel'].replace(np.nan, None)
    researcher['arquivo'] = researcher['arquivo'].astype(str)
    for _, data in researcher.iterrows():
        params = data.to_dict()
        if type(params['fim']) is not datetime:
            params['fim'] = None
        ConecteeRepository.insert_researcher(params)
    return True
