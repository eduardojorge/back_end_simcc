import logging
import os
from datetime import datetime

import numpy as np
import pandas as pd

from simcc.repositories import conn

LOG_PATH = 'logs'

if __name__ == '__main__':
    log_format = '%(levelname)s | %(asctime)s - %(message)s'

    logging.basicConfig(
        filename=os.path.join(LOG_PATH, 'conectee_researcher.log'),
        filemode='w',
        format=log_format,
        level=logging.DEBUG,
    )

    logger = logging.getLogger(__name__)

    for directory in [LOG_PATH]:
        if not os.path.exists(directory):
            os.makedirs(directory)

    researchers = pd.read_excel('storage/conectee/researcher_data.xlsx')
    researchers['tempo_nivel'] = researchers['tempo_nivel'].replace(np.nan, None)

    SCRIPT_SQL = """
        INSERT INTO ufmg.researcher_data
            (nome, cpf, classe, nivel, inicio, fim, tempo_nivel, tempo_acumulado,
            arquivo)
        VALUES (%(nome)s, %(cpf)s, %(classe)s, %(nivel)s, %(inicio)s, %(fim)s,
            %(tempo_nivel)s, %(tempo_acumulado)s, %(arquivo)s);
        """

    for _, researcher in researchers.iterrows():
        params = researcher.to_dict()

        if type(params['fim']) is not datetime:
            params['fim'] = None

        conn.exec(SCRIPT_SQL, params)
        print('Sucesso com o pesquisdor: ', researcher['nome'])
