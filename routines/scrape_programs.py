import pandas as pd
import psycopg

from simcc.repositories import conn

programs = pd.read_csv('storage/programs.csv')


def get_institution_id(program):
    SCRIPT_SQL = """
        SELECT id FROM institution WHERE acronym = %(siglaIes)s;
        """
    params = program.to_dict()
    result = conn.select(SCRIPT_SQL, params)
    return result[0]['id'] if result else '7477a2a8-3fcb-47ac-8f66-12338ab298df'


programs['institution_id'] = programs.apply(get_institution_id, axis=1)


def get_visible_id(program):
    if program['situação'] == 'EM FUNCIONAMENTO':
        return True
    return False


programs['visible'] = programs.apply(get_visible_id, axis=1)


for _, data in programs.iterrows():
    try:
        SCRIPT_SQL = """
            INSERT INTO graduate_program
            (code, name, area, modality, TYPE, rating, institution_id, visible)
            VALUES
            (%(código)s, %(nome)s, %(nomeAreaAvaliacao)s, %(modalidade)s, %(grau)s,
            %(conceito)s, %(institution_id)s, %(visible)s);
            """
        conn.exec(SCRIPT_SQL, data.to_dict())
        print(f'Sucesso {_}')
    except psycopg.errors.UniqueViolation:
        try:
            SCRIPT_SQL_UPDATE = """
                UPDATE graduate_program
                SET name = %(nome)s, area = %(nomeAreaAvaliacao)s,
                    modality = %(modalidade)s, TYPE = %(grau)s,
                    rating = %(conceito)s, visible = %(visible)s,
                    institution_id = %(institution_id)s
                WHERE code = %(código)s;
                """
            conn.exec(SCRIPT_SQL_UPDATE, data.to_dict())
            print(f'Registro atualizado {data}')
        except Exception as e:
            print(f'Erro ao atualizar {data}: {e}')
