from simcc.repositories import conn
from simcc.schemas.Conectee import ResearcherData


def get_researcher(cpf=None, name=None):
    params = {}
    cpf_filter = str()
    name_filter = str()

    if cpf:
        cpf = cpf.replace('.', '').replace('-', '')
        params['cpf'] = cpf
        cpf_filter = "AND REPLACE(REPLACE(cpf, '.', ''), '-', '') = %(cpf)s"

    if name:
        params['name'] = name + '%'
        name_filter = 'AND nome ILIKE %(name)s'

    SCRIPT_SQL = f"""
        SELECT nome, cpf, classe, nivel, inicio, fim, tempo_nivel,
            tempo_acumulado, arquivo
        FROM ufmg.researcher_data
        WHERE 1 = 1
            {cpf_filter}
            {name_filter};
        """
    result = conn.select(SCRIPT_SQL, params)
    return result


def insert_researcher(researcher: ResearcherData):
    SCRIPT_SQL = """
        DELETE FROM ufmg.researcher_data WHERE cpf = %(cpf)s;
        """
    conn.exec(SCRIPT_SQL, researcher)

    SCRIPT_SQL = """
        INSERT INTO ufmg.researcher_data
            (nome, cpf, classe, nivel, inicio, fim, tempo_nivel, tempo_acumulado,
            arquivo)
        VALUES (%(nome)s, %(cpf)s, %(classe)s, %(nivel)s, %(inicio)s, %(fim)s,
            %(tempo_nivel)s, %(tempo_acumulado)s, %(arquivo)s);
        """
    conn.exec(SCRIPT_SQL, researcher)
