from simcc.repositories import conn


def get_researcher(cpf=None, name=None):
    params = {}
    cpf_filter = str()
    name_filter = str()

    if cpf:
        params['cpf'] = cpf
        cpf_filter = 'AND cpf = %(cpf)s'

    if name:
        params['name'] = name
        name_filter = 'AND nome = %(name)s'

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
