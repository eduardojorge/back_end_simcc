import httpx
from bs4 import BeautifulSoup

from routines.logger import logger_researcher_routine, logger_routine
from simcc.repositories import conn


def get_lattes_id_10(lattes_id: str) -> str:
    headers = {
        'Connection': 'keep-alive',
        'Cache-Control': 'max-age=0',
        'Origin': 'http://buscatextual.cnpq.br',
        'Upgrade-Insecure-Requests': '1',
        'DNT': '1',
        'Content-Type': 'application/x-www-form-urlencoded',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/93.0.4577.63 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,'
        '*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Referer': 'http://buscatextual.cnpq.br/buscatextual/busca.do?metodo=apresentar',
        'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
    }

    url = f'http://lattes.cnpq.br/{lattes_id}'
    with httpx.Client(follow_redirects=True) as client:
        response = client.get(url, headers=headers)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, 'html.parser')
        input_element = soup.find('input', {'type': 'hidden', 'name': 'id'})
        if input_element is not None:
            code = input_element.get('value', '')[:10]
            return code
    return None


def update_lattes_id_10(researcher):
    lattes_10_id = get_lattes_id_10(researcher['lattes_id'])

    SCRIPT_SQL = """
        UPDATE researcher
        SET lattes_10_id = %(lattes_10_id)s
        WHERE id = %(id)s;
        """

    params = {
        'id': researcher['researcher_id'],
        'lattes_10_id': lattes_10_id,
    }

    conn.exec(SCRIPT_SQL, params)


def list_researchers():
    SCRIPT_SQL = """
        SELECT r.id as researcher_id, r.lattes_id as lattes_id
        FROM researcher r
        LEFT JOIN logs.researcher_routine lrr
            ON r.id = lrr.researcher_id
            AND lrr.routine_type = 'LATTES_10'
        WHERE r.lattes_10_id IS NULL
            OR lrr.created_at < NOW() - INTERVAL '30 days';
        """
    result = conn.select(SCRIPT_SQL)
    return result


if __name__ == '__main__':
    researchers = list_researchers()
    for researcher in researchers:
        update_lattes_id_10(researcher)
        logger_researcher_routine(researcher['id'], 'LATTES_10', False)
    logger_routine('LATTES_10', False)
