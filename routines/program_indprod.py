import pandas as pd
from unidecode import unidecode

from simcc.repositories import conn


def article_indprod():
    SCRIPT_SQL = """
        SELECT year, qualis, COUNT(*) AS count_article, researcher_id
        FROM bibliographic_production bp
        RIGHT JOIN bibliographic_production_article bpa
            ON bp.id = bpa.bibliographic_production_id
        GROUP BY year, qualis, researcher_id;
        """
    result = conn.select(SCRIPT_SQL)
    articles = pd.DataFrame(result)

    articles['article_prod'] = (
        articles['qualis'].map(barema) * articles['count_article']
    )
    articles = articles.groupby(['year', 'researcher_id']).sum().reset_index()

    columns = ['year', 'researcher_id', 'article_prod']
    articles = articles[columns]
    articles['year'] = articles['year'].astype(int)
    return articles.to_dict(orient='records')


def book_indprod():
    SCRIPT_SQL = """
        SELECT year, COUNT(*) AS count_book, researcher_id
        FROM bibliographic_production bp
        WHERE type = 'BOOK'
        GROUP BY year, researcher_id;
        """
    result = conn.select(SCRIPT_SQL)
    books = pd.DataFrame(result)

    books['book_prod'] = books['count_book'] * barema.get('BOOK', 0)

    columns = ['year', 'researcher_id', 'book_prod']
    books = books[columns]
    books['year'] = books['year'].astype(int)
    return books.to_dict(orient='records')


def book_chapter_indprod():
    SCRIPT_SQL = """
        SELECT year, COUNT(*) AS count_book_chapter, researcher_id
        FROM bibliographic_production bp
        WHERE type = 'BOOK_CHAPTER'
        GROUP BY year, researcher_id;
        """
    result = conn.select(SCRIPT_SQL)
    book_chapter = pd.DataFrame(result)

    book_chapter['book_chapter_prod'] = book_chapter[
        'count_book_chapter'
    ] * barema.get('BOOK_CHAPTER', 0)

    book_chapter['year'] = book_chapter['year'].astype(int)
    return book_chapter.to_dict(orient='records')


def patent_indprod():
    SCRIPT_SQL = """
        SELECT development_year AS year, 'PATENT_GRANTED' AS granted,
            researcher_id, COUNT(*) as count_patent
        FROM patent p
        WHERE grant_date IS NOT NULL
        GROUP BY development_year, researcher_id

        UNION

        SELECT development_year AS year, 'PATENT_NOT_GRANTED' AS granted,
            researcher_id, COUNT(*) as count_patent
        FROM patent p
        WHERE grant_date IS NULL
        GROUP BY development_year, researcher_id
        """
    result = conn.select(SCRIPT_SQL)
    patent = pd.DataFrame(result)

    patent['patent_prod'] = (
        patent['granted'].map(barema) * patent['count_patent']
    )
    columns = ['patent_prod', 'year', 'researcher_id']
    patent = patent[columns]
    patent = patent.groupby(['researcher_id', 'year']).sum().reset_index()
    patent['year'] = patent['year'].astype(int)
    return patent.to_dict(orient='records')


def software_indprod():
    SCRIPT_SQL = """
        SELECT year, COUNT(*) AS software_count, researcher_id
        FROM software
        GROUP BY year, researcher_id;
        """
    result = conn.select(SCRIPT_SQL)
    software = pd.DataFrame(result)
    software['software_prod'] = software['software_count'] * barema.get(
        'SOFTWARE', 0
    )
    columns = ['software_prod', 'year', 'researcher_id']
    software = software[columns]
    software['year'] = software['year'].astype(int)
    return software.to_dict(orient='records')


def report_indprod():
    SCRIPT_SQL = """
        SELECT year, COUNT(*) AS report_count, researcher_id
        FROM research_report
        GROUP BY year, researcher_id;
        """
    result = conn.select(SCRIPT_SQL)
    report = pd.DataFrame(result)

    report['report_prod'] = report['report_count'] * barema.get('REPORT', 0)
    columns = ['year', 'researcher_id', 'report_prod']
    report = report[columns]
    report['year'] = report['year'].astype(int)
    return report.to_dict(orient='records')


def guidance_indprod():
    SCRIPT_SQL = """
        SELECT year, nature || ' ' || status AS nature_status,
            COUNT(*) AS guidance_count, researcher_id
        FROM guidance
        GROUP BY year, nature_status, researcher_id;
        """
    result = conn.select(SCRIPT_SQL)
    guidance = pd.DataFrame(result)

    def normalise(nature_status):
        return unidecode(nature_status).upper()

    guidance['nature_status'] = guidance['nature_status'].apply(normalise)
    guidance['guidance_prod'] = (
        guidance['nature_status'].map(barema) * guidance['guidance_count']
    )
    guidance = guidance.groupby(['year', 'researcher_id']).sum().reset_index()
    columns = ['year', 'researcher_id', 'guidance_prod']
    guidance = guidance[columns]
    guidance['year'] = guidance['year'].astype(int)
    return guidance.to_dict(orient='records')


def list_researchers():
    SCRIPT_SQL = """
        SELECT graduate_program_id, researcher_id
        FROM graduate_program_researcher;
        """
    result = conn.select(SCRIPT_SQL)
    return result


if __name__ == '__main__':
    barema = {
        'A1': 1,
        'A2': 0.875,
        'A3': 0.75,
        'A4': 0.625,
        'B1': 0.5,
        'B2': 0.375,
        'B3': 0.25,
        'B4': 0.125,
        'C': 0,
        'SQ': 0,
        'BOOK': 1,
        'BOOK_CHAPTER': 0.25,
        'SOFTWARE': 0.25,
        'PATENT_GRANTED': 1,
        'PATENT_NOT_GRANTED': 0.25,
        'REPORT': 0.25,
        'TESE DE DOUTORADO CONCLUIDA': 0.5,
        'TESE DE DOUTORADO EM ANDAMENTO': 0.25,
        'DISSERTACAO DE MESTRADO CONCLUIDA': 0.25,
        'DISSERTACAO DE MESTRADO EM ANDAMENTO': 0.125,
        'INICIACAO CIENTIFICA CONCLUIDA': 0.125,
        'INICIACAO CIENTIFICA EM ANDAMENTO': 0.1,
    }

    YEAR = range(2008, 2025)
    history = pd.DataFrame(YEAR, columns=['year'])

    programs = list_researchers()
    programs = pd.DataFrame(programs)

    programs = programs.merge(history, how='cross')

    on = ['researcher_id', 'year']

    articles = article_indprod()
    articles = pd.DataFrame(articles)
    programs = programs.merge(articles, on=on, how='left')

    books = book_indprod()
    books = pd.DataFrame(books)
    programs = programs.merge(books, on=on, how='left')

    book_chapter = book_chapter_indprod()
    book_chapter = pd.DataFrame(book_chapter)
    programs = programs.merge(book_chapter, on=on, how='left')

    software = software_indprod()
    software = pd.DataFrame(software)
    programs = programs.merge(software, on=on, how='left')

    patent = patent_indprod()
    patent = pd.DataFrame(patent)
    programs = programs.merge(patent, on=on, how='left')

    report = report_indprod()
    report = pd.DataFrame(report)
    programs = programs.merge(report, on=on, how='left')

    guidance = guidance_indprod()
    guidance = pd.DataFrame(guidance)
    programs = programs.merge(guidance, on=on, how='left')

    programs = programs.drop(columns=['researcher_id'])

    programs = (
        programs.groupby(['graduate_program_id', 'year'])
        .sum()
        .reset_index()
        .sort_values(by=['graduate_program_id', 'year'])
    )

    SCRIPT_SQL = """
        DELETE FROM graduate_program_ind_prod;
        """
    conn.exec(SCRIPT_SQL)

    SCRIPT_SQL = """
        INSERT INTO graduate_program_ind_prod (graduate_program_id, year,
            ind_prod_article, ind_prod_book, ind_prod_book_chapter,
            ind_prod_software, ind_prod_granted_patent,
            ind_prod_not_granted_patent, ind_prod_report, ind_prod_guidance)
        VALUES (%(graduate_program_id)s, %(year)s, %(article_prod)s,
            %(book_prod)s, %(book_chapter_prod)s, %(software_prod)s,
            %(patent_prod)s, %(patent_prod)s,
            %(report_prod)s, %(guidance_prod)s);
        """
    for _, program in programs.iterrows():
        print(f'Inserting row for group: {_}')
        conn.exec(SCRIPT_SQL, program.fillna(0).to_dict())
