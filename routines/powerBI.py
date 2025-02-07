import os
from datetime import datetime

import nltk
import pandas as pd

from simcc.repositories import conn

PATH = 'storage/powerBI'


def dim_titulacao():
    print('Dimensão da Tabela Titulação!')


def fat_area_specialty():
    SCRIPT_SQL = """
        SELECT DISTINCT asp.id AS area_specialty_id, researcher_id,
            asp.name AS area_specialty
        FROM researcher_area_expertise r
        INNER JOIN area_specialty asp ON asp.id = r.area_specialty_id;
        """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'fat_area_specialty.csv')
    csv.to_csv(csv_path)


def fat_great_area():
    SCRIPT_SQL = """
        SELECT gae.id AS great_area_id, researcher_id,
            REPLACE(gae.name, '_', ' ') AS name
        FROM great_area_expertise gae
            LEFT JOIN researcher_area_expertise r
                ON gae.id = r.great_area_expertise_id;
        """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'fat_great_area.csv')
    csv.to_csv(csv_path)


def dim_area_specialty():
    SCRIPT_SQL = """
        SELECT id, REPLACE(name, '_', ' ') AS name FROM area_specialty;
        """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'dim_area_specialty.csv')
    csv.to_csv(csv_path)


def dim_great_area():
    SCRIPT_SQL = """
        SELECT id, REPLACE(name, '_', ' ') AS name
        FROM great_area_expertise;
        """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'dim_great_area.csv')
    csv.to_csv(csv_path)


def fat_openalex_researcher():
    SCRIPT_SQL = """
        SELECT researcher_id, h_index, relevance_score, works_count,
            cited_by_count, i10_index, scopus, orcid, openalex
        FROM openalex_researcher;
        """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'fat_openalex_researcher.csv')
    csv.to_csv(csv_path)


def researcher_area_leader():
    SCRIPT_SQL = """
        SELECT id AS researcher_id, unnest(string_to_array(extra_field, ';')) AS area_leader
        FROM researcher;
        """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'researcher_area_leader.csv')
    csv.to_csv(csv_path)


def fat_openalex_article():
    SCRIPT_SQL = """
        SELECT article_id, article_institution, issn, authors_institution,
            abstract, authors, language, citations_count, pdf, landing_page_url,
            keywords
        FROM public.openalex_article;
        """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'fat_openalex_article.csv')
    csv.to_csv(csv_path)


def dim_area_leader():
    SCRIPT_SQL = """
        SELECT DISTINCT extra_field AS area_leader
        FROM researcher;
        """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'dim_area_leader.csv')
    csv.to_csv(csv_path)


def npai():
    print('Imagem NPAI!')


def iapos():
    print('Imagem NPAI!')


def dim_city():
    SCRIPT_SQL = """
        SELECT c.id AS city_id, c.name
        FROM city c;
        """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'dim_city.csv')
    csv.to_csv(csv_path)


def ufmg_researcher():
    SCRIPT_SQL = """
        SELECT researcher_id, matric, inscufmg, nome, genero, situacao, rt, clas,
            cargo, classe, ref, titulacao, entradanaufmg, progressao, semester
        FROM ufmg.researcher;
        """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    columns = [
        'researcher_id',
        'matric',
        'inscufmg',
        'nome',
        'genero',
        'situacao',
        'rt',
        'clas',
        'cargo',
        'classe',
        'ref',
        'titulacao',
        'entradanaufmg',
        'progressao',
        'semester',
    ]
    csv = csv.reindex(columns, axis='columns', fill_value=0)
    csv_path = os.path.join(PATH, 'ufmg_researcher.csv')
    csv.to_csv(csv_path)


def DimensaoAno():
    print('Dimensão da Tabela Ano!')


def DimensaoTipoProducao():
    print('Dimensão da Tabela TipoProducao!')


def platform_image():
    print('Dimensão da Tabela Platform Image!')


def Qualis():
    print('Dimensão da Tabela Qualis!')


def dim_departament():
    SCRIPT_SQL = """
        SELECT dep_id, dep_nom, 'Escola de Engenharia' AS institution,
            '083a16f0-cccf-47d2-a676-d10b8931f66b' AS institution_id
        FROM ufmg.departament
        """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    columns = ['dep_id', 'dep_nom', 'institution', 'institution_id']
    csv = csv.reindex(columns, axis='columns', fill_value=0)
    csv_path = os.path.join(PATH, 'dim_departament.csv')
    csv.to_csv(csv_path)


def data():
    now = datetime.now()
    date_str = now.strftime('%Y-%m-%d %H:%M:%S.%f')
    csv = pd.DataFrame({'data': [date_str]})
    csv_path = os.path.join(PATH, 'data.csv')
    csv.to_csv(csv_path)
    return date_str


def cimatec_graduate_program_student():
    SCRIPT_SQL = """
        SELECT researcher_id, graduate_program_id,
            EXTRACT(YEAR FROM CURRENT_DATE) AS year
        FROM graduate_program_student
        """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'cimatec_graduate_program_student.csv')
    csv.to_csv(csv_path)


def graduate_program_student_year_unnest():
    SCRIPT_SQL = """
        SELECT graduate_program_id, researcher_id, unnest(year) AS year
        FROM graduate_program_student;
        """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'graduate_program_student_year_unnest.csv')
    csv.to_csv(csv_path)


def dim_graduate_program_acronym():
    SCRIPT_SQL = """
        SELECT graduate_program_id, acronym, name
        FROM graduate_program;
        """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'dim_graduate_program_acronym.csv')
    csv.to_csv(csv_path)


def graduate_program_researcher_year_unnest():
    SCRIPT_SQL = """
        SELECT graduate_program_id, researcher_id, unnest(year) AS year
        FROM graduate_program_researcher;
        """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'graduate_program_researcher_year_unnest.csv')
    csv.to_csv(csv_path)


def dim_departament_technician():
    SCRIPT_SQL = """
        SELECT dep_id, technician_id
        FROM ufmg.departament_technician;
        """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    print(csv)
    columns = ['dep_id', 'technician_id']
    csv.reindex(columns, axis='columns', fill_value=0)
    csv_path = os.path.join(PATH, 'dim_departament_technician.csv')
    csv.to_csv(csv_path)


def dim_departament_researcher():
    SCRIPT_SQL = """
        SELECT dep_id, researcher_id
        FROM ufmg.departament_researcher;
        """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    columns = ['dep_id', 'researcher_id']
    csv = csv.reindex(columns, axis='columns', fill_value=0)
    csv_path = os.path.join(PATH, 'dim_departament_researcher.csv')
    csv.to_csv(csv_path)


def fat_group_leaders():
    SCRIPT_SQL = """
        SELECT id, name, institution, first_leader, first_leader_id,
            second_leader, second_leader_id, area, census,
            start_of_collection, end_of_collection, group_identifier, year,
            institution_name, category
        FROM research_group
        WHERE 1 = 1
            AND first_leader_id IS NOT NULL
            OR second_leader_id IS NOT NULL
        """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    csv.rename(columns={'area': 'AREA', 'year': 'YEAR'}, inplace=True)
    csv_path = os.path.join(PATH, 'fat_group_leaders.csv')
    csv.to_csv(csv_path)


def dim_research_group():
    SCRIPT_SQL = """
        SELECT rg.id AS group_id, TRANSLATE(rg.name, '"', '') AS group_name,
            rg.area, i.id AS institution_id
        FROM public.research_group rg
        RIGHT JOIN institution i ON rg.institution ILIKE '%' || i.acronym || '%';
        """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'dim_research_group.csv')
    csv.to_csv(csv_path)


def dim_category_level_code():
    SCRIPT_SQL = """
        SELECT DISTINCT category_level_code
        FROM foment
        """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'dim_category_level_code.csv')
    csv.to_csv(csv_path)


def fat_foment():
    SCRIPT_SQL = """
        SELECT r.id AS researcher_id, r.institution_id, modality_code,
            modality_name, category_level_code, funding_program_name,
            aid_quantity, scholarship_quantity
        FROM public.foment s
            LEFT JOIN researcher r ON r.id = s.researcher_id
        WHERE s.researcher_id IS NOT NULL;
        """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'fat_foment.csv')
    csv.to_csv(csv_path)


def fat_production_tecnical_year_novo_csv_db():
    SCRIPT_SQL = """
        SELECT DISTINCT
            title, development_year::int AS year, 'PATENTE' AS TYPE,
            p.researcher_id, c.id AS city_id, r.institution_id AS institution_id,
            unaccent(LOWER(title)) AS sanitized_title
        FROM patent p, researcher r, researcher_production rp, city c
        WHERE 1 = 1
            AND r.id = p.researcher_id
            AND rp.researcher_id = p.researcher_id
            AND rp.city = c.name

        UNION

        SELECT DISTINCT
            title, s.year AS year, 'SOFTWARE' AS TYPE, s.researcher_id, c.id,
            r.institution_id, unaccent(LOWER(title)) AS sanitized_title
        FROM software s, researcher r, researcher_production rp, city c
        WHERE 1 = 1
            AND r.id = s.researcher_id
            AND rp.researcher_id = s.researcher_id
            AND rp.city = c.name

        UNION

        SELECT DISTINCT
            title, b.year AS year, 'MARCA' AS TYPE, b.researcher_id, c.id,
            r.institution_id, unaccent(LOWER(title)) AS sanitized_title
        FROM brand b, researcher r, researcher_production rp, city c
        WHERE 1 = 1
            AND r.id = b.researcher_id
            AND rp.researcher_id = b.researcher_id
            AND rp.city = c.name

        UNION

        SELECT DISTINCT
            title, b.year AS year, 'RELATÓRIO TÉCNICO' AS TYPE, b.researcher_id,
            c.id, r.institution_id, unaccent(LOWER(title)) AS sanitized_title
        FROM research_report b, researcher r, researcher_production rp, city c
        WHERE 1 = 1
            AND r.id = b.researcher_id
            AND rp.researcher_id = b.researcher_id
            AND rp.city = c.name
        """

    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'fat_production_tecnical_year_novo_csv_db.csv')
    csv.to_csv(csv_path)


def dim_institution():
    SCRIPT_SQL = """
        SELECT i.id AS institution_id, name, acronym
        FROM  institution i;
        """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'dim_institution.csv')
    csv.to_csv(csv_path)


def researcher_city():
    SCRIPT_SQL = """
        SELECT researcher_id, city
        FROM researcher_production;
        """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'researcher_city.csv')
    csv.to_csv(csv_path)


def dim_researcher(origin: str):
    SCRIPT_SQL = f"""
        SELECT r.name AS researcher, r.id AS researcher_id,
            TO_CHAR(r.last_update,'dd/mm/yyyy') AS last_update,
            r.graduation AS graduation, r.institution_id, r.docente,
            regexp_replace(r.abstract, E'[\\n\\r]+', ' - ', 'g' ) AS abstract,
            '{origin}ResearcherData/Image?researcher_id=' || r.id AS image,
            r.orcid
        FROM researcher r
        """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)

    stopwords = nltk.corpus.stopwords.words('english')
    stopwords += nltk.corpus.stopwords.words('portuguese')

    parameters = {}
    parameters['stopwords'] = stopwords

    SCRIPT_SQL = r"""
        WITH unified_data AS (
            SELECT researcher_id, translate(title,'-\.:,;''', ' ') AS title
            FROM bibliographic_production
            UNION ALL
            SELECT researcher_id, translate(title,'-\.:,;''', ' ') AS title
            FROM patent
            UNION ALL
            SELECT researcher_id, translate(title,'-\.:,;''', ' ') AS title
            FROM brand
            UNION ALL
            SELECT researcher_id, translate(title,'-\.:,;''', ' ') AS title
            FROM event_organization
            UNION ALL
            SELECT researcher_id, translate(title,'-\.:,;''', ' ') AS title
            FROM software
        ),
        word_split AS (
            SELECT researcher_id, unnest(string_to_array(lower(regexp_replace(title, '[^a-zA-Z0-9\s]', '', 'g')), ' ')) AS word
            FROM unified_data
        ),
        word_count AS (
            SELECT researcher_id, word, COUNT(*) AS frequency
            FROM word_split
            WHERE word <> ''
            GROUP BY researcher_id, word
        ),
        ranked_words AS (
            SELECT researcher_id, word, frequency, RANK() OVER (PARTITION BY researcher_id ORDER BY frequency DESC) AS rank
            FROM word_count
        )
        SELECT researcher_id, STRING_AGG(word, ' | ') AS list_of_words
        FROM ranked_words
        WHERE 1 = 1
            AND rank <= 20
            AND CHAR_LENGTH(word) > 3
            AND TRIM(word) <> ALL(%(stopwords)s)
        GROUP BY researcher_id
        ORDER BY researcher_id;
        """  # noqa: E501

    result = conn.select(SCRIPT_SQL, parameters)
    list_words = pd.DataFrame(result)

    csv = csv.merge(list_words, on='researcher_id', how='left')
    csv_path = os.path.join(PATH, 'dim_researcher.csv')
    csv.to_csv(csv_path)


def fat_simcc_bibliographic_production():
    SCRIPT_SQL = """
        SELECT DISTINCT
            title, b.type as tipo, b.researcher_id, year, i.id AS institution_id,
            bar.qualis, bar.periodical_magazine_name, bar.jcr, bar.jcr_link,
            c.id AS city_id, b.id AS bibliographic_production_id,
            unaccent(LOWER(title)) AS sanitized_title
        FROM bibliographic_production b
        LEFT JOIN bibliographic_production_article bar
            ON b.id = bar.bibliographic_production_id, researcher r
        LEFT JOIN  institution i
            ON r.institution_id = i.id
        LEFT JOIN city c
            ON r.city_id = c.id
        WHERE 1 = 1
            AND b.researcher_id IS NOT NULL
            AND r.id =  b.researcher_id
        ORDER BY
            YEAR desc;
        """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'fat_simcc_bibliographic_production.csv')
    csv.to_csv(csv_path)


def production_tecnical_year():
    SCRIPT_SQL = """
        SELECT researcher_id, title, development_year::int AS YEAR,
            'PATENT' as type
        FROM patent
        UNION
        SELECT researcher_id,title,YEAR,'SOFTWARE'
        from software
        UNION
        SELECT researcher_id,title,YEAR,'BRAND' 
        from brand
        UNION
        SELECT researcher_id,title,YEAR,'REPORT' 
        from research_report
        """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'production_tecnical_year.csv')
    csv.to_csv(csv_path)


def researcher():
    SCRIPT_SQL = """
        SELECT r.name AS researcher, r.id AS researcher_id,
            TO_CHAR(r.last_update,'dd/mm/yyyy') AS last_update,
            r.graduation AS graduation, r.lattes_id, extra_field AS area_leader
        FROM  researcher r;
        """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'researcher.csv')
    csv.to_csv(csv_path)


def article_qualis_year_institution():
    SCRIPT_SQL = """
        SELECT DISTINCT
            title, bar.qualis, bar.jcr, year, i.acronym as institution,
            rd.city as city, bar.jcr_link
        FROM bibliographic_production b, bibliographic_production_article bar,
            periodical_magazine pm, researcher r, institution i,
            researcher_address rd
        WHERE 1 = 1
            AND pm.id = bar.periodical_magazine_id
            AND r.id =  b.researcher_id
            AND r.institution_id = i.id
            AND b.id = bar.bibliographic_production_id
            AND rd.researcher_id = r.id
        GROUP BY title, bar.qualis, bar.jcr, year, i.acronym, rd.city,
            bar.jcr_link
        ORDER BY bar.qualis desc
        """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'article_qualis_year_institution.csv')
    csv.to_csv(csv_path)


def production_researcher():
    SCRIPT_SQL = """
        SELECT r.name AS researcher, r.id AS researcher_id,
            rp.articles AS articles, rp.book_chapters AS book_chapters,
            rp.book AS book, rp.work_in_event AS work_in_event,
            rp.great_area AS great_area, rp.area_specialty AS area_specialty,
            r.graduation as graduation
        FROM researcher_production rp, researcher r
        WHERE r.id = rp.researcher_id
        """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'production_researcher.csv')
    csv.to_csv(csv_path)


def article_qualis_year():
    SCRIPT_SQL = """
        SELECT DISTINCT
            title, bar.qualis, year, r.id AS researcher_id,
            r.name AS researcher, 'institution', 'city',
            pm.name AS name_magazine, pm.issn AS issn, bar.jcr as jcr,
            bar.jcr_link as jcr_link, b.type as type
        FROM bibliographic_production b
            LEFT JOIN (bibliographic_production_article bar
                LEFT JOIN periodical_magazine pm
                    ON pm.id = bar.periodical_magazine_id)
                ON b.id = bar.bibliographic_production_id,
            researcher r
        WHERE r.id =  b.researcher_id;
        """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'article_qualis_year.csv')
    csv.to_csv(csv_path)


def production_year_distinct():
    SCRIPT_SQL = """
        SELECT DISTINCT
            year, title, type AS tipo, i.acronym AS institution
        FROM bibliographic_production AS b, institution i, researcher r
        WHERE 1 = 1
            AND r.id = b.researcher_id
            AND r.institution_id = i.id
        GROUP BY year, title, tipo, i.acronym
        """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'production_year_distinct.csv')
    csv.to_csv(csv_path)


def production_year():
    SCRIPT_SQL = """
        SELECT DISTINCT
            title, b.type AS tipo, b.researcher_id, year, 'institution'
        FROM bibliographic_production AS b, researcher r
        WHERE 1 = 1
            AND b.researcher_id IS NOT NULL
            AND r.id = b.researcher_id
        GROUP BY title, tipo, b.researcher_id, year
        ORDER BY year, tipo DESC
        """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'production_year.csv')
    csv.to_csv(csv_path)


def production_coauthors_csv_db():
    SCRIPT_SQL = """
        SELECT COUNT(*) AS qtd, a.doi, a.title, ba.qualis, a.year,
            gp.graduate_program_id, gp.year AS year_pos, a.type
        FROM bibliographic_production a
            LEFT JOIN bibliographic_production_article ba
                ON a.id = ba.bibliographic_production_id,
            bibliographic_production b, graduate_program_researcher gp
        WHERE 1 = 1
            AND (a.doi = b.doi OR a.title = b.title)
            AND a.researcher_id = gp.researcher_id
            AND b.researcher_id = gp.researcher_id
        GROUP BY a.doi, a.title, ba.qualis, a.year, gp.graduate_program_id,
            gp.year,a."type"
        HAVING COUNT(*) > 1
        """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'production_coauthors_csv_db.csv')
    csv.to_csv(csv_path)


def fat_researcher_ind_prod():
    SCRIPT_SQL = """
        SELECT researcher_id, YEAR, ind_prod_article, ind_prod_book,
            ind_prod_book_chapter, ind_prod_granted_patent,
            ind_prod_not_granted_patent, ind_prod_software, ind_prod_report,
            ind_prod_guidance
        FROM researcher_ind_prod;
        """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'fat_researcher_ind_prod.csv')
    csv.to_csv(
        csv_path,
        decimal=',',
        sep=';',
        index=False,
        encoding='UTF8',
        float_format=None,
    )


def graduate_program_ind_prod():
    SCRIPT_SQL = """
        SELECT graduate_program_id, YEAR, ind_prod_article, ind_prod_book,
            ind_prod_book_chapter, ind_prod_granted_patent,
            ind_prod_not_granted_patent, ind_prod_software, ind_prod_report,
            ind_prod_guidance
        FROM graduate_program_ind_prod;
        """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'graduate_program_ind_prod.csv')
    csv.to_csv(
        csv_path,
        decimal=',',
        sep=';',
        index=False,
        encoding='UTF8',
        float_format=None,
    )


def researcher_production_novo_csv_db():
    SCRIPT_SQL = """
        SELECT title, qualis, year, r.id as researcher_id, r.name as researcher,
            bar.periodical_magazine_name as name_magazine, issn AS issn,
            jcr as jcr, jcr_link, b.type as type
        FROM bibliographic_production b
            LEFT JOIN  bibliographic_production_article bar
                ON b.id = bar.bibliographic_production_id,
                researcher r
        WHERE r.id = b.researcher_id;
        """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'researcher_production_novo_csv_db.csv')
    csv.to_csv(csv_path)


def article_distinct_novo_csv_db():
    SCRIPT_SQL = """
        SELECT DISTINCT
            title, bar.qualis, bar.jcr, b.year as year,
            gp.graduate_program_id as graduate_program_id, b.year as year_pos,
            periodical_magazine.name AS type
        FROM bibliographic_production b
            LEFT JOIN bibliographic_production_article bar
                ON b.id = bar.bibliographic_production_id
            LEFT JOIN periodical_magazine
                ON periodical_magazine.id = bar.periodical_magazine_id,
            researcher r, graduate_program_researcher gpr, graduate_program gp
        WHERE gpr.graduate_program_id = gp.graduate_program_id
            AND gpr.researcher_id = r.id
            AND r.id = b.researcher_id
            AND b.year::INT = ANY(gpr.year)
            AND b.type = 'ARTICLE'
            --- AND gpr.type_ = 'PERMANENTE'
        ORDER BY qualis desc
    """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'article_distinct_novo_csv_db.csv')
    csv.to_csv(csv_path)


def production_distinct_novo_csv_db():
    SCRIPT_SQL = """
        SELECT DISTINCT
            title, qualis,jcr, b.year AS year,
            gp.graduate_program_id AS graduate_program_id, b.year as year_pos,
            b.type AS type
        FROM bibliographic_production b
            LEFT JOIN  bibliographic_production_article bar
                ON b.id = bar.bibliographic_production_id,
            researcher r, graduate_program_researcher gpr, graduate_program gp
        WHERE gpr.graduate_program_id = gp.graduate_program_id
            AND gpr.researcher_id = r.id
            AND r.id = b.researcher_id
            AND b.year::INT = ANY(gpr.year)
        order by qualis desc
        """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'production_distinct_novo_csv_db.csv')
    csv.to_csv(csv_path)


def cimatec_graduate_program_researcher():
    SCRIPT_SQL = """
    SELECT researcher_id, graduate_program_id,
        EXTRACT(YEAR FROM CURRENT_DATE) AS year, type_
    FROM graduate_program_researcher;
    """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'cimatec_graduate_program_researcher.csv')
    csv.to_csv(csv_path)


def cimatec_graduate_program():
    SCRIPT_SQL = """
        SELECT gp.graduate_program_id, gp.code, gp.name, gp.area, gp.modality,
            gp.type, gp.rating, i.id AS institution_id, i.name AS institution,
            gp.city
        FROM graduate_program gp
            LEFT JOIN institution i
                ON i.id = gp.institution_id
        """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'cimatec_graduate_program.csv')
    csv.to_csv(csv_path)


def dim_research_project():
    SCRIPT_SQL = """
        SELECT id, researcher_id, start_year, end_year, agency_code, agency_name,
            project_name, status, nature, number_undergraduates, description,
            number_specialists, number_academic_masters, number_phd
        FROM research_project;
    """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)

    csv['start_year'] = (
        pd.to_numeric(csv['start_year'], errors='coerce').fillna(0).astype(int)
    )
    csv['end_year'] = (
        pd.to_numeric(csv['end_year'], errors='coerce').fillna(0).astype(int)
    )

    csv_path = os.path.join(PATH, 'dim_research_project.csv')
    csv.to_csv(csv_path, encoding='ISO-8859-1')


def fat_research_project_foment():
    SCRIPT_SQL = """
        SELECT project_id, agency_name, agency_code, nature
        FROM research_project_foment;
        """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'fat_research_project_foment.csv')
    csv.to_csv(csv_path)


def dim_terms():
    SCRIPT_SQL = r"""
        WITH unified_data AS (
            SELECT id, 'BIBLIOGRAPHIC_PRODUCTION' AS type_, translate(title,'-\.:,;''', ' ') AS title
            FROM bibliographic_production
            UNION ALL
            SELECT id, 'PATENT', translate(title,'-\.:,;''', ' ') AS title
            FROM patent
            UNION ALL
            SELECT id, 'BRAND', translate(title,'-\.:,;''', ' ') AS title
            FROM brand
            UNION ALL
            SELECT id, 'EVENT_ORGANIZATION',translate(title,'-\.:,;''', ' ') AS title
            FROM event_organization
            UNION ALL
            SELECT id, 'SOFTWARE', translate(title,'-\.:,;''', ' ') AS title
            FROM software
        ),
        word_split AS (
            SELECT id, type_, unnest(string_to_array(lower(regexp_replace(title, '[^a-zA-Z0-9\s]', '', 'g')), ' ')) AS word
            FROM unified_data
        ),
        word_count AS (
            SELECT id, type_, word, COUNT(*) AS frequency
            FROM word_split
            WHERE word <> ''
            GROUP BY id, type_, word
        ),
        ranked_words AS (
            SELECT id, type_, word, frequency, RANK() OVER (PARTITION BY id ORDER BY frequency DESC) AS rank
            FROM word_count
        )
        SELECT id, type_, UNNEST(ARRAY_AGG(ranked_words.word)) AS term
        FROM ranked_words
        WHERE 1 = 1
            AND rank <= 20
            AND CHAR_LENGTH(word) > 3
            AND TRIM(word) NOT IN ('da', 'de')
        GROUP BY id, type_
        ORDER BY id;
        """
    result = conn.select(SCRIPT_SQL)
    csv = pd.DataFrame(result)
    csv_path = os.path.join(PATH, 'dim_terms.csv')
    csv.to_csv(csv_path)


if __name__ == '__main__':
    for directory in [PATH]:
        if not os.path.exists(directory):
            os.makedirs(directory)
