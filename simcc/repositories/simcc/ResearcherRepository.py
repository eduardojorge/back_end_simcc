from uuid import UUID

from simcc.repositories import conn
from simcc.repositories.util import pagination, webseatch_filter
from simcc.schemas.Researcher import ResearcherArticleProduction


def list_article_production(
    program_id: UUID, year: int
) -> list[ResearcherArticleProduction]:
    params = {}

    program_filter = str()
    if program_id:
        params['program_id'] = program_id
        program_filter = """
            AND gpr.graduate_program_id = %(program_id)s
            AND gpr.type_ = 'PERMANENTE'
            """

    year_filter = str()
    if year:
        params['year'] = year
        year_filter = 'AND bp.year::int >= %(year)s'

    SCRIPT_SQL = f"""
        SELECT r.name, bpa.qualis, COUNT(*) AS among, bp.year,
            COALESCE(SUM(opa.citations_count), 0) AS citations
        FROM researcher r
            LEFT JOIN bibliographic_production bp ON bp.researcher_id = r.id
            RIGHT JOIN bibliographic_production_article bpa
                ON bpa.bibliographic_production_id = bp.id
            LEFT JOIN openalex_article opa ON opa.article_id = bp.id
            LEFT JOIN graduate_program_researcher gpr ON gpr.researcher_id = r.id
        WHERE 1 = 1
            {program_filter}
            {year_filter}
        GROUP BY r.id, bpa.qualis, bp.year
        HAVING 1 = 1;
        """

    result = conn.select(SCRIPT_SQL, params)
    return result


def search_in_articles(
    terms: str,
    graduate_program_id: UUID,
    university: str,
    page: int,
    lenght: int,
):
    params = {}

    filter_pagination = str()
    if page and lenght:
        filter_pagination = pagination(page, lenght)

    filter_terms = str()
    if terms:
        filter_terms, terms = webseatch_filter('b.title', terms)
        params |= terms

    join_program = str()
    filter_program = str()
    if graduate_program_id:
        params['graduate_program_id'] = graduate_program_id
        join_program = """
            RIGHT JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = r.id
            """
        filter_program = 'AND gpr.graduate_program_id = %(graduate_program_id)s'

    filter_institution = str()
    if university:
        params['institution']
        filter_institution = 'AND i.name = %(institution)s'

    SCRIPT_SQL = f"""
        SELECT
            r.id, r.name, r.lattes_id, r.lattes_10_id, r.abstract,
            TRIM(r.orcid) AS orcid, r.graduation, r.last_update AS lattes_update,
            REPLACE(rp.great_area, '_', ' ') AS area, rp.city,
            i.image AS image_university, i.name AS university,
            b.among, rp.articles, rp.book_chapters, rp.book, rp.patent,
            rp.software, rp.brand, opr.h_index, opr.relevance_score,
            opr.works_count, opr.cited_by_count, opr.i10_index, opr.scopus,
            opr.openalex, r.classification
        FROM researcher r
            LEFT JOIN institution i ON i.id = r.institution_id
            LEFT JOIN researcher_production rp ON rp.researcher_id = r.id
            RIGHT JOIN
                (SELECT b.researcher_id, COUNT(*) AS among
                FROM bibliographic_production b
                WHERE b.type = 'ARTICLE'
                    {filter_terms}
                GROUP BY researcher_id) b ON b.researcher_id = r.id
            LEFT JOIN openalex_researcher opr ON opr.researcher_id = r.id
            {join_program}
        WHERE 1 = 1
            {filter_program}
            {filter_institution}
        ORDER BY
            among DESC
        {filter_pagination};
        """

    result = conn.select(SCRIPT_SQL, params)
    return result


def search_in_abstracts(
    terms: str,
    graduate_program_id: UUID,
    university: str,
    page: int = None,
    lenght: int = None,
):
    params = {}

    filter_pagination = str()
    if page and lenght:
        filter_pagination = pagination(page, lenght)

    filter_terms = str()
    if terms:
        filter_terms, terms = webseatch_filter('r.abstract', terms)
        params |= terms

    join_program = str()
    filter_program = str()
    if graduate_program_id:
        params['graduate_program_id'] = graduate_program_id
        join_program = """
            RIGHT JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = r.id
            """
        filter_program = 'AND gpr.graduate_program_id = %(graduate_program_id)s'

    filter_institution = str()
    if university:
        params['institution']
        filter_institution = 'AND i.name = %(institution)s'

    SCRIPT_SQL = f"""
        SELECT
            r.id, r.name, r.lattes_id, r.lattes_10_id, r.abstract, r.orcid,
            r.graduation, r.last_update AS lattes_update,
            REPLACE(rp.great_area, '_', ' ') AS area, rp.city,
            i.image AS image_university, i.name AS university,
            1 AS among, rp.articles, rp.book_chapters, rp.book, rp.patent,
            rp.software, rp.brand, opr.h_index, opr.relevance_score,
            opr.works_count, opr.cited_by_count, opr.i10_index, opr.scopus,
            opr.openalex, r.classification
        FROM researcher r
            LEFT JOIN institution i ON i.id = r.institution_id
            LEFT JOIN researcher_production rp ON rp.researcher_id = r.id
            LEFT JOIN openalex_researcher opr ON opr.researcher_id = r.id
            {join_program}
        WHERE 1 = 1
            {filter_terms}
            {filter_program}
            {filter_institution}
        ORDER BY
            among DESC
            {filter_pagination};
        """
    result = conn.select(SCRIPT_SQL, params)
    return result


def search_in_name(
    name: str,
    graduate_program_id: UUID,
    dep_id: UUID,
    page: int = None,
    lenght: int = None,
):
    params = {}

    join_departament = str()
    filter_departament = str()
    if dep_id:
        params['dep_id'] = dep_id
        join_departament = """
            LEFT JOIN ufmg.departament_researcher dr ON dr.researcher_id = r.id
            """
        filter_departament = 'AND dr.dep_id = %(dep_id)s'

    filter_name = str()
    if name:
        params['name'] = name.replace(';', ' ') + '%'
        filter_name = 'AND r.name ILIKE %(name)s'

    filter_pagination = str()
    if page and lenght:
        filter_pagination = pagination(page, lenght)

    join_program = str()
    filter_program = str()
    if graduate_program_id:
        params['graduate_program_id'] = graduate_program_id
        join_program = """
            RIGHT JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = r.id
            """
        filter_program = 'AND gpr.graduate_program_id = %(graduate_program_id)s'

    SCRIPT_SQL = f"""
        SELECT
            r.id, r.name, r.lattes_id, r.lattes_10_id, r.abstract, r.orcid,
            r.graduation, r.last_update AS lattes_update,
            REPLACE(rp.great_area, '_', ' ') AS area, rp.city,
            i.image AS image_university, i.name AS university,
            1 AS among, rp.articles, rp.book_chapters, rp.book, rp.patent,
            rp.software, rp.brand, opr.h_index, opr.relevance_score,
            opr.works_count, opr.cited_by_count, opr.i10_index, opr.scopus,
            opr.openalex, r.classification
        FROM researcher r
            LEFT JOIN institution i ON i.id = r.institution_id
            LEFT JOIN researcher_production rp ON rp.researcher_id = r.id
            LEFT JOIN openalex_researcher opr ON opr.researcher_id = r.id
            {join_program}
            {join_departament}
        WHERE 1 = 1
            {filter_program}
            {filter_name}
            {filter_departament}
        ORDER BY
            among DESC
            {filter_pagination};
        """
    result = conn.select(SCRIPT_SQL, params)
    return result


def list_graduate_programs():
    SCRIPT_SQL = """
        SELECT gpr.researcher_id AS id,
            JSONB_AGG(JSONB_BUILD_OBJECT(
                    'graduate_program_id', gp.graduate_program_id,
                    'name',gp.name
                )) AS graduate_programs
        FROM graduate_program_researcher gpr
            LEFT JOIN graduate_program gp
                ON gpr.graduate_program_id = gp.graduate_program_id
        GROUP BY gpr.researcher_id
        """
    result = conn.select(SCRIPT_SQL)
    return result


def list_research_groups():
    SCRIPT_SQL = """
        SELECT r.id AS id,
            JSONB_AGG(JSONB_BUILD_OBJECT(
                'research_group_id', rg.id,
                'name', rg.name,
                'area', rg.area,
                'census',rg.census,
                'start_of_collection', rg.start_of_collection,
                'end_of_collection', rg.end_of_collection,
                'group_identifier', rg.group_identifier,
                'year', rg.year,
                'institution_name', rg.institution_name,
                'category', rg.category
                )) AS research_groups
        FROM researcher r
        INNER JOIN research_group rg
            ON rg.second_leader_id = r.id OR rg.first_leader_id = r.id
        GROUP BY r.id
        """
    result = conn.select(SCRIPT_SQL)
    return result


def list_foment_data():
    SCRIPT_SQL = """
        SELECT s.researcher_id AS id,
            JSONB_AGG(JSONB_BUILD_OBJECT(
                'id', s.id,
                'modality_code', s.modality_code,
                'modality_name', s.modality_name,
                'call_title', s.call_title,
                'category_level_code', s.category_level_code,
                'funding_program_name', s.funding_program_name,
                'institute_name', s.institute_name,
                'aid_quantity', s.aid_quantity,
                'scholarship_quantity', s.scholarship_quantity
            )) AS subsidy
        FROM foment s
        GROUP BY s.researcher_id
        """
    result = conn.select(SCRIPT_SQL)
    return result


def list_departament_data():
    SCRIPT_SQL = """
        SELECT dpr.researcher_id AS id,
            JSONB_AGG(JSONB_BUILD_OBJECT(
                'dep_id', dp.dep_id,
                'org_cod', dp.org_cod,
                'dep_nom', dp.dep_nom,
                'dep_des', dp.dep_des,
                'dep_email', dp.dep_email,
                'dep_site', dp.dep_site,
                'dep_sigla', dp.dep_sigla,
                'dep_tel', dp.dep_tel
            )) AS departments
        FROM ufmg.departament_researcher dpr
            LEFT JOIN ufmg.departament dp ON dpr.dep_id = dp.dep_id
        GROUP BY dpr.researcher_id;
        """
    result = conn.select(SCRIPT_SQL)
    return result


def list_ufmg_data():
    SCRIPT_SQL = """
        SELECT researcher_id AS id, matric AS matric, inscufmg AS inscufmg,
            genero AS genero, situacao AS situacao, rt AS rt, clas AS clas,
            cargo AS cargo, classe AS classe, ref AS ref,titulacao AS titulacao,
            entradanaufmg AS entradanaufmg, progressao AS progressao,
            semester AS semester
        FROM ufmg.researcher
        WHERE researcher_id IS NOT NULL;
        """
    result = conn.select(SCRIPT_SQL)
    return result
