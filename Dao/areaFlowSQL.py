import Dao.sgbdSQL as sgbdSQL
import unidecode
import pandas as pd
import Dao.util as util


def lists_great_area_expertise_researcher_db(researcher_id):
    reg = sgbdSQL.consultar_db(
        f"""
        SELECT distinct gae.name as area
        FROM great_area_expertise gae, researcher_area_expertise r
        WHERE
        gae.id = r.great_area_expertise_id
        AND r.researcher_id='{researcher_id}'"""
    )

    df_bd = pd.DataFrame(reg, columns=["area"])
    area = ""
    for Index, Data in df_bd.iterrows():
        area = Data.area + " ; " + area

    x = len(area)
    area = area[0 : x - 2]
    return area


def lists_area_speciality_researcher_db(researcher_id):
    reg = sgbdSQL.consultar_db(
        "SELECT distinct asp.name as specialty, ae.name as area_expertise  "
        + "  from area_expertise ae,area_specialty asp, researcher_area_expertise r "
        "  WHERE "
        " asp.id =r.area_specialty_id "
        " AND r.area_expertise_id = ae.id"
        "  AND r.researcher_id='%s'" % researcher_id
    )

    df_bd = pd.DataFrame(reg, columns=["specialty", "area_expertise"])
    area = ""
    for i, infos in df_bd.iterrows():
        area = infos.specialty + " | " + infos.area_expertise + " ; " + area

    x = len(area)
    area = area[0 : x - 2]
    return area


def lists_great_area_expertise_term_initials_db(initials):
    initials = unidecode.unidecode(initials)

    reg = sgbdSQL.consultar_db(
        "SELECT id, name as nome from great_area_expertise WHERE   LOWER(unaccent(name)) LIKE '"
        + initials
        + "%' order by nome"
    )

    df_bd = pd.DataFrame(reg, columns=["id", "nome"])

    print(df_bd)
    return df_bd


def lists_area_speciality_term_initials_db(initials, area, graduate_program_id):
    area = area.replace(" ", "_")
    filter = util.filterSQL(area.lower(), ",", "or", "gea.name")

    initials = unidecode.unidecode(initials)

    initials = unidecode.unidecode(initials)
    t = initials.split(";")
    filter_ = ""
    for word in t:
        filter_ = (
            "(        translate(unaccent(LOWER(asp.name)),':;','') ::tsvector@@ unaccent(LOWER('"
            + word.lower()
            + "'))::tsquery)=TRUE  and"
            + filter_
        )

    x = len(filter_)
    filter_ = filter_[0 : x - 3]
    filter_ = "(" + filter_ + ")"
    fetch = ""
    if filter_ == "":
        fetch = "  fetch FIRST 30 rows only"
    filtergraduate_program = ""
    if graduate_program_id != "":
        filtergraduate_program = "AND gpr.graduate_program_id=" + graduate_program_id

    reg = sgbdSQL.consultar_db(
        " SELECT  distinct ae.name as area_expertise, asp.name as area_specialty "
        + " FROM area_expertise ae, area_specialty asp, great_area_expertise gea, "
        " researcher_area_expertise rae  LEFT JOIN graduate_program_researcher gpr ON  rae.researcher_id =gpr.researcher_id  "
        + " WHERE "
        + " gea.id = rae.great_area_expertise_id "
        + " AND ae.id = rae.area_expertise_id "
        + " AND asp.id = rae.area_specialty_id "
        " AND ( LOWER(unaccent(asp.name)) LIKE '"
        + initials.lower()
        + "%' )"
        +
        #  "  or " +filter_+")"+
        " %s" % filter
        + " %s " % filtergraduate_program
        +
        # " AND  LOWER(gea.name)='%s'" % area.lower()+
        " ORDER BY  ae.name,asp.name %s" % fetch
    )

    df_bd = pd.DataFrame(reg, columns=["area_expertise", "area_specialty"])

    print(df_bd)
    return df_bd


def lista_researcher_area_expertise_db(term, institution):
    term_filter = util.web_search_filter(term, "gae.name")

    institution_filter = str()
    if institution:
        institution_filter = util.filterSQL(institution, ";", "or", "i.name")

    script_sql = f"""
        SELECT
            r.id AS id,
            r.name AS researcher_name,
            r.lattes_id AS lattes,
            0 as among,
            rp.articles AS articles,
            rp.book_chapters AS book_chapters,
            rp.book AS book,
            rp.patent AS patent,
            rp.software AS software,
            rp.brand AS brand,
            i.name AS university,
            r.abstract AS abstract,
            UPPER(REPLACE(LOWER(TRIM(rp.great_area)), '_', ' ')) AS area,
            rp.city AS city,
            r.orcid AS orcid,
            i.image AS image_university,
            r.graduation AS graduation,
            to_char(r.last_update,'dd/mm/yyyy') AS lattes_update
        FROM
            researcher r
            LEFT JOIN city c ON c.id = r.city_id
            LEFT JOIN institution i ON r.institution_id = i.id
            LEFT JOIN researcher_production rp ON r.id = rp.researcher_id
            RIGHT JOIN researcher_area_expertise re ON re.researcher_id = r.id
            RIGHT JOIN great_area_expertise gae ON gae.id = re.great_area_expertise_id
        WHERE
            {term_filter}
            {institution_filter}
        GROUP BY
            r.id, r.name, r.lattes_id, rp.articles, rp.book_chapters,
            rp.book, rp.software, rp.brand, i.name, r.abstract,
            rp.great_area, rp.city, r.orcid, i.image, r.graduation,
            r.last_update, rp.patent;
            """
    registry = sgbdSQL.consultar_db(script_sql)

    data_frame = pd.DataFrame(
        registry,
        columns=[
            "id",
            "name",
            "lattes_id",
            "among",
            "articles",
            "book_chapters",
            "book",
            "patent",
            "software",
            "brand",
            "university",
            "abstract",
            "area",
            "city",
            "orcid",
            "image_university",
            "graduation",
            "lattes_update",
        ],
    )

    data_frame = data_frame.merge(researcher_graduate_program_db(), on="id", how="left")
    data_frame = data_frame.merge(researcher_research_group_db(), on="id", how="left")
    data_frame = data_frame.merge(researcher_openAlex_db(), on="id", how="left")
    data_frame = data_frame.merge(researcher_foment_db(), on="id", how="left")
    data_frame = data_frame.merge(researcher_departament(), on="id", how="left")

    return data_frame.fillna("").to_dict(orient="records")


def lista_production_article_area_expertise_db(
    great_area_expertise, area_specialty, year, qualis, graduate_program_id
):
    great_area_expertise = great_area_expertise.replace(" ", "_")
    filter = util.filterSQL(great_area_expertise, ";", "or", "rp.great_area_expertise")

    area_specialty = area_specialty.replace("&", " ")
    area_specialty = unidecode.unidecode(area_specialty.lower())

    filter_specialty = util.filterSQLLike(
        area_specialty, ";", "or", "rp.area_specialty"
    )

    filterQualis = util.filterSQL(qualis, ";", "or", "qualis")

    filtergraduate_program = ""
    if graduate_program_id != "":
        filtergraduate_program = "AND gpr.graduate_program_id=" + graduate_program_id

    script_sql = f"""
        SELECT DISTINCT
            bp.id AS id,
            op.article_institution as article_institution,
            array_cat(string_to_array(op.issn, ','), string_to_array(bar.issn, ',')) AS issn,
            op.authors_institution as authors_institution,
            op.abstract as abstract,
            op.authors as authors,
            op.language as language,
            op.citations_count as citations_count,
            op.pdf as pdf,
            op.landing_page_url as landing_page_url,
            op.keywords as keywords,
            bp.title AS title,
            r.name AS researcher,
            r.lattes_id AS lattes_id,
            lattes_10_id,
            UPPER(REPLACE(LOWER(TRIM(rp.great_area)), '_', ' ')) AS area,
            bp.year AS year,
            periodical_magazine_name AS magazine,
            doi,
            qualis,
            jcr,
            jcr_link
        FROM
            researcher r
            LEFT JOIN graduate_program_researcher gpr ON r.id = gpr.researcher_id
            LEFT JOIN researcher_production rp ON r.id = rp.researcher_id
            LEFT JOIN bibliographic_production bp ON bp.researcher_id = r.id
            LEFT JOIN bibliographic_production_article bar ON bp.id = bar.bibliographic_production_id
            LEFT JOIN openalex_article op ON op.article_id = b.id
        WHERE
            bp.year >= {year}
            {filter}
            {filterQualis}
            {filter_specialty}
            {filtergraduate_program}
        GROUP BY
            bp.id,
            op.article_institution,
            op.issn,
            op.authors_institution,
            op.abstract,
            op.authors,
            op.language,
            op.citations_count,
            op.pdf,
            op.landing_page_url,
            op.keywords,
            bp.title,
            r.name,
            r.lattes_id,
            lattes_10_id,
            rp.great_area,
            bp.year,
            periodical_magazine_name,
            doi,
            qualis,
            jcr,
            jcr_link;
        """

    reg = sgbdSQL.consultar_db(script_sql)

    df_bd = pd.DataFrame(
        reg,
        columns=[
            "id",
            "article_institution",
            "issn",
            "authors_institution",
            "abstract",
            "authors",
            "language",
            "citations_count",
            "pdf",
            "landing_page_url",
            "keywords",
            "title",
            "researcher",
            "lattes_id",
            "lattes_10_id",
            "area",
            "year",
            "magazine",
            "doi",
            "qualis",
            "jcr",
            "jcr_link",
        ],
    )
    return df_bd


def lista_institution_area_expertise_db(great_area, area_specialty, institution):
    # reg = consultar_db('SELECT  name,id FROM researcher WHERE '+
    #                 ' (name::tsvector@@ \''+termX+'\'::tsquery)=true')

    great_area = great_area.replace(" ", "_")
    filter_great_area = util.filterSQL(great_area, ";", "or", "rp.great_area")
    # filter_area_specialty = util.filterSQL(area_specialty,";","or","asp.name")
    print(area_specialty)
    area_specialty = area_specialty.replace("&", " ")
    area_specialty = unidecode.unidecode(area_specialty.lower())

    filter_specialty = util.filterSQLLike(
        area_specialty, ";", "or", "rp.area_specialty"
    )

    filterinstitution = util.filterSQL(institution, ";", "or", "i.name")
    if filterinstitution == " ":
        filterinstitution = " AND i.name in ('Universidade do Estado da Bahia','Universidade Estadual de Feira de Santana','Universidade Estadual de Santa Cruz','Universidade Estadual do Sudoeste da Bahia' )"

    reg = sgbdSQL.consultar_db(
        "SELECT i.id as id,i.image as image,"
        + "i.name as institution,count(r.id) as qtd"
        + " FROM  researcher r , institution i,researcher_production rp  "
        + " WHERE "
        + " rp.researcher_id =r.id"
        " AND r.institution_id = i.id "
        + "%s" % filter_great_area
        + "%s" % filter_specialty
        + "%s" % filterinstitution
        +
        # ' AND term = \''+term+"\'"
        # ' AND (name::tsvector@@ \''+termX+'\'::tsquery)=true ' +
        " Group by i.id ,i.image,"
        + "i.name "
        + " ORDER BY i.name"
    )

    df_bd = pd.DataFrame(reg, columns=["id", "image", "institution", "qtd"])
    return df_bd


def lista_researcher_area_speciality_db(term, institution, graduate_program_id):
    term_filter = util.web_search_filter(term, "rp.area_specialty")

    institution_filter = str()
    if institution:
        institution_filter = util.filterSQL(institution, ";", "or", "i.name")

    filter_graduate_program = str()
    if graduate_program_id:
        filter_graduate_program = f"""
            AND r.id IN (
                SELECT DISTINCT gpr.researcher_id
                FROM graduate_program_researcher gpr
                WHERE gpr.graduate_program_id = '{graduate_program_id}')
            """

    script_sql = f"""
        SELECT
            r.id AS id,
            r.name AS researcher_name,
            r.lattes_id AS lattes,
            0 as among,
            rp.articles AS articles,
            rp.book_chapters AS book_chapters,
            rp.book AS book,
            rp.patent AS patent,
            rp.software AS software,
            rp.brand AS brand,
            i.name AS university,
            r.abstract AS abstract,
            UPPER(REPLACE(LOWER(TRIM(rp.great_area)), '_', ' ')) AS area,
            rp.city AS city,
            r.orcid AS orcid,
            i.image AS image_university,
            r.graduation AS graduation,
            to_char(r.last_update,'dd/mm/yyyy') AS lattes_update
        FROM
            researcher r
            LEFT JOIN city c ON c.id = r.city_id
            LEFT JOIN institution i ON r.institution_id = i.id
            LEFT JOIN researcher_production rp ON r.id = rp.researcher_id
        WHERE
            {term_filter}
            {institution_filter}
            {filter_graduate_program}
        GROUP BY
            r.id, r.name, r.lattes_id, rp.articles, rp.book_chapters,
            rp.book, rp.software, rp.brand, i.name, r.abstract,
            rp.great_area, rp.city, r.orcid, i.image, r.graduation,
            r.last_update, rp.patent;
            """
    registry = sgbdSQL.consultar_db(script_sql)

    data_frame = pd.DataFrame(
        registry,
        columns=[
            "id",
            "name",
            "lattes_id",
            "among",
            "articles",
            "book_chapters",
            "book",
            "patent",
            "software",
            "brand",
            "university",
            "abstract",
            "area",
            "city",
            "orcid",
            "image_university",
            "graduation",
            "lattes_update",
        ],
    )

    data_frame = data_frame.merge(researcher_graduate_program_db(), on="id", how="left")
    data_frame = data_frame.merge(researcher_research_group_db(), on="id", how="left")
    data_frame = data_frame.merge(researcher_openAlex_db(), on="id", how="left")
    data_frame = data_frame.merge(researcher_foment_db(), on="id", how="left")
    data_frame = data_frame.merge(researcher_departament(), on="id", how="left")

    return data_frame.fillna("").to_dict(orient="records")

    print(script_sql)


def lista_researcher_participation_event_db(term, institution, graduate_program_id):
    term_filter = util.web_search_filter(term, "event_name")

    institution_filter = str()
    if institution:
        institution_filter = util.filterSQL(institution, ";", "or", "i.name")

    filter_graduate_program = str()
    if graduate_program_id:
        filter_graduate_program = f"""
            AND r.id IN (
                SELECT DISTINCT gpr.researcher_id
                FROM graduate_program_researcher gpr
                WHERE gpr.graduate_program_id = '{graduate_program_id}')
            """

    script_sql = f"""
        SELECT
            r.id AS id,
            r.name AS researcher_name,
            r.lattes_id AS lattes,
            COUNT(DISTINCT p.id) as among,
            rp.articles AS articles,
            rp.book_chapters AS book_chapters,
            rp.book AS book,
            rp.patent AS patent,
            rp.software AS software,
            rp.brand AS brand,
            i.name AS university,
            r.abstract AS abstract,
            UPPER(REPLACE(LOWER(TRIM(rp.great_area)), '_', ' ')) AS area,
            rp.city AS city,
            r.orcid AS orcid,
            i.image AS image_university,
            r.graduation AS graduation,
            to_char(r.last_update,'dd/mm/yyyy') AS lattes_update
        FROM
            researcher r
            LEFT JOIN city c ON c.id = r.city_id
            LEFT JOIN institution i ON r.institution_id = i.id
            LEFT JOIN researcher_production rp ON r.id = rp.researcher_id
            RIGHT JOIN participation_events p ON p.researcher_id = r.id
        WHERE
            type_participation in ('Apresentação Oral',
                                   'Conferencista','Moderador','Simposista')
            AND {term_filter}
            {institution_filter}
            {filter_graduate_program}
        GROUP BY
            r.id, r.name, r.lattes_id, rp.articles, rp.book_chapters,
            rp.book, rp.software, rp.brand, i.name, r.abstract,
            rp.great_area, rp.city, r.orcid, i.image, r.graduation,
            r.last_update, rp.patent
        ORDER BY
            among;
            """
    registry = sgbdSQL.consultar_db(script_sql)

    data_frame = pd.DataFrame(
        registry,
        columns=[
            "id",
            "name",
            "lattes_id",
            "among",
            "articles",
            "book_chapters",
            "book",
            "patent",
            "software",
            "brand",
            "university",
            "abstract",
            "area",
            "city",
            "orcid",
            "image_university",
            "graduation",
            "lattes_update",
        ],
    )

    data_frame = data_frame.merge(researcher_graduate_program_db(), on="id", how="left")
    data_frame = data_frame.merge(researcher_research_group_db(), on="id", how="left")
    data_frame = data_frame.merge(researcher_openAlex_db(), on="id", how="left")
    data_frame = data_frame.merge(researcher_foment_db(), on="id", how="left")
    data_frame = data_frame.merge(researcher_departament(), on="id", how="left")

    return data_frame.fillna("").to_dict(orient="records")


def lista_researcher_patent_db(term, institution, graduate_program_id):
    term_filter = util.web_search_filter(term, "p.title")

    institution_filter = str()
    if institution:
        institution_filter = util.filterSQL(institution, ";", "or", "i.name")

    filter_graduate_program = str()
    if graduate_program_id and graduate_program_id != "0":
        filter_graduate_program = f"""
            AND r.id IN (
                SELECT DISTINCT gpr.researcher_id
                FROM graduate_program_researcher gpr
                WHERE gpr.graduate_program_id = '{graduate_program_id}')
            """

    script_sql = f"""
        SELECT
            r.id AS id,
            r.name AS researcher_name,
            r.lattes_id AS lattes,
            COUNT(DISTINCT p.id) as among,
            rp.articles AS articles,
            rp.book_chapters AS book_chapters,
            rp.book AS book,
            rp.patent AS patent,
            rp.software AS software,
            rp.brand AS brand,
            i.name AS university,
            r.abstract AS abstract,
            UPPER(REPLACE(LOWER(TRIM(rp.great_area)), '_', ' ')) AS area,
            rp.city AS city,
            r.orcid AS orcid,
            i.image AS image_university,
            r.graduation AS graduation,
            to_char(r.last_update,'dd/mm/yyyy') AS lattes_update
        FROM
            researcher r
            LEFT JOIN city c ON c.id = r.city_id
            LEFT JOIN institution i ON r.institution_id = i.id
            LEFT JOIN researcher_production rp ON r.id = rp.researcher_id
            RIGHT JOIN patent p ON p.researcher_id = r.id
        WHERE
            {term_filter}
            {institution_filter}
            {filter_graduate_program}
        GROUP BY
            r.id, r.name, r.lattes_id, rp.articles, rp.book_chapters,
            rp.book, rp.software, rp.brand, i.name, r.abstract,
            rp.great_area, rp.city, r.orcid, i.image, r.graduation,
            r.last_update, rp.patent
        ORDER BY
            among;
            """
    registry = sgbdSQL.consultar_db(script_sql)

    data_frame = pd.DataFrame(
        registry,
        columns=[
            "id",
            "name",
            "lattes_id",
            "among",
            "articles",
            "book_chapters",
            "book",
            "patent",
            "software",
            "brand",
            "university",
            "abstract",
            "area",
            "city",
            "orcid",
            "image_university",
            "graduation",
            "lattes_update",
        ],
    )

    data_frame = data_frame.merge(researcher_graduate_program_db(), on="id", how="left")
    data_frame = data_frame.merge(researcher_research_group_db(), on="id", how="left")
    data_frame = data_frame.merge(researcher_openAlex_db(), on="id", how="left")
    data_frame = data_frame.merge(researcher_foment_db(), on="id", how="left")
    data_frame = data_frame.merge(researcher_departament(), on="id", how="left")

    return data_frame.fillna("").to_dict(orient="records")


def lista_researcher_event_db(term, institution, graduate_program_id):
    term_filter = util.web_search_filter(term, "p.title")

    institution_filter = str()
    if institution:
        institution_filter = util.filterSQL(institution, ";", "or", "i.name")

    filter_graduate_program = str()
    if graduate_program_id:
        filter_graduate_program = f"""
            AND r.id IN (
                SELECT DISTINCT gpr.researcher_id
                FROM graduate_program_researcher gpr
                WHERE gpr.graduate_program_id = '{graduate_program_id}')
            """

    script_sql = f"""
        SELECT
            r.id AS id,
            r.name AS researcher_name,
            r.lattes_id AS lattes,
            0 as among,
            rp.articles AS articles,
            rp.book_chapters AS book_chapters,
            rp.book AS book,
            rp.patent AS patent,
            rp.software AS software,
            rp.brand AS brand,
            i.name AS university,
            r.abstract AS abstract,
            UPPER(REPLACE(LOWER(TRIM(rp.great_area)), '_', ' ')) AS area,
            rp.city AS city,
            r.orcid AS orcid,
            i.image AS image_university,
            r.graduation AS graduation,
            to_char(r.last_update,'dd/mm/yyyy') AS lattes_update
        FROM
            researcher r
            LEFT JOIN city c ON c.id = r.city_id
            LEFT JOIN institution i ON r.institution_id = i.id
            LEFT JOIN researcher_production rp ON r.id = rp.researcher_id
            LEFT JOIN participation_events p ON p.researcher_id = r.id
        WHERE
            {term_filter}
            {institution_filter}
            {filter_graduate_program};
            """
    registry = sgbdSQL.consultar_db(script_sql)

    data_frame = pd.DataFrame(
        registry,
        columns=[
            "id",
            "name",
            "lattes_id",
            "among",
            "articles",
            "book_chapters",
            "book",
            "patent",
            "software",
            "brand",
            "university",
            "abstract",
            "area",
            "city",
            "orcid",
            "image_university",
            "graduation",
            "lattes_update",
        ],
    )

    data_frame = data_frame.merge(researcher_graduate_program_db(), on="id", how="left")
    data_frame = data_frame.merge(researcher_research_group_db(), on="id", how="left")
    data_frame = data_frame.merge(researcher_openAlex_db(), on="id", how="left")
    data_frame = data_frame.merge(researcher_foment_db(), on="id", how="left")
    data_frame = data_frame.merge(researcher_departament(), on="id", how="left")

    return data_frame.fillna("").to_dict(orient="records")


def city_search(city_name: str = None) -> str:
    if city_name is None:
        return None
    sql = """
        SELECT id FROM city WHERE LOWER(unaccent(name)) = LOWER(unaccent('{filter}'));
        """.format(filter=city_name)
    return pd.DataFrame(sgbdSQL.consultar_db(sql=sql), columns=["id"])["id"][0]


def lista_researcher_full_name_db(name, graduate_program_id, dep_id):
    filter_name = str()
    if name:
        name = name.replace(";", " ")
        filter_name = f"AND unaccent(r.name) ILIKE unaccent('{name}%')"

    filter_graduate_program = str()
    if graduate_program_id:
        filter_graduate_program = f"""
            AND r.id IN (
                SELECT DISTINCT gpr.researcher_id
                FROM graduate_program_researcher gpr
                WHERE gpr.graduate_program_id = '{graduate_program_id}')
            """

    filter_departament = str()
    if dep_id:
        filter_departament = f"""
            AND r.id IN (
                SELECT researcher_id
                FROM public.departament_researcher
                WHERE dep_id = '{dep_id}'
            )
            """

    script_sql = f"""
        SELECT
            r.id AS id,
            r.name AS researcher_name,
            r.lattes_id AS lattes,
            0 as among,
            rp.articles AS articles,
            rp.book_chapters AS book_chapters,
            rp.book AS book,
            rp.patent AS patent,
            rp.software AS software,
            rp.brand AS brand,
            i.name AS university,
            r.abstract AS abstract,
            UPPER(REPLACE(LOWER(TRIM(rp.great_area)), '_', ' ')) AS area,
            rp.city AS city,
            r.orcid AS orcid,
            i.image AS image_university,
            r.graduation AS graduation,
            to_char(r.last_update,'dd/mm/yyyy') AS lattes_update
        FROM
            researcher r
            LEFT JOIN city c ON c.id = r.city_id
            LEFT JOIN institution i ON r.institution_id = i.id
            LEFT JOIN researcher_production rp ON r.id = rp.researcher_id
        WHERE
            1 = 1
            {filter_name}
            {filter_graduate_program}
            {filter_departament};
            """
    registry = sgbdSQL.consultar_db(script_sql)

    data_frame = pd.DataFrame(
        registry,
        columns=[
            "id",
            "name",
            "lattes_id",
            "among",
            "articles",
            "book_chapters",
            "book",
            "patent",
            "software",
            "brand",
            "university",
            "abstract",
            "area",
            "city",
            "orcid",
            "image_university",
            "graduation",
            "lattes_update",
        ],
    )
    data_frame = data_frame.merge(researcher_graduate_program_db(), on="id", how="left")
    data_frame = data_frame.merge(researcher_research_group_db(), on="id", how="left")
    data_frame = data_frame.merge(researcher_openAlex_db(), on="id", how="left")
    data_frame = data_frame.merge(researcher_foment_db(), on="id", how="left")
    data_frame = data_frame.merge(researcher_departament(), on="id", how="left")
    data_frame = data_frame.merge(ufmg_researcher(), on="id", how="left")

    return data_frame.fillna("").to_dict(orient="records")


def lista_researcher_book_db(text, institution, graduate_program_id, book_type):
    filter_term = util.web_search_filter(text, "b.title")

    filter_institution = str()
    if institution:
        filter_institution = util.filterSQL(institution, ";", "or", "i.name")

    filter_graduate_program = str()
    if graduate_program_id:
        filter_graduate_program = (
            f"AND gpr.graduate_program_id = '{graduate_program_id}'"
        )

    filter_type = str()
    if book_type:
        filter_type = f"AND (b.type='{book_type}' OR  b.type='BOOK_CHAPTER') "

    script_sql = f"""
        SELECT
            COUNT(DISTINCT b.id) AS qtd,
            UPPER(REPLACE(LOWER(TRIM(rp.great_area)), '_', ' ')) AS area,
            rp.area_specialty AS area_specialty,
            r.id AS id,
            r.name AS researcher_name,
            i.name AS institution,
            rp.articles AS articles,
            rp.book_chapters AS book_chapters,
            rp.book AS book,
            r.lattes_id AS lattes,
            r.lattes_10_id AS lattes_10_id,
            r.abstract AS abstract,
            r.orcid AS orcid,
            rp.city AS city,
            i.image AS image,
            rp.patent,
            rp.software,
            rp.brand,
            r.last_update,
            r.graduation
        FROM
            researcher r
            LEFT JOIN graduate_program_researcher gpr ON r.id = gpr.researcher_id
            LEFT JOIN institution i ON r.institution_id = i.id
            LEFT JOIN researcher_production rp ON rp.researcher_id = r.id
            LEFT JOIN bibliographic_production b ON b.researcher_id = r.id
            LEFT JOIN city c ON r.city_id = c.id
            LEFT JOIN openalex_researcher opr ON r.id = opr.researcher_id
        WHERE
            {filter_term}
            {filter_institution}
            {filter_graduate_program}
            {filter_type}
        GROUP BY
            rp.great_area,
            rp.area_specialty,
            r.id,
            r.name,
            i.name,
            rp.articles,
            rp.book_chapters,
            rp.book,
            r.lattes_id,
            r.lattes_10_id,
            r.abstract,
            r.orcid,
            rp.city,
            i.image,
            rp.patent,
            rp.software,
            rp.brand,
            r.last_update,
            r.graduation
        ORDER BY
            qtd DESC;
        """
    reg = sgbdSQL.consultar_db(script_sql)

    data_frame = pd.DataFrame(
        reg,
        columns=[
            "among",
            "area",
            "area_specialty",
            "id",
            "name",
            "university",
            "articles",
            "book_chapters",
            "book",
            "lattes_id",
            "lattes_10_id",
            "abstract",
            "orcid",
            "city",
            "image_university",
            "patent",
            "software",
            "brand",
            "lattes_update",
            "graduation",
        ],
    )
    data_frame = data_frame.merge(researcher_graduate_program_db(), on="id", how="left")
    data_frame = data_frame.merge(researcher_research_group_db(), on="id", how="left")
    data_frame = data_frame.merge(researcher_openAlex_db(), on="id", how="left")
    data_frame = data_frame.merge(researcher_foment_db(), on="id", how="left")
    data_frame = data_frame.merge(researcher_departament(), on="id", how="left")
    data_frame = data_frame.merge(ufmg_researcher(), on="id", how="left")

    return data_frame.fillna("").to_dict(orient="records")


def researcher_research_project(term, year, graduate_program_id, researcher_id):
    if term:
        filter_term = f"""AND {util.web_search_filter(term, "title")}"""
    else:
        filter_term = str()

    if graduate_program_id:
        filter_graduate_program = """AND researcher_id IN (SELECT researcher_id FROM graduate_program_researcher WHERE graduate_program_id = '{graduate_program_id}')"""
    else:
        filter_graduate_program = str()

    if researcher_id:
        researcher_filter = f"""AND researcher_id = '{researcher_id}'"""
    else:
        researcher_filter = str()
    SCRIPQ_SQL = f"""
        SELECT
            rp.id,
            rp.researcher_id,
            r.name,
            rp.start_year,
            rp.end_year,
            rp.agency_code,
            rp.agency_name,
            rp.project_name,
            rp.status,
            rp.nature,
            rp.number_undergraduates,
            rp.number_specialists,
            rp.number_academic_masters,
            rp.number_phd,
            rp.description,
            rpp.production,
            rpf.foment,
            rpc.components
        FROM
            public.research_project rp
        LEFT JOIN researcher r ON r.id = rp.researcher_id
        LEFT JOIN (
            SELECT 
                project_id, 
                JSONB_AGG(JSONB_BUILD_OBJECT('title', title, 'type', type)) AS production
            FROM 
                public.research_project_production
            WHERE
                1 = 1
                {filter_term}
            GROUP BY 
                project_id
        ) rpp ON rpp.project_id = rp.id
        LEFT JOIN (
            SELECT 
                project_id, 
                JSONB_AGG(JSONB_BUILD_OBJECT('agency_name', agency_name, 'agency_code', agency_code, 'nature', nature)) AS foment 
            FROM 
                public.research_project_foment 
            GROUP BY 
                project_id
        ) rpf ON rpf.project_id = rp.id
        LEFT JOIN (
            SELECT 
                project_id, 
                JSONB_AGG(JSONB_BUILD_OBJECT('name', name, 'lattes_id', lattes_id, 'citations', citations)) AS components 
            FROM 
                public.research_project_components 
            GROUP BY 
                project_id
        ) rpc ON rpc.project_id = rp.id
        WHERE
            start_year >= {year}
            {filter_graduate_program}
            {researcher_filter}
        """

    registry = sgbdSQL.consultar_db(SCRIPQ_SQL)

    data_frame = pd.DataFrame(
        registry,
        columns=[
            "id",
            "researcher_id",
            "researcher_name",
            "start_year",
            "end_year",
            "agency_code",
            "agency_name",
            "project_name",
            "status",
            "nature",
            "number_undergraduates",
            "number_specialists",
            "number_academic_masters",
            "number_phd",
            "description",
            "production",
            "foment",
            "components",
        ],
    )
    return data_frame.fillna("").to_dict(orient="records")


def researcher_graduate_program_db():
    script_sql = """
        SELECT
            gpr.researcher_id as id,
            jsonb_agg(jsonb_build_object(
            'graduate_program_id', gp.graduate_program_id,
            'name', gp.name
            )) as graduate_programs
        FROM
            graduate_program_researcher gpr
            LEFT JOIN graduate_program gp ON gpr.graduate_program_id = gp.graduate_program_id
        GROUP BY
            gpr.researcher_id
        """
    registry = sgbdSQL.consultar_db(script_sql)

    data_frame = pd.DataFrame(registry, columns=["id", "graduate_programs"])

    return data_frame.fillna("")


def researcher_research_group_db():
    script_sql = """
        SELECT 
            rg.first_leader_id as id,
            jsonb_agg(jsonb_build_object(
            'group_id', rg.id,
            'name', rg.name,
            'area', rg.area,
            'census',rg.census,
            'start_of_collection', rg.start_of_collection,
            'end_of_collection', rg.end_of_collection,
            'group_identifier', rg.group_identifier,
            'year', rg.year,
            'institution_name', rg.institution_name,
            'category', rg.category
            )) as research_groups
        FROM 
            research_group rg
        WHERE rg.first_leader_id IS NOT NULL
        GROUP BY
            rg.first_leader_id

        UNION

        SELECT 
            rg.second_leader_id as id,
            jsonb_agg(jsonb_build_object(
            'group_id', rg.id,
            'name', rg.name,
            'area', rg.area,
            'census',rg.census,
            'start_of_collection', rg.start_of_collection,
            'end_of_collection', rg.end_of_collection,
            'group_identifier', rg.group_identifier,
            'year', rg.year,
            'institution_name', rg.institution_name,
            'category', rg.category
            )) as research_groups
        FROM 
            research_group rg
        WHERE rg.second_leader_id IS NOT NULL
        GROUP BY
            rg.second_leader_id
        """
    registry = sgbdSQL.consultar_db(script_sql)

    data_frame = pd.DataFrame(registry, columns=["id", "research_groups"])

    return data_frame.fillna("")


def researcher_openAlex_db():
    script_sql = """
        SELECT 
            researcher_id as id, 
            h_index, 
            relevance_score, 
            works_count, 
            cited_by_count, 
            i10_index, 
            scopus, 
            openalex
        FROM 
            public.openalex_researcher;
        """
    registry = sgbdSQL.consultar_db(script_sql)

    data_frame = pd.DataFrame(
        registry,
        columns=[
            "id",
            "h_index",
            "relevance_score",
            "works_count",
            "cited_by_count",
            "i10_index",
            "scopus",
            "openalex",
        ],
    )

    return data_frame.fillna("")


def researcher_foment_db():
    script_sql = """
        SELECT 
            s.researcher_id as id,
            jsonb_agg(jsonb_build_object(
            'id', s.id,
            'modality_code', s.modality_code, 
            'modality_name', s.modality_name, 
            'call_title', s.call_title,
            'category_level_code', s.category_level_code, 
            'funding_program_name', s.funding_program_name, 
            'institute_name', s.institute_name, 
            'aid_quantity', s.aid_quantity, 
            'scholarship_quantity', s.scholarship_quantity
            )) as foment
        FROM
            foment s
        GROUP BY
            s.researcher_id
        """
    registry = sgbdSQL.consultar_db(script_sql)

    data_frame = pd.DataFrame(registry, columns=["id", "subsidy"])

    return data_frame.fillna("")


def researcher_departament():
    script_sql = """
        SELECT 
            dpr.researcher_id as id,
            jsonb_agg(jsonb_build_object(
                'dep_id', dp.dep_id,
                'org_cod', dp.org_cod,
                'dep_nom', dp.dep_nom,
                'dep_des', dp.dep_des,
                'dep_email', dp.dep_email,
                'dep_site', dp.dep_site,
                'dep_sigla', dp.dep_sigla,
                'dep_tel', dp.dep_tel
            )) as departments
        FROM
            public.departament_researcher dpr
            LEFT JOIN public.ufmg_departament dp ON dpr.dep_id = dp.dep_id
        GROUP BY
            dpr.researcher_id;
        """

    reg = sgbdSQL.consultar_db(script_sql)

    df = pd.DataFrame(reg, columns=["id", "departments"])

    return df


def ufmg_researcher():
    script_sql = """
        SELECT
            researcher_id as id,
            matric,
            inscufmg,
            genero,
            situacao,
            rt,
            clas,
            cargo,
            classe,
            ref,
            titulacao,
            entradanaufmg,
            progressao,
            semester
        FROM
            public.ufmg_teacher
        WHERE researcher_id IS NOT NULL;
        """
    reg = sgbdSQL.consultar_db(script_sql)

    df = pd.DataFrame(
        reg,
        columns=[
            "id",
            "matric",
            "inscufmg",
            "genero",
            "situacao",
            "rt",
            "clas",
            "cargo",
            "classe",
            "ref",
            "titulacao",
            "entradanaufmg",
            "progressao",
            "semester",
        ],
    )

    return df
