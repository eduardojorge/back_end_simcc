import Dao.sgbdSQL as sgbdSQL
import unidecode
import pandas as pd
import Dao.util as util

# Função para listar a palavras do dicionário passando as iniciais


# Função que lista  as áreas de expertrize por Iniciais


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
        " AND ( LOWER(unaccent(asp.name)) LIKE '" + initials.lower() + "%' )" +
        #  "  or " +filter_+")"+
        " %s" % filter + " %s " % filtergraduate_program +
        # " AND  LOWER(gea.name)='%s'" % area.lower()+
        " ORDER BY  ae.name,asp.name %s" % fetch
    )

    df_bd = pd.DataFrame(reg, columns=["area_expertise", "area_specialty"])

    print(df_bd)
    return df_bd


def lista_researcher_area_expertise_db(text, institution):
    text = text.replace(" ", "_")
    filter = util.filterSQL(text, ";", "or", "gae.name")

    filterinstitution = util.filterSQL(institution, ";", "or", "i.name")

    if filter != " ":
        reg = sgbdSQL.consultar_db(
            "SELECT distinct rp.great_area as area,r.id as id,"
            + "r.name as researcher_name,i.name as institution,rp.articles as articles,"
            + " rp.book_chapters as book_chapters, rp.book as book, r.lattes_id as lattes,r.lattes_10_id as lattes_10_id,r.abstract as abstract,"
            + "r.orcid as orcid,rp.city as city, i.image as image,rp.patent,rp.sofwtare,rp.brand,r.last_update "
            " FROM  researcher r , institution i, researcher_production rp,researcher_area_expertise re, great_area_expertise gae, city c "
            + " WHERE "
            + "  re.researcher_id =r.id"
            " And r.city_id=c.id"
            " AND gae.id = re.great_area_expertise_id"
            " AND r.institution_id = i.id "
            + " AND rp.researcher_id = r.id "
            + "%s" % filter
            + "%s" % filterinstitution
            +
            #' AND term = \''+term+"\'"
            #' AND (name::tsvector@@ \''+termX+'\'::tsquery)=true ' +
            " ORDER BY researcher_name"
        )

    else:
        reg = sgbdSQL.consultar_db(
            "SELECT distinct rp.great_area as area,r.id as id,"
            + "r.name as researcher_name,i.name as institution,rp.articles as articles,"
            + " rp.book_chapters as book_chapters, rp.book as book, r.lattes_id as lattes,r.lattes_10_id as lattes_10_id,r.abstract as abstract,"
            + "r.orcid as orcid,rp.city as city, i.image as image,,rp.patent,rp.sofwtare,rp.brand.r.last_update "
            + " FROM  researcher r , institution i, researcher_production rp,city c "
            + " WHERE "
            +
            #   '  re.researcher_id =r.id'
            " r.city_id=c.id"
            # ' AND gae.id = re.great_area_expertise_id'
            " AND r.institution_id = i.id "
            + " AND rp.researcher_id = r.id "
            + "%s" % filter
            + "%s" % filterinstitution
            +
            #' AND term = \''+term+"\'"
            #' AND (name::tsvector@@ \''+termX+'\'::tsquery)=true ' +
            " ORDER BY researcher_name"
        )

    df_bd = pd.DataFrame(
        reg,
        columns=[
            "area",
            "id",
            "researcher_name",
            "institution",
            "articles",
            "book_chapters",
            "book",
            "lattes",
            "lattes_10_id",
            "abstract",
            "orcid",
            "city",
            "image",
            "patent",
            "sofwtare",
            "brand",
            "last_update",
        ],
    )
    # print (df_bd)
    return df_bd


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
            bp.title AS title,
            r.name AS researcher,
            r.lattes_id AS lattes_id,
            lattes_10_id,
            rp.great_area AS area,
            bp.year AS year,
            periodical_magazine_name AS magazine,
            doi,
            qualis,
            jcr,
            jcr_link
        FROM
            researcher r
            LEFT JOIN graduate_program_researcher gpr ON r.id = gpr.researcher_id
            JOIN researcher_production rp ON r.id = rp.researcher_id
            JOIN bibliographic_production bp ON bp.researcher_id = r.id
            JOIN bibliographic_production_article bar ON bp.id = bar.bibliographic_production_id
        WHERE
            bp.year >= {year}
            {filter}
            {filterQualis}
            {filter_specialty}
            {filtergraduate_program}
        GROUP BY
            bp.id,
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
    print(df_bd)
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
        #' AND term = \''+term+"\'"
        #' AND (name::tsvector@@ \''+termX+'\'::tsquery)=true ' +
        " Group by i.id ,i.image," + "i.name " + " ORDER BY i.name"
    )

    df_bd = pd.DataFrame(reg, columns=["id", "image", "institution", "qtd"])
    print(df_bd)
    return df_bd


def lista_researcher_area_speciality_db(term, institution, graduate_program_id):

    filter_term = util.web_search_filter(term, "rp.area_specialty")

    filter_institution = str()
    if institution:
        filter_institution = util.filterSQL(institution, ";", "or", "i.name")

    filtergraduate_program = str()
    if graduate_program_id:
        filtergraduate_program = "AND gpr.graduate_program_id = 'graduate_program_id'"

    script_sql = f"""
        SELECT 
            DISTINCT rp.great_area AS area,
            opr.h_index,
            opr.relevance_score,
            opr.works_count,
            opr.cited_by_count,
            opr.i10_index,
            opr.scopus,
            opr.openalex,
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
        FROM researcher r 
        LEFT JOIN graduate_program_researcher gpr ON r.id = gpr.researcher_id
        LEFT JOIN institution i ON r.institution_id = i.id
        LEFT JOIN researcher_production rp ON rp.researcher_id = r.id 
        LEFT JOIN researcher_area_expertise re ON re.researcher_id = r.id 
        LEFT JOIN great_area_expertise gae ON gae.id = re.great_area_expertise_id 
        LEFT JOIN city c ON r.city_id = c.id
        LEFT JOIN openalex_researcher opr ON r.id = opr.researcher_id 
        WHERE 
            {filter_term} 
            {filter_institution} 
            {filtergraduate_program}
        ORDER BY researcher_name
        """

    reg = sgbdSQL.consultar_db(script_sql)

    data_frame = pd.DataFrame(
        reg,
        columns=[
            "area",
            "h_index",
            "relevance_score",
            "works_count",
            "cited_by_count",
            "i10_index",
            "scopus",
            "openalex",
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
            "last_update",
            "graduation",
        ],
    )
    return data_frame.fillna(0).to_dict(orient="records")


def lista_researcher_participation_event_db(term, institution, graduate_program_id):
    term_filter = util.web_search_filter(term, "p.title")

    institution_filter = str()
    if institution:
        institution_filter = util.filterSQL(institution, ";", "or", "i.name")

    filtergraduate_program = str()
    if graduate_program_id:
        filtergraduate_program = (
            f"AND gpr.graduate_program_id = '{graduate_program_id}'"
        )

    script_slq = f"""
        SELECT 
            COUNT(DISTINCT p.id) AS qtd,
            opr.h_index,
            opr.relevance_score,
            opr.works_count,
            opr.cited_by_count,
            opr.i10_index,
            opr.scopus,
            opr.openalex,
            rp.great_area AS area,
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
            LEFT JOIN participation_events p ON p.researcher_id = r.id
            LEFT JOIN city c ON r.city_id = c.id
            LEFT JOIN openalex_researcher opr ON r.id = opr.researcher_id
        WHERE 
            {term_filter}
            {institution_filter}
            {filtergraduate_program}
            AND type_participation IN ('Apresentação Oral', 'Conferencista', 'Moderador', 'Simposista') 
        GROUP BY 
            rp.great_area,
            opr.h_index,
            opr.relevance_score,
            opr.works_count,
            opr.cited_by_count,
            opr.i10_index,
            opr.scopus,
            opr.openalex,
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
    reg = sgbdSQL.consultar_db(script_slq)

    data_frame = pd.DataFrame(
        reg,
        columns=[
            "among",
            "h_index",
            "relevance_score",
            "works_count",
            "cited_by_count",
            "i10_index",
            "scopus",
            "openalex",
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
    return data_frame.fillna(0).to_dict(orient="records")


def lista_researcher_patent_db(term, institution, graduate_program_id):
    term_filter = util.web_search_filter(term, "p.title")

    filter_institution = str()
    if institution:
        filter_institution = util.filterSQL(institution, ";", "or", "i.name")

    filter_graduate_program = str()
    if graduate_program_id and graduate_program_id != "0":
        filter_graduate_program = (
            f"AND gpr.graduate_program_id = '{graduate_program_id}'"
        )

    script_sql = f"""
        SELECT
            COUNT(DISTINCT p.id) AS qtd,
            opr.h_index,
            opr.relevance_score,
            opr.works_count,
            opr.cited_by_count,
            opr.i10_index,
            opr.scopus,
            opr.openalex,
            rp.great_area as area,
            rp.area_specialty as area_specialty,
            r.id as id,
            r.name as researcher_name,
            i.name as institution,
            rp.articles as articles,
            rp.book_chapters as book_chapters,
            rp.book as book,
            r.lattes_id as lattes,
            r.lattes_10_id as lattes_10_id,
            r.abstract as abstract,
            r.orcid as orcid,
            rp.city as city,
            i.image as image,
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
        LEFT JOIN patent p ON p.researcher_id = r.id
        LEFT JOIN city c ON r.city_id = c.id
        LEFT JOIN openalex_researcher opr ON r.id = opr.researcher_id
        WHERE
            {term_filter}
            {filter_institution}
            {filter_graduate_program}
        GROUP BY
            opr.h_index,
            opr.relevance_score,
            opr.works_count,
            opr.cited_by_count,
            opr.i10_index,
            opr.scopus,
            opr.openalex,
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
            "h_index",
            "relevance_score",
            "works_count",
            "cited_by_count",
            "i10_index",
            "scopus",
            "openalex",
            "area",
            "area_specialty",
            "id",
            "researcher_name",
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
    return data_frame.fillna(0).to_dict(orient="records")


def lista_researcher_event_db(term, institution, graduate_program_id):
    term_filter = util.web_search_filter(term, "p.title")

    institution_filter = str()
    if institution:
        institution_filter = util.filterSQL(institution, ";", "or", "i.name")

    filtergraduate_program = str()
    if graduate_program_id:
        filtergraduate_program = "AND gpr.graduate_program_id = '{graduate_program_id}'"

    script_sql = f"""
        SELECT 
            DISTINCT rp.great_area as area, 
            opr.h_index,
            opr.relevance_score,
            opr.works_count,
            opr.cited_by_count,
            opr.i10_index,
            opr.scopus,
            opr.openalex,
            rp.area_specialty as area_specialty, 
            r.id as id,
            r.name as researcher_name,
            i.name as institution,
            rp.articles as articles,
            rp.book_chapters as book_chapters, 
            rp.book as book, 
            r.lattes_id as lattes,
            r.lattes_10_id as lattes_10_id,
            r.abstract as abstract,
            r.orcid as orcid,
            rp.city  as city, 
            i.image as image,
            '{term}' as terms,
            rp.patent,
            rp.software,
            rp.brand,
            r.last_update,
            r.graduation
        FROM researcher r 
        LEFT JOIN graduate_program_researcher gpr ON
            r.id = gpr.researcher_id, 
            institution i, 
            researcher_production rp,
            participation_events p,  
            city c,
            openalex_researcher opr
        WHERE 
            {term_filter} 
            {institution_filter} 
            {filtergraduate_program}
            AND r.city_id = c.id
            AND r.institution_id = i.id 
            AND rp.researcher_id = r.id 
            AND p.researcher_id = r.id
            AND r.id = opr.researcher_id
            AND type_participation in ('Apresentação Oral','Conferencista','Moderador','Simposista')  
            """

    registry = sgbdSQL.consultar_db(script_sql)

    data_frame = pd.DataFrame(
        registry,
        columns=[
            "area",
            "h_index",
            "relevance_score",
            "works_count",
            "cited_by_count",
            "i10_index",
            "scopus",
            "openalex",
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
            "terms",
            "patent",
            "software",
            "brand",
            "last_update",
            "graduation",
        ],
    )
    return data_frame.fillna(0).to_dict(orient="records")


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
            opr.h_index,
            opr.relevance_score,
            opr.works_count,
            opr.cited_by_count,
            opr.i10_index,
            opr.scopus,
            opr.openalex,
            rp.great_area AS area,
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
            opr.h_index,
            opr.relevance_score,
            opr.works_count,
            opr.cited_by_count,
            opr.i10_index,
            opr.scopus,
            opr.openalex,
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
            "h_index",
            "relevance_score",
            "works_count",
            "cited_by_count",
            "i10_index",
            "scopus",
            "openalex",
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
    return data_frame.fillna(0).to_dict(orient="records")
