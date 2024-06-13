import nltk
import pandas as pd
import unidecode

import Dao.sgbdSQL as sgbdSQL
import Dao.util as util


def get_researcher_address_db(researcher_id):

    script_sql = f"""
        SELECT 
            distinct city, 
            organ 
        FROM 
            researcher_address ra 
        WHERE 
            ra.researcher_id = '{researcher_id}'"""

    reg = sgbdSQL.consultar_db(script_sql)

    df_bd = pd.DataFrame(reg, columns=["city", "organ"])

    return df_bd


# Função para listar a palavras do dicionário passando as iniciais
def list_research_dictionary_db(initials, type):

    initials = unidecode.unidecode(initials.lower())

    filter = " AND   LOWER(unaccent(term)) LIKE '" + initials + "%' "
    fetch = "  fetch FIRST 50 rows only"
    filterType = ""
    if type == "BOOK":
        filterType = " (type_='BOOK' or type_='BOOK_CHAPTER') "
    else:
        filterType = " type_='" + type + "'"

    sql = """
           SELECT  distinct unaccent(term) as term,count(frequency) as frequency ,type_  
                                from research_dictionary r 
                                 WHERE 
                                  %s
                                 %s 
                                 GROUP BY unaccent(term),type_  ORDER BY frequency desc %s
      """ % (
        filterType,
        filter,
        fetch,
    )
    reg = sgbdSQL.consultar_db(sql)

    df_bd = pd.DataFrame(reg, columns=["term", "frequency", "type"])

    return df_bd


def lists_patent_production_researcher_db(researcher_id, year, term):
    filter_term = str()
    if term:
        filter_term = util.web_search_filter(term, "title")

    filter_year = str()
    if year:
        filter_year = f"AND p.development_year::integer >= {year}"

    filter_researcher = str()
    if researcher_id:
        filter_researcher = f"AND researcher_id = '{researcher_id}'"

    script_sql = f"""
        SELECT 
            p.id as id, 
            p.title as title, 
            p.development_year as year, 
            p.grant_date as grant_date
        FROM  
            patent p
        where 
            {filter_term}
            {filter_year}
            {filter_researcher}
        ORDER BY development_year desc
        """

    reg = sgbdSQL.consultar_db(script_sql)

    df_bd = pd.DataFrame(reg, columns=["id", "title", "year", "grant_date"])
    df_bd["grant_date"] = df_bd["grant_date"].astype("str").replace("NaT", "")

    return df_bd


def lists_book_production_researcher_db(researcher_id, year, term):
    filter_term = str()
    if term:
        filter_term = f'AND {util.web_search_filter(term, "title")}'

    script_sql = """SELECT 
                b.id as id, 
                b.title as title, 
                b.year as year,
                bb.isbn,
                bb.publishing_company
            FROM   
                bibliographic_production b,
                bibliographic_production_book bb
            where 
                bb.bibliographic_production_id = b.id AND
                researcher_id = '%s'
                AND b.year_>=%s
                AND b.type='%s'
                %s
                ORDER BY year_ desc""" % (
        researcher_id,
        year,
        "BOOK",
        filter_term,
    )
    print(script_sql)
    reg = sgbdSQL.consultar_db(script_sql)

    df_bd = pd.DataFrame(
        reg, columns=["id", "title", "year", "isbn", "publishing_company"]
    )

    return df_bd


def lists_book_chapter_production_researcher_db(researcher_id, year, term):
    filter = str()
    if term:
        filter = f'AND {util.web_search_filter(term, "title")}'

    sql = """SELECT 
                b.id as id, 
                b.title as title, 
                b.year as year,
                bc.isbn,
                bc.publishing_company
            FROM   
                bibliographic_production b, bibliographic_production_book_chapter bc
            where 
                bc.bibliographic_production_id = b.id AND
                researcher_id='%s'
                AND b.year_>=%s
                AND b.type='%s'
                %s
            ORDER BY year_ desc""" % (
        researcher_id,
        year,
        "BOOK_CHAPTER",
        filter,
    )

    reg = sgbdSQL.consultar_db(sql)

    df_bd = pd.DataFrame(
        reg, columns=["id", "title", "year", "isbn", "publishing_company"]
    )

    return df_bd


def lists_brand_production_researcher_db(researcher_id, year):
    sql = """SELECT b.id as id, b.title as title, 
            b.year as year
                        
            FROM  brand b
                           where 
                           researcher_id='%s'
                           AND b.year>=%s
                           ORDER BY year desc""" % (
        researcher_id,
        year,
    )
    # print(sql)

    reg = sgbdSQL.consultar_db(sql)

    df_bd = pd.DataFrame(reg, columns=["id", "title", "year"])

    return df_bd


def lists_Researcher_Report_db(researcher_id, year):

    sql = """SELECT rr.id as id, rr.title as title, 
            rr.year as year,project_name,financing_institutionc
                        
            FROM  research_report rr
                           where 
                           researcher_id='%s'
                           AND rr.year>=%s
                           ORDER BY year desc""" % (
        researcher_id,
        year,
    )

    reg = sgbdSQL.consultar_db(sql)

    df_bd = pd.DataFrame(
        reg, columns=["id", "title", "year", "project_name", "financing_institutionc"]
    )

    return df_bd


def lists_guidance_researcher_db(researcher_id, year):
    script_sql = """
        SELECT 
            g.id as id,
            g.title as title, 
            nature,
            oriented,
            type,
            status,
            g.year as year            
        FROM 
            guidance g
        WHERE 
            researcher_id = '%s'
            AND g.year >= %s
            ORDER BY year desc""" % (
        researcher_id,
        year,
    )

    reg = sgbdSQL.consultar_db(script_sql)

    df_bd = pd.DataFrame(
        reg, columns=["id", "title", "nature", "oriented", "type", "status", "year"]
    )

    return df_bd


def lists_pevent_researcher_db(researcher_id, year, term, nature):
    filter = util.filterSQLRank(term, ";", "event_name")
    filterNature = util.filterSQL(nature, ";", "or", "nature")

    sql = """SELECT p.id as id, event_name, nature,form_participation,year
           
                        
            FROM  participation_events p
                           where 
                           researcher_id='%s'
                           AND p.year>=%s
                           %s
                           %s

                           ORDER BY year desc""" % (
        researcher_id,
        year,
        filterNature,
        filter,
    )
    # print(sql)

    reg = sgbdSQL.consultar_db(sql)
    print(sql)

    df_bd = pd.DataFrame(
        reg, columns=["id", "event_name", "nature", "form_participation", "year"]
    )

    return df_bd


def lists_software_production_researcher_db(researcher_id, year):
    sql = """SELECT s.id as id, s.title as title, 
            s.year as year
                        
            FROM  software s
                           where 
                           researcher_id='%s'
                           AND s.year>=%s
                           ORDER BY year desc""" % (
        researcher_id,
        year,
    )
    # print(sql)

    reg = sgbdSQL.consultar_db(sql)

    df_bd = pd.DataFrame(reg, columns=["id", "title", "year"])

    return df_bd


def lists_bibliographic_production_article_researcher_db(
    term: str = None,
    researcher_id: str = None,
    year: int = None,
    type: str = None,
    boolean_condition: str = None,
    qualis: str = None,
):

    filter = str()
    if term:
        filter = util.filterSQLRank(unidecode.unidecode(term.lower()), ";", "title")

    filter_qualis = str()
    if qualis:
        filter_qualis = util.filterSQL(qualis, ";", "or", "qualis")

    if type == "ARTICLE":
        script_sql = f""" 
            SELECT DISTINCT 
                b.id AS id,
                title,
                b.year AS year,
                type,
                doi,
                ba.qualis,
                periodical_magazine_name AS magazine,
                r.name AS researcher,
                r.lattes_10_id AS lattes_10_id,
                r.lattes_id AS lattes_id,
                jcr AS jif,
                jcr_link,
                r.id as researcher_id
            FROM 
                bibliographic_production b, 
                bibliographic_production_article ba,
                institution i, 
                researcher r 
            WHERE 
                r.id = b.researcher_id
                AND b.id = ba.bibliographic_production_id 
                AND r.institution_id = i.id
                AND year_ >= {year}  
                {filter} 
                {filter_qualis}
                AND r.id = '{researcher_id}' 
            ORDER BY 
                year DESC
            """

    if type == "ABSTRACT":
        script_sql = f"""
        SELECT DISTINCT 
            b.id AS id,
            title,
            year,
            type,
            doi,
            ba.qualis,
            periodical_magazine_name AS magazine,
            r.name AS researcher,
            r.lattes_10_id AS lattes_10_id,
            r.lattes_id AS lattes_id,
            jcr AS jif,
            jcr_link
        FROM 
            bibliographic_production b,
            bibliographic_production_article ba, 
            researcher r 
        WHERE  
            r.id = b.researcher_id
            AND rf.researcher_id = r.id 
            AND pm.id = ba.periodical_magazine_id 
            AND b.id = ba.bibliographic_production_id 
            AND year_ >= {year} 
            {filter} 
            {filter_qualis}
            AND r.id = '{researcher_id}'
        ORDER BY
            year DESC
        """

    print(script_sql)

    reg = sgbdSQL.consultar_db(script_sql)

    data_frame = pd.DataFrame(
        reg,
        columns=[
            "id",
            "title",
            "year",
            "type",
            "doi",
            "qualis",
            "magazine",
            "researcher",
            "lattes_10_id",
            "lattes_id",
            "jif",
            "jcr_link",
            "researcher_id",
        ],
    )

    return data_frame


def lists_bibliographic_production_qtd_qualis_researcher_db(
    researcher_id, year, graduate_program_id
):

    filter = ""
    if researcher_id != "":
        filter = f"AND b.researcher_id='{researcher_id}'"
    filter_graduate_program = ""
    if graduate_program_id != "":
        filter_graduate_program = (
            f"AND gpr.graduate_program_id = '{graduate_program_id}'"
        )

    sql = f"""
        SELECT COUNT(*) AS qtd, bar.qualis
        FROM PUBLIC.bibliographic_production b
        LEFT JOIN graduate_program_researcher gpr ON b.researcher_id = gpr.researcher_id,
        bibliographic_production_article bar,
        periodical_magazine pm
        WHERE pm.id = bar.periodical_magazine_id
        AND b.id = bar.bibliographic_production_id
        AND year_ >= {year}
        {filter}
        {filter_graduate_program}
        GROUP BY bar.qualis
        ORDER BY qualis ASC
        """

    reg = sgbdSQL.consultar_db(sql)

    df_bd = pd.DataFrame(reg, columns=["qtd", "qualis"])

    return df_bd


def lists_word_researcher_db(researcher_id, graduate_program):

    stopwords = nltk.corpus.stopwords.words("portuguese")
    stopwords += nltk.corpus.stopwords.words("english")

    filter_researcher = str()
    filter_graduate_program = str()

    if researcher_id:
        filter_researcher = f"WHERE b.researcher_id = '{researcher_id}'"
    elif graduate_program:
        filter_graduate_program = f"""
        JOIN
        	graduate_program_researcher gpr ON
        		b.researcher_id = gpr.researcher_id
        WHERE gpr.graduate_program_id = '{graduate_program}'
        """

    script_sql = f"""
        SELECT
            translate(unaccent(LOWER(b.title)),'-\\.:;?(),', ' ')::tsvector  
        FROM 
            bibliographic_production b
        {filter_researcher}
        {filter_graduate_program}
        """

    script_sql = f"""
            SELECT 
                ndoc AS qtd,
                INITCAP(word) AS term
            FROM 
                ts_stat($${script_sql}$$)
            WHERE 
                CHAR_LENGTH(word)>3 
                AND word NOT IN {tuple(stopwords)}
        	ORDER BY
                ndoc DESC 
            FETCH FIRST 20 ROWS ONLY;
            """

    reg = sgbdSQL.consultar_db(script_sql)

    data_frame = pd.DataFrame(reg, columns=["qtd", "term"])

    return data_frame


def lista_institution_production_db(text, institution, type_):

    text = unidecode.unidecode(text)
    filter = filter = util.filterSQLRank(text, ";", "b.title")

    filterinstitution = util.filterSQL(institution, ";", "or", "i.name")

    print(filter)
    # b.id = rf.bibliographic_production_id
    # researcher_frequency rf,
    #  AND rf.researcher_id = r.id
    print("type " + type_)
    sql = ""

    if type_ == "SPEAKER":

        filter = filter = util.filterSQLRank(text, ";", "event_name")

        sql = """SELECT  COUNT(distinct r.id) AS qtd,i.id as id, i.name as institution,image 
                  FROM  researcher r , institution i, researcher_production rp,   participation_events AS b 
                           WHERE 
                            r.institution_id = i.id 
                           AND r.id = b.researcher_id 
                           AND rp.researcher_id = r.id 
                           AND acronym IS NOT NULL

                           AND type_participation in ('Apresentação Oral','Conferencista','Moderador','Simposista') 
                           
                           
                           %s
                           %s
                          
                           GROUP BY  i.id, i.name """ % (
            filter,
            filterinstitution,
        )

    if type_ == "ABSTRACT":

        filter = filter = util.filterSQLRank(text, ";", "r.abstract")

        sql = """SELECT  COUNT(distinct r.id) AS qtd,i.id as id, i.name as institution,image 
                  FROM  researcher r , institution i, researcher_production rp 
                           WHERE 
                            r.institution_id = i.id 
                          
                           AND rp.researcher_id = r.id 
                           AND acronym IS NOT NULL
                         
                           
                           %s
                           %s
                          
                           GROUP BY  i.id, i.name """ % (
            filter,
            filterinstitution,
        )

    if type_ == "ARTICLE" or type_ == "BOOK":
        filterType = " AND b.type='%s'" % type_

        if type_ == "BOOK":
            filterType = " AND (b.type='%s' or b.type='BOOK_CHAPTER') " % type_
        # AND acronym IS NOT NULL
        sql = """SELECT  COUNT( r.id) AS qtd,i.id as id, i.name as institution,image 
                  FROM  researcher r , institution i, researcher_production rp, bibliographic_production AS b 
                           WHERE 
                            r.institution_id = i.id 
                           AND r.id = b.researcher_id 
                           AND rp.researcher_id = r.id 
                           AND acronym IS NOT NULL
                           
                           %s
                           
                           %s
                           %s
                          
                           GROUP BY  i.id, i.name """ % (
            filterType,
            filter,
            filterinstitution,
        )

    if type_ == "PATENT":
        sql = """SELECT  COUNT(distinct b.title) AS qtd,i.id as id, i.name as institution,image 
                  FROM  researcher r , institution i, researcher_production rp, patent AS b 
                           WHERE 
                            r.institution_id = i.id 
                           AND r.id = b.researcher_id 
                           AND rp.researcher_id = r.id 
                           AND acronym IS NOT NULL
                         
                           
                           %s
                           %s
                          
                           GROUP BY  i.id, i.name """ % (
            filter,
            filterinstitution,
        )
    print(sql)

    reg = sgbdSQL.consultar_db(sql)
    df_bd = pd.DataFrame(reg, columns=["qtd", "id", "institution", "image"])

    return df_bd


def lista_researcher_id_db(researcher_id):

    script_sql = f"""
        SELECT 
            DISTINCT r.id AS id,
            opr.h_index,
            opr.relevance_score,
            opr.works_count,
            opr.cited_by_count,
            opr.i10_index,
            opr.scopus,
            opr.openalex,
            r.name AS researcher_name,
            i.name AS institution,
            rp.articles AS articles,
            rp.book_chapters AS book_chapters,
            rp.book AS book,
            r.lattes_id AS lattes,
            r.lattes_10_id AS lattes_10_id,
            r.abstract AS abstract,
            UPPER(REPLACE(LOWER(TRIM(rp.great_area)), '_', ' ')) AS area,
            rp.city AS city,
            i.image AS image,
            r.orcid AS orcid,
            r.graduation AS graduation,
            rp.patent AS patent,
            rp.software AS software,
            rp.brand AS brand,
            TO_CHAR(r.last_update, 'dd/mm/yyyy') AS lattes_update
        FROM
            researcher r
            LEFT JOIN city c ON c.id = r.city_id
            LEFT JOIN institution i ON r.institution_id = i.id
            LEFT JOIN researcher_production rp ON rp.researcher_id = r.id
            LEFT JOIN openalex_researcher opr ON r.id = opr.researcher_id
        WHERE
            r.id = '{researcher_id}'
     """
    reg = sgbdSQL.consultar_db(script_sql)

    data_frame = pd.DataFrame(
        reg,
        columns=[
            "id",
            "h_index",
            "relevance_score",
            "works_count",
            "cited_by_count",
            "i10_index",
            "scopus",
            "openalex",
            "name",
            "university",
            "articles",
            "book_chapters",
            "book",
            "lattes_id",
            "lattes_10_id",
            "abstract",
            "area",
            "city",
            "image_university",
            "orcid",
            "graduation",
            "patent",
            "software",
            "brand",
            "lattes_update",
        ],
    )

    return data_frame.fillna(0).to_dict(orient="records")


def list_researchers_originals_words_db(terms, institution, type_, graduate_program_id):
    filter_type = str()
    if type_ == "ARTICLE":
        term_filter = util.web_search_filter(terms, "title")
        filter_type = " AND b.type='ARTICLE' "
    elif type_ == "ABSTRACT":
        term_filter = util.web_search_filter(terms, "abstract")

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
            COUNT(DISTINCT b.id),
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
            LEFT JOIN bibliographic_production b ON b.researcher_id = r.id
        WHERE
            {term_filter}
            {institution_filter}
            {filter_graduate_program}
            {filter_type}
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
    data_frame = data_frame.merge(researcher_subsidy_db(), on="id", how="left")

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
            rg.researcher_id as id,
            jsonb_agg(jsonb_build_object(
            'research_group_id', rg.research_group_id,
            'name', rg.research_group_name
            )) as research_groups
        FROM 
            research_group rg
        GROUP BY
            rg.researcher_id
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


def researcher_subsidy_db():
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
            )) as subsidy
        FROM
            subsidy s
        GROUP BY
            s.researcher_id
        """
    registry = sgbdSQL.consultar_db(script_sql)

    data_frame = pd.DataFrame(registry, columns=["id", "subsidy"])

    return data_frame.fillna("")
