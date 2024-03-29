import Dao.sgbdSQL as sgbdSQL
import unidecode
import pandas as pd
import Dao.util as util
import nltk

# Função que lista  as áreas de expertrize por Iniciais


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
    filter = util.filterSQLRank(term, ";", "title")

    sql = """SELECT p.id as id, p.title as title, 
            p.development_year as year, p.grant_date as grant_date
                        
            FROM  patent p
                           where 
                           researcher_id='%s'
                           AND p.development_year::integer>=%s
                           %s
                           ORDER BY development_year desc""" % (
        researcher_id,
        year,
        filter,
    )
    # print(sql)

    reg = sgbdSQL.consultar_db(sql)

    df_bd = pd.DataFrame(reg, columns=["id", "title", "year", "grant_date"])

    return df_bd


def lists_book_production_researcher_db(researcher_id, year, term):

    filter = util.filterSQLRank(term, ";", "title")

    sql = """SELECT b.id as id, b.title as title, 
            b.year as year,bb.isbn,bb.publishing_company
                        
            FROM   bibliographic_production b,bibliographic_production_book bb
                           where 
                           bb.bibliographic_production_id = b.id AND
                           researcher_id='%s'
                           AND b.year_>=%s
                           AND b.type='%s'
                           %s
                           ORDER BY year_ desc""" % (
        researcher_id,
        year,
        "BOOK",
        filter,
    )
    print(sql)

    reg = sgbdSQL.consultar_db(sql)

    df_bd = pd.DataFrame(
        reg, columns=["id", "title", "year", "isbn", "publishing_company"]
    )

    return df_bd


def lists_book_chapter_production_researcher_db(researcher_id, year, term):
    filter = util.filterSQLRank(term, ";", "title")

    sql = """SELECT b.id as id, b.title as title, 
            b.year as year,bc.isbn,bc.publishing_company
                        
            FROM   bibliographic_production b, bibliographic_production_book_chapter bc
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
    # print(sql)

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
    # print(sql)

    reg = sgbdSQL.consultar_db(sql)

    df_bd = pd.DataFrame(
        reg, columns=["id", "title", "year", "project_name", "financing_institutionc"]
    )

    return df_bd


def lists_guidance_researcher_db(researcher_id, year):
    sql = """SELECT g.id as id, g.title as title, 
                nature,
                oriented,
                type,
                status,
                g.year as year
                        
            FROM  guidance g
                           where 
                           researcher_id='%s'
                           AND g.year>=%s
                           ORDER BY year desc""" % (
        researcher_id,
        year,
    )
    # print(sql)

    reg = sgbdSQL.consultar_db(sql)

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


def list_researchers_originals_words_db(
    terms, institution, type, boolean_condition, graduate_program_id
):
    filter = util.filterSQLRank(terms, ";", "title")

    filter_institution = ""
    filter_institution = util.filterSQL(institution, ";", "or", "i.name")

    filtergraduate_program = ""
    if graduate_program_id != "":
        filtergraduate_program = (
            f"AND gpr.graduate_program_id = '{graduate_program_id}'"
        )

    if type == "ARTICLE":

        filter_type = " AND b.type='ARTICLE' "

        sql = f"""
            SELECT r.id AS id,
                COUNT(DISTINCT b.id) AS qtd,
                r.name AS researcher_name,
                i.name AS institution,
                rp.articles AS articles,
                rp.book_chapters AS book_chapters,
                rp.book AS book,
                r.lattes_id AS lattes,
                r.lattes_10_id AS lattes_10_id,
                abstract,
                rp.great_area AS area,
                rp.city AS city,
                r.orcid AS orcid,
                i.image AS image,
                r.graduation AS graduation,
                rp.patent AS patent,
                rp.software AS software,
                rp.brand AS brand,
                TO_CHAR(r.last_update, 'dd/mm/yyyy') AS lattes_update,
                '{terms}' AS terms
            FROM researcher r
            LEFT JOIN graduate_program_researcher gpr ON r.id = gpr.researcher_id,
                institution i,
                researcher_production rp,
                city c,
                bibliographic_production b
            WHERE c.id = r.city_id
                {filter}
                {filter_institution} 
                {filtergraduate_program} 
                {filter_type}
                AND r.institution_id = i.id
                AND rp.researcher_id = r.id
                AND b.researcher_id = r.id
            GROUP BY r.id,
                    r.name,
                    i.name,
                    articles,
                    book_chapters,
                    book,
                    r.lattes_id,
                    lattes_10_id,
                    abstract,
                    rp.great_area,
                    rp.city,
                    r.orcid,
                    i.image,
                    r.graduation,
                    rp.patent,
                    rp.software,
                    rp.brand,
                    TO_CHAR(r.last_update, 'dd/mm/yyyy')
            ORDER BY qtd DESC;
            """
        reg = sgbdSQL.consultar_db(sql)

    if type == "ABSTRACT":
        filter = util.filterSQLRank2(terms, ";", "abstract")

        sql = f"""
            SELECT 
                DISTINCT r.id AS id,
                0 AS qtd,
                r.name AS researcher_name,
                i.name AS institution,
                rp.articles AS articles,
                rp.book_chapters AS book_chapters,
                rp.book AS book,
                r.lattes_id AS lattes,
                r.lattes_10_id AS lattes_10_id,
                abstract,
                rp.great_area AS area,
                rp.city AS city,
                r.orcid AS orcid,
                i.image AS image,
                r.graduation AS graduation,
                rp.patent AS patent,
                rp.software AS software,
                rp.brand AS brand,
                TO_CHAR(r.last_update, 'dd/mm/yyyy') AS lattes_update,
                '{terms}' AS terms
            FROM 
                researcher r
            LEFT JOIN graduate_program_researcher gpr 
            ON r.id = gpr.researcher_id,
                institution i,
                researcher_production rp,
                city c
            WHERE c.id = r.city_id
                {filter} 
                {filter_institution} 
                {filtergraduate_program}
                AND r.institution_id = i.id
                AND rp.researcher_id = r.id
            ORDER BY qtd DESC;
            """
        reg = sgbdSQL.consultar_db(sql)

    df_bd = pd.DataFrame(
        reg,
        columns=[
            "id",
            "qtd",
            "researcher_name",
            "institution",
            "articles",
            "book_chapters",
            "book",
            "lattes",
            "lattes_10_id",
            "abstract",
            "area",
            "city",
            "orcid",
            "image",
            "graduation",
            "patent",
            "software",
            "brand",
            "lattes_update",
            "terms",
        ],
    )

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
        script_sql = """
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
            {filterQualis}
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

    sql = (
        """
     SELECT distinct r.id as id,
     r.name as researcher_name,i.name as institution,rp.articles as articles,
     rp.book_chapters as book_chapters, rp.book as book, r.lattes_id as lattes,r.lattes_10_id as lattes_10_id,
     r.abstract as abstract,rp.great_area as area,rp.city as city, i.image as image,r.orcid as orcid,
     r.graduation as graduation,rp.patent as patent,rp.software as software,rp.brand as brand,
      TO_CHAR(r.last_update,'dd/mm/yyyy') as lattes_update 
     FROM  researcher r , city c,  institution i, researcher_production rp WHERE 
                          
       c.id=r.city_id
                                       
     AND r.institution_id = i.id 
     AND rp.researcher_id = r.id 
     AND r.id='%s' 
     """
        % researcher_id
    )
    reg = sgbdSQL.consultar_db(sql)

    df_bd = pd.DataFrame(
        reg,
        columns=[
            "id",
            "researcher_name",
            "institution",
            "articles",
            "book_chapters",
            "book",
            "lattes",
            "lattes_10_id",
            "abstract",
            "area",
            "city",
            "image",
            "orcid",
            "graduation",
            "patent",
            "software",
            "brand",
            "lattes_update",
        ],
    )

    return df_bd
