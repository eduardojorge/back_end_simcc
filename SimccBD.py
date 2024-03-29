import pandas as pd
from nltk.tokenize import RegexpTokenizer
import unidecode
import Dao.sgbdSQL as sgbdSQL
import Dao.util as util


def researcher_text_db():
    script_sql = "SELECT id FROM researcher WHERE id='35e6c140-7fbb-4298-b301-c5348725c467' OR id='c0ae713e-57b9-4dc3-b4f0-65e0b2b72ecf'"
    reg = sgbdSQL.consultar_db(script_sql)
    df_bd = pd.DataFrame(reg, columns=["id"])
    researcher_text = list()
    for Index, Data in df_bd.iterrows():
        print(Data.id)
        researcher_text.append(production_db(Data.id))
    return researcher_text


def term_frequency_substring_db():
    script_sql = "SELECT id FROM researcher WHERE id='35e6c140-7fbb-4298-b301-c5348725c467' OR id='c0ae713e-57b9-4dc3-b4f0-65e0b2b72ecf'"
    reg = sgbdSQL.consultar_db(script_sql)
    df_bd = pd.DataFrame(reg, columns=["id"])
    researcher_text = list()
    for Index, Data in df_bd.iterrows():
        print(Data.id)
        researcher_text.append(production_db(Data.id))
    return researcher_text


def production_db(researcher_id):
    script_sql = f"""
        SELECT 
            DISTINCT title, 
            b.type, 
            year
        FROM 
            bibliographic_production AS b, 
            bibliographic_production_author AS ba 
        WHERE 
            b.id = ba.bibliographic_production_id 
            AND researcher_id= '{researcher_id}' 
        GROUP BY 
            title, 
            b.type, 
            year
        """
    reg = sgbdSQL.consultar_db(script_sql)
    df_bd = pd.DataFrame(reg, columns=["title", "b.type", "year"])
    texto = str()

    for Index, infos in df_bd.iterrows():
        tokenize = RegexpTokenizer(r"\w+")

        tokens = list()
        tokens = tokenize.tokenize(infos.title)

        lista = list()
        lista = list(set(tokens))

        for word in lista:
            texto = f"{texto} {word}"

        linha = list()
        linha.append(researcher_id)
        linha.append(texto)

        return linha


def bibliographic_production_total_db():
    scrip_sql = """
        SELECT 
            COUNT(DISTINCT title) as qtd 
        FROM 
            bibliographic_production"""

    reg = sgbdSQL.consultar_db(scrip_sql)
    df_bd = pd.DataFrame(reg, columns=["qtd"])

    return df_bd["qtd"].iloc[0]


def researcher_total_db():
    script_sql = "SELECT COUNT(*) as qtd FROM researcher"
    reg = sgbdSQL.consultar_db(script_sql)
    df_bd = pd.DataFrame(reg, columns=["qtd"])

    return df_bd["qtd"].iloc[0]


def institution_total_db():
    script_sql = "SELECT COUNT(*) as qtd FROM institution "
    reg = sgbdSQL.consultar_db(script_sql)
    df_bd = pd.DataFrame(reg, columns=["qtd"])

    return df_bd["qtd"].iloc[0]


def list_originals_words_initials_term_db(initials):
    initials = unidecode.unidecode(initials)

    script_sql = "SELECT originals_words FROM researcher_term rt WHERE LOWER(unaccent(term)) LIKE '{initials}%'"
    reg = sgbdSQL.consultar_db(script_sql)
    df_bd = pd.DataFrame(reg, columns=["originals_words"])

    text = str()
    for Index, infos in df_bd.iterrows():
        text = text + infos.originals_words

    tokenize = RegexpTokenizer(r"\w+")

    tokens = list()
    tokens = tokenize.tokenize(text)

    lista = list()
    lista = list(set(tokens))

    return lista


def list_sub_area_expertise_initials_term_db(initials):
    initials = unidecode.unidecode(initials)

    scrip_sql = f"SELECT  name as word FROM sub_area_expertise sub  WHERE LOWER(unaccent(name)) LIKE '{initials.lower()}%' AND char_length(unaccent(LOWER(name)))>3 AND to_tsvector('portuguese', unaccent(LOWER(name)))!='' and  unaccent(LOWER(name))!='sobre' "
    reg = sgbdSQL.consultar_db(scrip_sql)
    df_bd = pd.DataFrame(reg, columns=["word"])

    return df_bd


def list_researcher_sub_area_expertise_db(sub_area_experise):
    initials = unidecode.unidecode(sub_area_experise)

    reg = sgbdSQL.consultar_db(
        "SELECT  sub.name as word "
        + " FROM sub_area_expertise sub,researcher_area_expertise rae "
        + " WHERE "
        + "  LOWER(unaccent(sub.name))='"
        + sub_area_experise.lower()
        + "'"
        + " AND sub.id = rae.sub_area_expertise_id"
    )

    df_bd = pd.DataFrame(reg, columns=["researcher_id"])

    return df_bd


def list_area_expertise_term_db(term):

    term = unidecode.unidecode(term)

    script_sql = (
        "SELECT (a.name) AS area_expertise_, a.id AS id FROM area_expertise a WHERE unaccent(LOWER(a.name)) LIKE '"
        + term.lower()
        + "%' ORDER BY a.name"
    )
    reg = sgbdSQL.consultar_db(script_sql)
    df_bd = pd.DataFrame(reg, columns=["area_expertise_", "id"])

    return df_bd


def list_researcher_area_expertise_term_db(term):
    term = unidecode.unidecode(term)

    script_sql = (
        "SELECT unaccent((a.name || ' | ' || b.name)) AS area_expertise, r.name FROM area_expertise a, area_expertise b, researcher r, researcher_area_expertise rae WHERE a.id = b.parent_id AND r.id = rae.researcher_id AND rae.area_expertise_id = b.id AND a.parent_id IS NOT NULL AND (translate(unaccent(LOWER((a.name || ' ' || b.name))),':','')::tsvector @@ '"
        + term
        + "'::tsquery)=true"
    )
    reg = sgbdSQL.consultar_db(script_sql)
    df_bd = pd.DataFrame(reg, columns=["area_expertise", "r.name"])

    return df_bd


def lista_researcher_name_db(text):
    tokenize = RegexpTokenizer(r"\w+")
    tokens = list()
    tokens = tokenize.tokenize(text)
    term = list()
    for word in tokens:
        term = term + word + " & "
    x = len(term)
    termX = term[0 : x - 3]
    print(termX)

    script_sql = (
        "SELECT rf.researcher_id as id,COUNT(rf.term) AS qtd,"
        + "r.name as researcher_name,i.name as institution,rp.articles as articles,rp.book_chapters as book_chapters,rp.book as book, r.lattes_id as lattes"
        + " FROM researcher_frequency rf, researcher r , institution i, researcher_production rp "
        + " WHERE "
        + " rf.researcher_id = r.id"
        + " AND r.institution_id = i.id "
        + " AND rp.researcher_id = r.id "
        + " AND (translate(unaccent(LOWER(r.name)),':','') ::tsvector@@ '"
        + unidecode.unidecode(text)
        + "'::tsquery)=true"
        + " GROUP BY rf.researcher_id,r.name, i.name,articles, book_chapters,book,r.lattes_id"
        + " ORDER BY qtd desc"
    )
    reg = sgbdSQL.consultar_db(script_sql)
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
        ],
    )

    return df_bd


def lista_researcher_full_name_db_(name, graduate_program_id):

    filter_graduate_program = str()
    if graduate_program_id != "":
        filter_graduate_program = (
            f"AND gpr.graduate_program_id = '{graduate_program_id}'"
        )
    filter_name = str()
    if name != "":
        filter_name = f"AND to_tsvector('portuguese', unaccent(r.name)) @@ to_tsquery('portuguese', unaccent('{name.replace(';', '&')}'))"

    script_sql = f"""SELECT 
            DISTINCT r.id AS id,
            r.name AS researcher_name,
            i.name AS institution,
            rp.articles AS articles,
            rp.book_chapters AS book_chapters,
            rp.book AS book,
            r.lattes_id AS lattes,
            r.lattes_10_id AS lattes_10_id,
            r.abstract AS abstract,
            rp.great_area AS area,
            rp.city AS city,
            i.image AS image,
            r.orcid AS orcid,
            r.graduation AS graduation,
            rp.patent AS patent,
            rp.software AS software,
            rp.brand AS brand,
            to_char(r.last_update,'dd/mm/yyyy') AS lattes_update
        FROM 
            researcher r
        LEFT JOIN graduate_program_researcher gpr
        ON r.id = gpr.researcher_id, city c, institution i, researcher_production rp
        WHERE 
            c.id = r.city_id
            AND r.institution_id = i.id
            AND rp.researcher_id = r.id 
            {filter_name} 
            {filter_graduate_program}"""

    reg = sgbdSQL.consultar_db(script_sql)

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


def lists_researcher_initials_term_db(initials, graduate_program_id):

    initials = unidecode.unidecode(initials)
    filter = util.filterSQLRank2(initials, ";", "r.name")
    filtergraduate_program = ""
    if graduate_program_id != "":
        filtergraduate_program = "AND gpr.graduate_program_id=" + graduate_program_id

    sql = """SELECT distinct id, name as nome FROM PUBLIC.researcher r LEFT JOIN graduate_program_researcher gpr ON  r.id =gpr.researcher_id 
    WHERE   r.institution_id is NOT null %s %s 
    order by nome """ % (
        filter,
        filtergraduate_program,
    )
    reg = sgbdSQL.consultar_db(sql)
    df_bd = pd.DataFrame(reg, columns=["id", "nome"])

    return df_bd


def lists_bibliographic_production_article_db(
    term, year, qualis, institution, distinct, graduate_program_id
):

    filter_institution = util.filterSQL(institution, ";", "or", "i.name")

    term = unidecode.unidecode(term.lower())
    filter_term = util.filterSQLRank(term, ";", "title")

    filter_qualis = util.filterSQL(qualis, ";", "or", "qualis")

    filter_graduate_program = ""
    if not graduate_program_id or graduate_program_id == 0:
        filter_graduate_program = "AND gpr.graduate_program_id=" + graduate_program_id

    if distinct == "1":
        script_sql = f"""
            SELECT DISTINCT title,
                 r.id AS researcher_id,
                 year_,
                 doi,
                 qualis,
                 periodical_magazine_name AS magazine,
                 a.jcr,
                 a.jcr_link
            FROM institution i,
                 public.bibliographic_production b,
                 bibliographic_production_article a,
                 researcher r
            LEFT JOIN graduate_program_researcher gpr
              ON r.id = gpr.researcher_id
           WHERE r.id = b.researcher_id
             AND a.bibliographic_production_id = b.id
             AND i.id = r.institution_id 
             {filter_term} 
             {filter_institution} 
             {filter_graduate_program} 
             {filter_qualis}
             AND year_ >= {year}
             AND b.type = 'ARTICLE'
           ORDER BY year_ DESC
           """
        print(script_sql)
        reg = sgbdSQL.consultar_db(script_sql)

        data_frame = pd.DataFrame(
            reg,
            columns=[
                "title",
                "researcher_id",
                "year",
                "doi",
                "qualis",
                "magazine",
                "jcr",
                "jcr_link",
            ],
        )
        return data_frame

    if distinct == "0":
        script_sql = f"""
            SELECT DISTINCT title,
                r.id AS researcher_id,
                year_,
                doi,
                a.qualis as qualis,
                periodical_magazine_name AS magazine,
                r.name AS researcher,
                r.lattes_10_id AS lattes_10_id,
                r.lattes_id AS lattes_id,
                a.jcr,
                a.jcr_link
            FROM institution i,
                public.bibliographic_production b,
                bibliographic_production_article a,
                researcher r
            LEFT JOIN graduate_program_researcher gpr
            ON r.id = gpr.researcher_id
            WHERE r.id = b.researcher_id
            AND a.bibliographic_production_id = b.id
            AND i.id = r.institution_id {filter_term} {filter_institution} {filter_graduate_program} {filter_qualis}
            AND year_ >= {year}
            AND b.type = 'ARTICLE'
            ORDER BY year_ DESC
            """

        reg = sgbdSQL.consultar_db(script_sql)

        data_frame = pd.DataFrame(
            reg,
            columns=[
                "title",
                "researcher_id",
                "year",
                "doi",
                "qualis",
                "magazine",
                "researcher",
                "lattes_10_id",
                "lattes_id",
                "jcr",
                "jcr_link",
            ],
        )
        return data_frame


def lists_bibliographic_production_article_name_researcher_db(name, year, qualis):

    name = unidecode.unidecode(name.lower())
    t = []
    t = name.split(";")
    filter = ""
    i = 0
    for word in t:
        filter = "unaccent(LOWER(r.name))='" + word.lower() + "' or " + filter
        i = i + 1
    x = len(filter)
    filter = filter[0 : x - 3]
    filter = "(" + filter + ")"
    if qualis == "":
        filterQualis = ""
    else:
        t = []
        t = qualis.split(";")
        filterQualis = ""
        i = 0
        for word in t:
            filterQualis = (
                "unaccent(LOWER(qualis))='" + word.lower() + "' or " + filterQualis
            )
            i = i + 1
        x = len(filterQualis)
        filterQualis = filterQualis[0 : x - 3]
        filterQualis = " AND (" + filterQualis + ")"

    reg = sgbdSQL.consultar_db(
        " SELECT distinct bp.id as id,title,year,doi,qualis,r.name as researcher, m.name as magazine"
        + " FROM researcher r, PUBLIC.bibliographic_production bp, bibliographic_production_article a,periodical_magazine m "
        + "  WHERE m.id = a.periodical_magazine_id "
        + " AND bp.researcher_id = r.id "
        + "   AND a.bibliographic_production_id = bp.id "
        + " AND"
        + filter
        + "  AND year_ >=%s" % year
        + filterQualis
        + "  AND  bp.type = 'ARTICLE' "
    )

    df_bd = pd.DataFrame(
        reg, columns=["id", "title", "year", "doi", "qualis", "researcher", "magazine"]
    )
    print(df_bd)
    return df_bd


def lista_researcher_full_name_db():
    reg = sgbdSQL.consultar_db(
        "SELECT distinct r.id as id,"
        + " r.lattes_id as lattes,r.lattes_10_id as lattes_10_id"
        + " FROM  researcher r  "
    )
    df_bd = pd.DataFrame(reg, columns=["id", "lattes", "lattes_10_id"])
    df_shuffled = df_bd.sample(frac=1)

    return df_shuffled
