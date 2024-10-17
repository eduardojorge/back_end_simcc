import pandas as pd
import unidecode
from nltk.tokenize import RegexpTokenizer

import Dao.sgbdSQL as sgbdSQL
import Dao.util as util


def list_productivity_research():
    script_sql = """
        SELECT 
            s.researcher_id,
            r.name,
            s.modality_code, 
            s.modality_name, 
            s.call_title, 
            s.category_level_code, 
            s.funding_program_name, 
            s.institute_name, 
            s.aid_quantity, 
            s.scholarship_quantity
        FROM 
            foment s
            LEFT JOIN researcher r ON s.researcher_id = r.id
    """
    registry = sgbdSQL.consultar_db(script_sql)
    data_frame = pd.DataFrame(
        registry,
        columns=[
            "researcher_id",
            "name",
            "modality_code",
            "modality_name",
            "call_title",
            "category_level_code",
            "funding_program_name",
            "institute_name",
            "aid_quantity",
            "scholarship_quantity",
        ],
    )

    return data_frame.to_dict(orient="records")


def list_count_researcher_groups():
    script_sql = """
        SELECT 
            area,
            COUNT(*)
        FROM 
            public.research_group 
        GROUP BY area;
        """
    registry = sgbdSQL.consultar_db(script_sql)

    data_frame = pd.DataFrame(registry, columns=["area", "count"])

    return data_frame.to_dict(orient="records")


def lists_research_groups(group_id):
    if group_id:
        group_id_filter = f"""
            AND rg.id = '{group_id}'
            """
    else:
        group_id_filter = str()

    script_sql = f"""
        SELECT
            rg.id,
            name,
            institution,
            first_leader,
            first_leader_id,
            second_leader,
            second_leader_id,
            area,
            census,
            start_of_collection,
            end_of_collection,
            group_identifier,
            year,
            institution_name,
            category
        FROM
            research_group rg
        WHERE
            (rg.first_leader_id IS NOT NULL OR rg.second_leader_id IS NOT NULL)
            {group_id_filter}
        """

    registry = sgbdSQL.consultar_db(script_sql)
    data_frame = pd.DataFrame(
        registry,
        columns=[
            "id",
            "name",
            "institution",
            "first_leader",
            "first_leader_id",
            "second_leader",
            "second_leader_id",
            "area",
            "census",
            "start_of_collection",
            "end_of_collection",
            "group_identifier",
            "year",
            "institution_name",
            "category",
        ],
    )

    return data_frame.fillna("").to_dict(orient="records")


def list_research_lines(group_id):
    script_sql = f"""
        SELECT
            JSONB_AGG(jsonb_build_object(
                'line', rl.title,
                'objective', rl.objective,
                'keywords', rl.keyword,
                'major_area', rl.predominant_major_area,
                'area', rl.predominant_area,
                'year', rl.year
            ))
        FROM 
            research_lines rl
        WHERE
            rl.research_group_id = '{group_id}'
        GROUP BY
            rl.research_group_id
        """

    registry = sgbdSQL.consultar_db(script_sql)

    return registry[0][0]


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

    scrip_sql = f"""
        SELECT
            name as word
        FROM
            sub_area_expertise sub
        WHERE
            LOWER(unaccent(name)) LIKE '{initials.lower()}%'
            AND char_length(unaccent(LOWER(name))) > 3
            AND to_tsvector('portuguese', unaccent(LOWER(name)))!=''
            AND  unaccent(LOWER(name))!='sobre'"""
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
        + initials.lower()
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


def recently_updated_db(year, institution, departament):
    filter_institution = str()
    if not institution:
        filter_institution = util.filterSQL(institution, ";", "or", "i.name")

    script_sql = f"""
        SELECT
            DISTINCT title,
            op.article_institution as article_institution,
            array_cat(string_to_array(op.issn, ','), string_to_array(a.issn, ',')) AS issn,
            op.authors_institution as authors_institution,
            op.abstract as abstract,
            op.authors as authors,
            op.language as language,
            op.citations_count as citations_count,
            op.pdf as pdf,
            op.landing_page_url as landing_page_url,
            op.keywords as keywords,
            r.id AS researcher_id,
            year_,
            doi,
            a.qualis AS qualis,
            periodical_magazine_name AS magazine,
            r.name AS researcher,
            r.lattes_10_id AS lattes_10_id,
            r.lattes_id AS lattes_id,
            a.jcr,
            a.jcr_link,
            b.created_at
        FROM
            public.bibliographic_production b
            LEFT JOIN bibliographic_production_article a ON a.bibliographic_production_id = b.id
            LEFT JOIN researcher r ON r.id = b.researcher_id
            LEFT JOIN institution i ON i.id = r.institution_id
            LEFT JOIN graduate_program_researcher gpr ON r.id = gpr.researcher_id
            LEFT JOIN openalex_article op ON op.article_id = b.id
        WHERE
            year_ >= {year}
            AND b.type = 'ARTICLE'
            {filter_institution}
        ORDER BY
            year_ DESC,
            created_at DESC
        LIMIT 100;
    """

    registry = sgbdSQL.consultar_db(script_sql)
    data_frame = pd.DataFrame(
        registry,
        columns=[
            "title",
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
            "created_at",
        ],
    )

    if departament:
        script_sql = f"""
            SELECT
                researcher_id
            FROM
                public.departament_researcher
            WHERE dep_id = '{departament}'
            """
        registry = sgbdSQL.consultar_db(script_sql)

        data_frame_researchers = pd.DataFrame(registry, columns=["researcher_id"])

        data_frame = pd.merge(
            data_frame, data_frame_researchers, on="researcher_id", how="inner"
        )
    return data_frame.fillna("")


def lists_bibliographic_production_article_db(
    term, year, qualis, institution, distinct, graduate_program_id, dep_id
):
    filter_institution = str()
    if institution:
        filter_institution = util.filterSQL(institution, ";", "or", "i.name")

    if term:
        filter_term = f"""AND {util.web_search_filter(term, "title")}"""
    else:
        filter_term = str()

    filter_qualis = util.filterSQL(qualis, ";", "or", "qualis")

    filter_graduate_program = str()
    if graduate_program_id and graduate_program_id != "0":
        filter_graduate_program = f"""
            AND gpr.graduate_program_id = '{graduate_program_id}'
            """

    if dep_id:
        filter_departament = f"""
            AND r.id IN (
                SELECT researcher_id
                FROM public.departament_researcher
                WHERE dep_id = '{dep_id}'
            )
            """
    else:
        filter_departament = str()

    if distinct == "1":
        script_sql = f"""
            SELECT DISTINCT
                b.title,
                b.year_,
                b.doi,
                qualis,
                periodical_magazine_name AS magazine,
                a.jcr,
                a.jcr_link,
                op.article_institution,
                op.issn,
                op.authors_institution,
                op.abstract,
                op.authors,
                op.language,
                op.citations_count,
                op.pdf,
                op.landing_page_url,
                op.keywords
            FROM
                public.bibliographic_production b
                INNER JOIN researcher r ON r.id = b.researcher_id
                INNER JOIN institution i ON i.id = r.institution_id
                INNER JOIN bibliographic_production_article a ON a.bibliographic_production_id = b.id
                LEFT JOIN graduate_program_researcher gpr ON r.id = gpr.researcher_id
                LEFT JOIN openalex_article op ON op.article_id = b.id
            WHERE
                b.year_ >= {year}
                AND b.type = 'ARTICLE'
                {filter_term}
                {filter_institution}
                {filter_graduate_program}
                {filter_qualis}
                {filter_departament}
            ORDER BY
                b.year_ DESC;
                """

        reg = sgbdSQL.consultar_db(script_sql)

        data_frame = pd.DataFrame(
            reg,
            columns=[
                "title",
                "year",
                "doi",
                "qualis",
                "magazine",
                "jcr",
                "jcr_link",
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
            ],
        )

    elif distinct == "0":
        script_sql = f"""
            SELECT
                title,
                r.id,
                year_,
                doi,
                qualis,
                periodical_magazine_name AS magazine,
                r.name,
                r.lattes_id AS lattes_id,
                a.jcr,
                a.jcr_link,
                op.article_institution,
                op.issn,
                op.authors_institution,
                op.abstract,
                op.authors,
                op.language,
                op.citations_count,
                op.pdf,
                op.landing_page_url,
                op.keywords
            FROM
                public.bibliographic_production b
                LEFT JOIN researcher r ON r.id = b.researcher_id
                LEFT JOIN institution i ON i.id = r.institution_id
                LEFT JOIN bibliographic_production_article a ON a.bibliographic_production_id = b.id
                LEFT JOIN graduate_program_researcher gpr ON r.id = gpr.researcher_id
                LEFT JOIN openalex_article op ON op.article_id = b.id
            WHERE
                year_ >= {year}
                {filter_term}
                {filter_institution}
                {filter_graduate_program}
                {filter_qualis}
                {filter_departament}
                AND b.type = 'ARTICLE'
            ORDER BY
                year_ DESC;
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
                "lattes_id",
                "jcr",
                "jcr_link",
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
            ],
        )
    return data_frame.fillna("")


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
