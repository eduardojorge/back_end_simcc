import Dao.sgbdSQL as sgbdSQL
import pandas as pd


def researcher_search_city(city_id: str = None):
    if city_id == None:
        script_sql = """
            SELECT DISTINCT
                r.id AS researcher_id,
                r.name AS researcher_name,
                i.name AS institution,
                i.image AS image,
                c.name AS city,
                UPPER(REPLACE(LOWER(TRIM(rp.great_area)), '_', ' ')) AS area,
            FROM
                researcher r
            LEFT JOIN
                graduate_program_researcher gpr ON r.id = gpr.researcher_id
            JOIN
                city c ON c.id = r.city_id
            JOIN
                institution i ON r.institution_id = i.id
            JOIN
                researcher_production rp ON rp.researcher_id = r.id
            """

        registry = sgbdSQL.consultar_db(sql=script_sql)
        data_frame = pd.DataFrame(
            registry,
            columns=[
                "id",
                "researcher_name",
                "institution",
                "image",
                "city",
                "area",
            ],
        )
        return data_frame

    else:
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

        data_frame = data_frame.merge(
            researcher_graduate_program_db(), on="id", how="left"
        )
        data_frame = data_frame.merge(
            researcher_research_group_db(), on="id", how="left"
        )
        data_frame = data_frame.merge(researcher_openAlex_db(), on="id", how="left")
        data_frame = data_frame.merge(researcher_foment_db(), on="id", how="left")

        return data_frame.fillna("").to_dict(orient="records")


def generic_data(year_, graduate_program_id, dep_id):
    if graduate_program_id:
        filter_graduate_program = f"""
            AND researcher_id IN (SELECT researcher_id FROM graduate_program_researcher WHERE graduate_program_id = '{graduate_program_id}')
            """
    else:
        filter_graduate_program = str()
    if dep_id:
        filter_departament = f"""
            AND researcher_id IN (SELECT researcher_id FROM public.departament_researcher WHERE dep_id = '{dep_id}')
            """
    else:
        filter_departament = str()

    year = list(range(int(year_), 2025))

    data_frame = pd.DataFrame(year, columns=["year"])

    script_sql = f"""
        SELECT
            g.year,
            COUNT(*) as count_guidance,
            COUNT(CASE WHEN g.status = 'Concluída' THEN 1 ELSE NULL END) as count_concluido,
            COUNT(CASE WHEN g.status = 'Em andamento' THEN 1 ELSE NULL END) as count_andamento
        FROM
            guidance g
        WHERE
            g.year >= {year_}
            {filter_graduate_program}
            {filter_departament}
        GROUP BY
            g.year
        ORDER BY
            g.year;
        """

    registre = sgbdSQL.consultar_db(script_sql)

    df = pd.DataFrame(
        registre,
        columns=[
            "year",
            "count_guidance",
            "count_guidance_complete",
            "count_guidance_in_progress",
        ],
    )

    if not df.empty:
        data_frame = pd.merge(data_frame, df, on="year", how="left")
    else:
        data_frame["count_guidance"] = None
        data_frame["count_guidance_complete"] = None
        data_frame["count_guidance_in_progress"] = None

    script_sql = f"""
        SELECT
            bp.year,
            COUNT(DISTINCT title) AS count_book
        FROM
            public.bibliographic_production bp
        WHERE
            type = 'BOOK'
            AND bp.year::smallint >= {year_}
            {filter_graduate_program}
            {filter_departament}
        GROUP BY
            bp.year
        ORDER BY
            bp.year;
        """

    registre = sgbdSQL.consultar_db(script_sql)

    df = pd.DataFrame(registre, columns=["year", "count_book"])

    df["year"] = df["year"].astype("int64")
    if not df.empty:
        data_frame = pd.merge(data_frame, df, on="year", how="left")
    else:
        data_frame["count_book"] = None

    script_sql = f"""
        SELECT
            bp.year,
            COUNT(DISTINCT title) AS count_book_chapter
        FROM
            public.bibliographic_production bp
        WHERE
            type = 'BOOK_CHAPTER'
            AND bp.year::smallint >= {year_}
            {filter_graduate_program}
            {filter_departament}
        GROUP BY
            bp.year
        ORDER BY
            bp.year;
        """
    registre = sgbdSQL.consultar_db(script_sql)

    df = pd.DataFrame(registre, columns=["year", "count_book_chapter"])

    df["year"] = df["year"].astype("int64")
    if not df.empty:
        data_frame = pd.merge(data_frame, df, on="year", how="left")
    else:
        data_frame["count_book_chapter"] = None

    script_sql = f"""
        SELECT
            p.development_year,
            COUNT(CASE WHEN p.grant_date IS NULL THEN 1 ELSE NULL END) count_not_granted_patent,
            COUNT(CASE WHEN p.grant_date IS NOT NULL THEN 1 ELSE NULL END) as count_granted_patent,
            COUNT(*) as count_total
        FROM
            patent p
        WHERE
            p.development_year::smallint >= {year_}
            {filter_graduate_program}
            {filter_departament}
        GROUP BY
            p.development_year
        ORDER BY
            p.development_year;
        """

    registre = sgbdSQL.consultar_db(script_sql)

    df = pd.DataFrame(
        registre,
        columns=[
            "year",
            "count_patent",
            "count_patent_granted",
            "count_patent_not_granted",
        ],
    )

    df["year"] = df["year"].astype("int64")
    if not df.empty:
        data_frame = pd.merge(data_frame, df, on="year", how="left")
    else:
        data_frame["count_patent"] = None

    script_sql = f"""
        SELECT
            sw.year,
            COUNT(DISTINCT title) as count_software
        FROM
            public.software sw
        WHERE
            sw.year::smallint >= {year_}
            {filter_graduate_program}
            {filter_departament}
        GROUP BY
            sw.year
        """

    registre = sgbdSQL.consultar_db(script_sql)

    df = pd.DataFrame(registre, columns=["year", "count_software"])

    df["year"] = df["year"].astype("int64")
    if not df.empty:
        data_frame = pd.merge(data_frame, df, on="year", how="left")
    else:
        data_frame["count_software"] = None

    script_sql = f"""
        SELECT
            rr.year,
            COUNT(DISTINCT title) as count_report
        FROM
            research_report rr
        WHERE
            rr.year::smallint >= {year_}
            {filter_graduate_program}
            {filter_departament}
        GROUP BY
            rr.year
        ORDER BY
            rr.year
        """

    registre = sgbdSQL.consultar_db(script_sql)

    df = pd.DataFrame(registre, columns=["year", "count_report"])

    df["year"] = df["year"].astype("int64")
    if not df.empty:
        data_frame = pd.merge(data_frame, df, on="year", how="left")
    else:
        data_frame["count_report"] = None

    script_sql = f"""
        SELECT
            bpa.qualis,
            bp.year,
            COUNT(DISTINCT title) AS count_article
        FROM
            public.bibliographic_production bp
            RIGHT JOIN bibliographic_production_article bpa ON bpa.bibliographic_production_id = bp.id
        WHERE
            type = 'ARTICLE'
            AND bp.year::SMALLINT >= {year_}
            {filter_graduate_program}
            {filter_departament}
        GROUP BY
            bpa.qualis,
            bp.year
        ORDER BY
            bpa.qualis,
            bp.year;
        """
    registre = sgbdSQL.consultar_db(script_sql)

    df = pd.DataFrame(registre, columns=["qualis", "year", "count_article"])
    df = df.pivot_table(
        index="year", columns="qualis", values="count_article", fill_value=0
    )
    df["count_article"] = df.sum(axis=1)
    df.reset_index(inplace=True)

    df["year"] = df["year"].astype("int64")
    qualis_list = ["A1", "A2", "A3", "A4", "B1", "B2", "B3", "B4", "C", "SQ"]

    for qualis in qualis_list:
        if qualis not in df.columns:
            df[qualis] = 0.0

    if not df.empty:
        data_frame = pd.merge(data_frame, df, on="year", how="left")
    else:
        data_frame["count_article"] = None

    script_sql = f"""
        SELECT
            br.year,
            COUNT(DISTINCT br.title) AS count_brand
        FROM brand br
            WHERE br.year::smallint >= {year_}
            {filter_graduate_program}
            {filter_departament}
        GROUP BY
            br.year
    """
    registre = sgbdSQL.consultar_db(script_sql)

    df = pd.DataFrame(registre, columns=["year", "count_brand"])

    df["year"] = df["year"].astype("int64")
    if not df.empty:
        data_frame = pd.merge(data_frame, df, on="year", how="left")
    else:
        data_frame["count_brand"] = None

    return data_frame.fillna(0).to_dict(orient="records")


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
            rg.first_leader_id AS leader_id,
            jsonb_agg(
                jsonb_build_object(
                    'research_group_id', rg.id,
                    'name', rg.name
                )
            ) AS research_groups
        FROM 
            public.research_group rg
        GROUP BY
            rg.first_leader_id

        UNION ALL

        SELECT
            rg.second_leader_id AS leader_id,
            jsonb_agg(
                jsonb_build_object(
                    'research_group_id', rg.id,
                    'name', rg.name
                )
            ) AS research_groups
        FROM 
            public.research_group rg
        GROUP BY
            rg.second_leader_id;
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


def generic_researcher_data_data(year_, researcher_id):
    year = list(range(int(year_), 2025))

    data_frame = pd.DataFrame(year, columns=["year"])

    script_sql = f"""
        SELECT
            g.year,
            COUNT(*) as count_guidance,
            COUNT(CASE WHEN g.status = 'Concluída' THEN 1 ELSE NULL END) as count_concluido,
            COUNT(CASE WHEN g.status = 'Em andamento' THEN 1 ELSE NULL END) as count_andamento
        FROM
            guidance g
        WHERE
            g.year >= {year_}
            AND g.researcher_id = '{researcher_id}'
        GROUP BY
            g.year
        ORDER BY
            g.year;
        """

    registre = sgbdSQL.consultar_db(script_sql)

    df = pd.DataFrame(
        registre,
        columns=[
            "year",
            "count_guidance",
            "count_guidance_complete",
            "count_guidance_in_progress",
        ],
    )

    if not df.empty:
        data_frame = pd.merge(data_frame, df, on="year", how="left")
    else:
        data_frame["count_guidance"] = None
        data_frame["count_guidance_complete"] = None
        data_frame["count_guidance_in_progress"] = None

    script_sql = f"""
        SELECT
            bp.year,
            COUNT(DISTINCT title) AS count_book
        FROM
            public.bibliographic_production bp
        WHERE
            type = 'BOOK'
            AND bp.year::smallint >= {year_}
            AND bp.researcher_id = '{researcher_id}'
        GROUP BY
            bp.year
        ORDER BY
            bp.year;
        """

    registre = sgbdSQL.consultar_db(script_sql)

    df = pd.DataFrame(registre, columns=["year", "count_book"])

    df["year"] = df["year"].astype("int64")
    if not df.empty:
        data_frame = pd.merge(data_frame, df, on="year", how="left")
    else:
        data_frame["count_book"] = None

    script_sql = f"""
        SELECT
            bp.year,
            COUNT(DISTINCT title) AS count_book_chapter
        FROM
            public.bibliographic_production bp
        WHERE
            type = 'BOOK_CHAPTER'
            AND bp.year::smallint >= {year_}
            AND bp.researcher_id = '{researcher_id}'
        GROUP BY
            bp.year
        ORDER BY
            bp.year;
        """
    registre = sgbdSQL.consultar_db(script_sql)

    df = pd.DataFrame(registre, columns=["year", "count_book_chapter"])

    df["year"] = df["year"].astype("int64")
    if not df.empty:
        data_frame = pd.merge(data_frame, df, on="year", how="left")
    else:
        data_frame["count_book_chapter"] = None

    script_sql = f"""
        SELECT
            p.development_year,
            COUNT(CASE WHEN p.grant_date IS NULL THEN 1 ELSE NULL END) count_not_granted_patent,
            COUNT(CASE WHEN p.grant_date IS NOT NULL THEN 1 ELSE NULL END) as count_granted_patent,
            COUNT(*) as count_total
        FROM
            patent p
        WHERE
            p.development_year::smallint >= {year_}
            AND p.researcher_id = '{researcher_id}'
        GROUP BY
            p.development_year
        ORDER BY
            p.development_year;
        """

    registre = sgbdSQL.consultar_db(script_sql)

    df = pd.DataFrame(
        registre,
        columns=[
            "year",
            "count_patent",
            "count_patent_granted",
            "count_patent_not_granted",
        ],
    )

    df["year"] = df["year"].astype("int64")
    if not df.empty:
        data_frame = pd.merge(data_frame, df, on="year", how="left")
    else:
        data_frame["count_patent"] = None

    script_sql = f"""
        SELECT
            sw.year,
            COUNT(DISTINCT title) as count_software
        FROM
            public.software sw
        WHERE
            sw.year::smallint >= {year_}
            AND sw.researcher_id = '{researcher_id}'
        GROUP BY
            sw.year
        """

    registre = sgbdSQL.consultar_db(script_sql)

    df = pd.DataFrame(registre, columns=["year", "count_software"])

    df["year"] = df["year"].astype("int64")
    if not df.empty:
        data_frame = pd.merge(data_frame, df, on="year", how="left")
    else:
        data_frame["count_software"] = None

    script_sql = f"""
        SELECT
            rr.year,
            COUNT(DISTINCT title) as count_report
        FROM
            research_report rr
        WHERE
            rr.year::smallint >= {year_}
            AND rr.researcher_id = '{researcher_id}'
        GROUP BY
            rr.year
        ORDER BY
            rr.year
        """

    registre = sgbdSQL.consultar_db(script_sql)

    df = pd.DataFrame(registre, columns=["year", "count_report"])

    df["year"] = df["year"].astype("int64")
    if not df.empty:
        data_frame = pd.merge(data_frame, df, on="year", how="left")
    else:
        data_frame["count_report"] = None

    script_sql = f"""
        SELECT
            bpa.qualis,
            bp.year,
            COUNT(DISTINCT title) AS count_article
        FROM
            public.bibliographic_production bp
            RIGHT JOIN bibliographic_production_article bpa ON bpa.bibliographic_production_id = bp.id
        WHERE
            type = 'ARTICLE'
            AND bp.year::SMALLINT >= {year_}
            AND bp.researcher_id = '{researcher_id}'
        GROUP BY
            bpa.qualis,
            bp.year
        ORDER BY
            bpa.qualis,
            bp.year;
        """
    registre = sgbdSQL.consultar_db(script_sql)

    df = pd.DataFrame(registre, columns=["qualis", "year", "count_article"])

    df = df.pivot_table(
        index="year", columns="qualis", values="count_article", fill_value=0
    )
    df["count_article"] = df.sum(axis=1)
    df.reset_index(inplace=True)

    df["year"] = df["year"].astype("int64")
    qualis_list = ["A1", "A2", "A3", "A4", "B1", "B2", "B3", "B4", "C", "SQ"]

    for qualis in qualis_list:
        if qualis not in df.columns:
            df[qualis] = 0.0

    if not df.empty:
        data_frame = pd.merge(data_frame, df, on="year", how="left")
    else:
        data_frame["count_article"] = None

    script_sql = f"""
        SELECT
            br.year,
            COUNT(DISTINCT br.title) AS count_brand
        FROM brand br
            WHERE br.year::smallint >= {year_}
            AND br.researcher_id = '{researcher_id}'
        GROUP BY
            br.year
    """
    registre = sgbdSQL.consultar_db(script_sql)

    df = pd.DataFrame(registre, columns=["year", "count_brand"])

    df["year"] = df["year"].astype("int64")
    if not df.empty:
        data_frame = pd.merge(data_frame, df, on="year", how="left")
    else:
        data_frame["count_brand"] = None

    return data_frame.fillna(0).to_dict(orient="records")


def researcher_query_grant(institution_id):
    filter_institution = str()
    if institution_id:
        filter_institution = f"""
                AND r.institution_id = '{institution_id}'
                """

    SCRIPT_SQL = f"""
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
        WHERE
        s.researcher_id IS NOT NULL
        AND researcher_id NOT IN (SELECT id FROM researcher WHERE docente = false)
        {filter_institution}
        """

    registry = sgbdSQL.consultar_db(SCRIPT_SQL)

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
