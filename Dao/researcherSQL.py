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
        data_frame = data_frame.merge(researcher_subsidy_db(), on="id", how="left")

        return data_frame.fillna("").to_dict(orient="records")


def researcher_data_geral(year_):
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
        GROUP BY
            bpa.qualis,
            bp.year
        ORDER BY
            bpa.qualis,
            bp.year;
        """
    registre = sgbdSQL.consultar_db(script_sql)

    df = pd.DataFrame(registre, columns=["qualis", "year", "count_article"])

    df["year"] = df["year"].astype("int64")
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
