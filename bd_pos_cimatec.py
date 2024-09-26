import Dao.sgbdSQL as sgbdSQL
import pandas as pd


def insert_researcher_graduate_program_db(table, graduate_program_id, year):
    script_sql = f"""
        SELECT
            r.id,
            gdp.type_
        FROM
            {table} gdp,
            researcher r
        WHERE
            similarity(unaccent(LOWER(gdp.name)),
                       unaccent(LOWER(r.name))) > 0.8;
        """

    registry = sgbdSQL.consultar_db(script_sql)

    data_frame_bd = pd.DataFrame(registry, columns=["id", "type_"])

    for Index, infos in data_frame_bd.iterrows():
        researcher_id = infos.id
        type_ = infos.type_

        script_sql = f"""
        INSERT INTO public.graduate_program_researcher (researcher_id, graduate_program_id, year, type_)
        VALUES ('{researcher_id}', '{graduate_program_id}',
                '{year}', '{type_}');
        """

        sgbdSQL.execScript_db(script_sql)


def graduate_program_csv_db():
    script_sql = """
        SELECT graduate_program_id, code, name, area, modality, type, rating
        FROM graduate_program gp;
        """

    registry = sgbdSQL.consultar_db(script_sql)

    data_frame_bd = pd.DataFrame(
        registry,
        columns=[
            "graduate_program_id",
            "code",
            "name",
            "area",
            "modality",
            "type",
            "rating",
        ],
    )

    data_frame_bd.to_csv("Files/cimatec_graduate_program.csv")


def graduate_program_researcher_csv_db():
    script_sql = """
        SELECT researcher_id, graduate_program_id, year, type_
        FROM graduate_program_researcher;
        """

    registry = sgbdSQL.consultar_db(script_sql)

    data_frame_bd = pd.DataFrame(
        registry, columns=["researcher_id", "graduate_program_id", "year", "type_"]
    )

    data_frame_bd.to_csv("Files/cimatec_graduate_program_researcher.csv")


def graduate_program_student_researcher_csv_db():
    script_sql = """
        SELECT researcher_id, graduate_program_id, 2024
        FROM graduate_program_student
        """

    registry = sgbdSQL.consultar_db(script_sql)

    data_frame_db = pd.DataFrame(
        registry, columns=["researcher_id", "graduate_program_id", "year"]
    )

    data_frame_db.to_csv("Files/indicadores_simcc/cimatec_graduate_program_student.csv")


def cimatec_researcher_production_year_distinct_csv_db():
    script_sql = """
        SELECT DISTINCT
            title,
            b.type AS tipo,
            b.year AS year,
            gp.graduate_program_id AS graduate_program_id,
            gpr.year AS year_pos,
            bpa.qualis AS qualis
        FROM
            bibliographic_production AS b
        LEFT JOIN
            bibliographic_production_article AS bpa ON b.id = bpa.bibliographic_production_id
        LEFT JOIN
            periodical_magazine AS pm ON bpa.periodical_magazine_id = pm.id
            , researcher r, institution i, graduate_program_researcher gpr, graduate_program gp
        WHERE
            gpr.graduate_program_id = gp.graduate_program_id
            AND gpr.researcher_id = r.id
            AND r.id = b.researcher_id
            AND r.institution_id = i.id;
        """

    registry = sgbdSQL.consultar_db(script_sql)

    data_frame_bd = pd.DataFrame(
        registry,
        columns=["title", "tipo", "year", "graduate_program_id", "year_pos", "qualis"],
    )

    data_frame_bd.to_csv("Files/cimatec_production_year_distinct.csv")


def cimatec_article_qualis_distinct_csv_db():
    script_sql = """
        SELECT DISTINCT
            title,
            bar.qualis,
            bar.jcr,
            b.year AS year,
            gp.graduate_program_id AS graduate_program_id,
            gpr.year AS year_pos
        FROM
            PUBLIC.bibliographic_production b,
            bibliographic_production_article bar,
            periodical_magazine pm,
            researcher r,
            graduate_program_researcher gpr,
            graduate_program gp
        WHERE
            pm.id = bar.periodical_magazine_id
            AND gpr.graduate_program_id = gp.graduate_program_id
            AND gpr.researcher_id = r.id
            AND r.id = b.researcher_id
            AND b.id = bar.bibliographic_production_id
        GROUP BY
            title,
            bar.qualis,
            bar.jcr,
            b.year,
            gp.graduate_program_id,
            gpr.year
        ORDER BY
            qualis DESC;
        """

    registry = sgbdSQL.consultar_db(script_sql)

    data_frame_bd = pd.DataFrame(
        registry,
        columns=["title", "qualis", "jcr", "year", "graduate_program_id", "year_pos"],
    )

    data_frame_bd.to_csv("Files/cimatec_article_qualis_distinct.csv")


def insert_student_graduate_program_db(table, graduate_program_id, year):
    script_sql = f"""
        SELECT
            r.id
        FROM
            {table}
            gdp,
            researcher r
        WHERE
            similarity(unaccent(LOWER(gdp.aluno)),
                       unaccent(LOWER(r.name))) > 0.8;
        """

    registry = sgbdSQL.consultar_db(script_sql)

    data_frame_bd = pd.DataFrame(registry, columns=["id"])

    for Index, infos in data_frame_bd.iterrows():
        researcher_id = infos.id

        type_ = "EFETIVO"
        script_sql = f"""
        INSERT INTO public.graduate_program_student (researcher_id, graduate_program_id, year, type_)
        VALUES ('{researcher_id}', '{graduate_program_id}',
                '{year}', '{type_}');
        """

        sgbdSQL.execScript_db(script_sql)


def cimatec_production_tecnical_year_csv_db():
    script_sql = """
        SELECT DISTINCT
            title,
            development_year::int AS year,
            'PATENT' AS type,
            gp.graduate_program_id AS graduate_program_id,
            gpr.year AS year_pos
        FROM
            patent p,
            graduate_program_researcher gpr,
            graduate_program gp
        WHERE
            gpr.graduate_program_id = gp.graduate_program_id
            AND gpr.researcher_id = p.researcher_id

        UNION

        SELECT DISTINCT
            title,
            s.year AS year,
            'SOFTWARE' AS type,
            gp.graduate_program_id AS graduate_program_id,
            gpr.year AS year_pos
        FROM
            software s,
            graduate_program_researcher gpr,
            graduate_program gp
        WHERE
            gpr.graduate_program_id = gp.graduate_program_id
            AND gpr.researcher_id = s.researcher_id
        UNION

        SELECT DISTINCT
            title,
            b.year AS year,
            'BRAND' AS type,
            gp.graduate_program_id AS graduate_program_id,
            gpr.year AS year_pos
        FROM
            brand b,
            graduate_program_researcher gpr,
            graduate_program gp
        WHERE
            gpr.graduate_program_id = gp.graduate_program_id
            AND gpr.researcher_id = b.researcher_id

        UNION

        SELECT DISTINCT
            title,
            b.year AS year,
            'REPORT' AS type,
            gp.graduate_program_id AS graduate_program_id,
        """

    registry = sgbdSQL.consultar_db(script_sql)

    data_frame_db = pd.DataFrame(
        registry, columns=["title", "year", "type", "graduate_program_id", "year_pos"]
    )

    data_frame_db.to_csv(f"Files/cimatec_production_tecnical_year.csv")


if __name__ == "__main__":
    graduate_program_csv_db()
    graduate_program_researcher_csv_db()
    graduate_program_researcher_csv_db()
    cimatec_article_qualis_distinct_csv_db()
    cimatec_production_tecnical_year_csv_db()
    graduate_program_student_researcher_csv_db()
    cimatec_researcher_production_year_distinct_csv_db()
