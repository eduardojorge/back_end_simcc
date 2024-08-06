import Dao.sgbdSQL as sgbdSQL
import pandas as pd
import Dao.util as util
import Model.Resarcher_Production as Resarcher_Production


def article_qualis(resarcher_Production, infos):
    if infos.tipo[7:10] == "A1":
        resarcher_Production.article_A1 = infos.qtd
    if infos.tipo[7:10] == "A2":
        resarcher_Production.article_A2 = infos.qtd
    if infos.tipo[7:10] == "A3":
        resarcher_Production.article_A3 = infos.qtd
    if infos.tipo[7:10] == "A4":
        resarcher_Production.article_A4 = infos.qtd
    if infos.tipo[7:10] == "B1":
        resarcher_Production.article_B1 = infos.qtd
    if infos.tipo[7:10] == "B2":
        resarcher_Production.article_B2 = infos.qtd
    if infos.tipo[7:10] == "B3":
        resarcher_Production.article_B3 = infos.qtd
    if infos.tipo[7:10] == "B4":
        resarcher_Production.article_B4 = infos.qtd
    if infos.tipo[7:10] == "SQ":
        resarcher_Production.article_SQ = infos.qtd
    if infos.tipo[7:9] == "C":
        resarcher_Production.article_C = infos.qtd
    return resarcher_Production


def lists_guidance_researcher_db(year, resarcher_Production):

    sql = f"""
        SELECT
            COUNT(g.id) AS qtd,
            status,
            nature
        FROM
            guidance g
        WHERE
            g.researcher_id = '{resarcher_Production.id}'
            AND g.year >= {year.resource_completed}
        GROUP BY
            status,
            nature;
      """

    reg = sgbdSQL.consultar_db(sql)

    df_bd = pd.DataFrame(reg, columns=["qtd", "status", "nature"])

    for i, infos in df_bd.iterrows():
        if (
            util.unidecodelower(infos.nature, "Dissertação De Mestrado")
            and infos.status == "Concluída"
        ):
            resarcher_Production.guidance_m_c = infos.qtd

        if (
            util.unidecodelower(infos.nature.lower(),
                                "Dissertação De Mestrado")
            and infos.status == "Em andamento"
        ):
            resarcher_Production.guidance_m_a = infos.qtd

        if (
            util.unidecodelower(infos.nature, "Tese De Doutorado")
            and infos.status == "Concluída"
        ):
            resarcher_Production.guidance_d_c = infos.qtd

        if (
            util.unidecodelower(infos.nature, "Tese De Doutorado")
            and infos.status == "Em andamento"
        ):
            resarcher_Production.guidance_d_a = infos.qtd

        if (
            util.unidecodelower(infos.nature, "Iniciacao Cientifica")
            and infos.status == "Concluída"
        ):
            resarcher_Production.guidance_ic_c = infos.qtd

        if (
            util.unidecodelower(infos.nature, "Iniciacao Cientifica")
            and infos.status == "Em andamento"
        ):
            resarcher_Production.guidance_ic_a = infos.qtd

        if (
            util.unidecodelower(
                infos.nature, "Trabalho de Conclusao de Curso Graduacao"
            )
            and infos.status == "Concluída"
        ):
            resarcher_Production.guidance_g_c = infos.qtd

        if (
            util.unidecodelower(
                infos.nature, "Trabalho de Conclusao de Curso Graduacao"
            )
            and infos.status == "Em andamento"
        ):
            resarcher_Production.guidance_g_a = infos.qtd

        if (
            util.unidecodelower(
                infos.nature,
                "Monografia de Conclusao de Curso Aperfeicoamento e Especializacao",
            )
            and infos.status == "Concluída"
        ):
            resarcher_Production.guidance_e_c = infos.qtd

        if (
            util.unidecodelower(
                infos.nature,
                "Monografia de Conclusao de Curso Aperfeicoamento e Especializacao",
            )
            and infos.status == "Em andamento"
        ):
            resarcher_Production.guidance_e_a = infos.qtd

    return resarcher_Production


# Função processar e inserir a produção de cada pesquisador
def production_general_db(name, lattes_id, year):

    filter = ""
    if name == "":
        filter = f"r.lattes_id='{lattes_id}' AND "
    elif name != "todos":
        filter = f"r.name='{name}' AND "

    script_sql = f"""
        SELECT
            COUNT(p.id) AS qtd,
            'BRAND' AS type,
            r.name,
            r.lattes_10_id,
            r.graduation,
            r.id
        FROM
            brand p,
            researcher r
        WHERE
            p.researcher_id = r.id
            AND {filter} p.year >= {year.brand}
        GROUP BY
            type,
            r.name,
            r.lattes_10_id,
            r.graduation,
            r.id
        UNION

        SELECT
            COUNT(p.id) AS qtd,
            'PATENT' AS type,
            r.name,
            r.lattes_10_id,
            r.graduation,
            r.id
        FROM
            patent p,
            researcher r
        WHERE
            p.researcher_id = r.id
            AND {filter} p.development_year::INT >= {year.patent}
        GROUP BY
            type,
            r.name,
            r.lattes_10_id,
            r.graduation,
            r.id
        UNION

        SELECT
            COUNT(s.id) AS qtd,
            'SOFTWARE' AS type,
            r.name,
            r.lattes_10_id,
            r.graduation,
            r.id
        FROM
            software s,
            researcher r
        WHERE
            s.researcher_id = r.id
            AND {filter} s.year >= {year.software}
        GROUP BY
            type,
            r.name,
            r.lattes_10_id,
            r.graduation,
            r.id
        UNION

        SELECT
            COUNT(ba.id) AS qtd,
            'ARTICLE' || ba.qualis AS type,
            r.name,
            r.lattes_10_id,
            r.graduation,
            r.id
        FROM
            PUBLIC.bibliographic_production b,
            bibliographic_production_article ba,
            researcher r
        WHERE
            b.id = ba.bibliographic_production_id
            AND type='ARTICLE'
            AND b.researcher_id = r.id
            AND {filter} b.year_ >= {year.article}
        GROUP BY
            'ARTICLE' || ba.qualis,
            r.name,
            r.lattes_10_id,
            r.graduation, r.id
        UNION

        SELECT
            COUNT(b.id) AS qtd,
            'BOOK' AS type,
            r.name,
            r.lattes_10_id,
            r.graduation,
            r.id
        FROM
            PUBLIC.bibliographic_production b,
            researcher r
        WHERE
            b.researcher_id = r.id
            AND type IN ('BOOK')
            AND {filter} b.year_ >= {year.book}
        GROUP BY
            type,
            r.name,
            r.lattes_10_id,
            r.graduation,
            r.id
        UNION

        SELECT
            COUNT(b.id) AS qtd,
            'BOOK_CHAPTER' AS type,
            r.name,
            r.lattes_10_id,
            r.graduation,
            r.id
        FROM
            PUBLIC.bibliographic_production b,
            researcher r
        WHERE
            b.researcher_id = r.id
            AND type IN ('BOOK_CHAPTER')
            AND {filter} b.year_ >= {year.chapter_book}
        GROUP BY
            type,
            r.name,
            r.lattes_10_id,
            r.graduation,
            r.id
        UNION

        SELECT
            COUNT(b.id) AS qtd,
            'WORK_IN_EVENT' AS type,
            r.name,
            r.lattes_10_id,
            r.graduation,
            r.id
        FROM
            PUBLIC.bibliographic_production b,
            researcher r
        WHERE
            b.researcher_id = r.id
            AND type IN ('WORK_IN_EVENT')
            AND {filter} b.year_ >= {year.work_event}
        GROUP BY
            type,
            r.name,
            r.lattes_10_id,
            r.graduation,
            r.id
        UNION

        SELECT
            COUNT(b.id) AS qtd,
            'EVENT_ORGANIZATION' AS type,
            r.name,
            r.lattes_10_id,
            r.graduation,
            r.id
        FROM
            event_organization b,
            researcher r
        WHERE
            b.researcher_id = r.id
            AND {filter} b.year >= {year.participation_events}
        GROUP BY
            type,
            r.name,
            r.lattes_10_id,
            r.graduation,
            r.id
        UNION

        SELECT
            COUNT(b.id) AS qtd,
            'PARTICIPATION_EVENTS' AS type,
            r.name,
            r.lattes_10_id,
            r.graduation,
            r.id
        FROM
            participation_events b,
            researcher r
        WHERE
            b.researcher_id = r.id
            AND {filter} b.year >= {year.participation_events}
        GROUP BY
            type,
            r.name,
            r.lattes_10_id,
            r.graduation,
            r.id;
        """

    reg = sgbdSQL.consultar_db(script_sql)
    df_bd = pd.DataFrame(
        reg,
        columns=["qtd", "tipo", "name_", "lattes_10_id",
                 "graduation", "researcher_id"],
    )
    resarcher_Production = Resarcher_Production.Resarcher_Production()

    for Index, Data in df_bd.iterrows():
        resarcher_Production.lattes_10_id = Data.lattes_10_id
        resarcher_Production.researcher = str(Data.name_)
        resarcher_Production.id = str(Data.researcher_id)
        resarcher_Production.graduation = Data.graduation

        if Data.tipo == "BOOK":
            resarcher_Production.book = Data.qtd
        if Data.tipo == "WORK_IN_EVENT":
            resarcher_Production.work_in_event = Data.qtd
        if Data.tipo[0:7] == "ARTICLE":
            resarcher_Production = article_qualis(resarcher_Production, Data)
        if Data.tipo == "BOOK_CHAPTER":
            resarcher_Production.book_chapter = Data.qtd
        if Data.tipo == "PATENT":
            resarcher_Production.patent = Data.qtd
        if Data.tipo == "SOFTWARE":
            resarcher_Production.software = Data.qtd
        if Data.tipo == "BRAND":
            resarcher_Production.brand = Data.qtd
        if Data.tipo == "EVENT_ORGANIZATION":
            resarcher_Production.event_organization = Data.qtd
        if Data.tipo == "PARTICIPATION_EVENTS":
            resarcher_Production.participation_event = Data.qtd
    if reg:
        lists_guidance_researcher_db(year, resarcher_Production)
    return resarcher_Production.getJson()


def researcher_production_db(list_name, lattes_id, year):

    if lattes_id:
        lattes_id_list = list()
        lattes_id_list = lattes_id.split(";")

        cont = 0
        json_researcher = list()
        for lattes_id in lattes_id_list:
            cont += 1
            print(cont, " | ", lattes_id)
            json_researcher.append(production_general_db("", lattes_id, year))

        return json_researcher

    elif list_name.upper() == "TODOS":
        script_sql = "SELECT lattes_id FROM researcher;"

        reg = sgbdSQL.consultar_db(script_sql)

        cont = 0
        json_researcher = list()
        df_bd = pd.DataFrame(reg, columns=["lattes_id"])

        for cont, Data in df_bd.iterrows():
            cont += 1
            print(cont, " | ", Data["lattes_id"])
            json_researcher.append(
                production_general_db("", Data["lattes_id"], year))

        return json_researcher
    else:
        name_list = list()
        name_list = list_name.split(";")

        cont = 0
        json_researcher = list()
        for name in name_list:
            cont += 1
            print(cont, " | ", name)
            json_researcher.append(production_general_db(name, "", year))

        return json_researcher
