import pandas as pd
import Dao.sgbdSQL as db


def add_image(id, type_):
    bibliographic_production = [
        "BOOK",
        "BOOK_CHAPTER",
        "ARTICLE",
        "WORK_IN_EVENT",
        "TEXT_IN_NEWSPAPER_MAGAZINE",
    ]
    if type_ in bibliographic_production:
        add_bibliographic_production_image(id, type_)
    if type_ == "SOFTWARE":
        add_software_image(id)
    if type_ == "PATENT":
        add_patent_image(id)
    if type_ == "BRAND":
        add_brand_image(id)


def add_brand_image(id):
    SCRIPT_SQL = """
    UPDATE brand
    SET has_image = true
    WHERE id = %(id)s;
    """
    db.execScript_db(sql=SCRIPT_SQL, params={"id": id})


def add_patent_image(id):
    SCRIPT_SQL = """
    UPDATE patent
    SET has_image = true
    WHERE id = %(id)s;
    """
    db.execScript_db(sql=SCRIPT_SQL, params={"id": id})


def add_bibliographic_production_image(id, type_):
    SCRIPT_SQL = """
        UPDATE bibliographic_production
        SET has_image = true
        WHERE id = %(id)s AND type = %(type_)s;
        """
    db.execScript_db(sql=SCRIPT_SQL, params={"id": id, "type_": type_})


def add_software_image(id):
    SCRIPT_SQL = """
    UPDATE software
    SET has_image = true
    WHERE id = %(id)s;
    """
    db.execScript_db(sql=SCRIPT_SQL, params={"id": id})


def delete_image(id, type_):
    bibliographic_production = [
        "BOOK",
        "BOOK_CHAPTER",
        "ARTICLE",
        "WORK_IN_EVENT",
        "TEXT_IN_NEWSPAPER_MAGAZINE",
    ]
    if type_ in bibliographic_production:
        delete_bibliographic_production_image(id, type_)
    if type_ == "SOFTWARE":
        delete_software_image(id)
    if type_ == "PATENT":
        delete_patent_image(id)
    if type_ == "BRAND":
        delete_brand_image(id)


def delete_brand_image(id):
    SCRIPT_SQL = """
    UPDATE brand
    SET has_image = false
    WHERE id = %(id)s;
    """
    db.execScript_db(sql=SCRIPT_SQL, params={"id": id})


def delete_patent_image(id):
    SCRIPT_SQL = """
    UPDATE patent
    SET has_image = false
    WHERE id = %(id)s;
    """
    db.execScript_db(sql=SCRIPT_SQL, params={"id": id})


def delete_bibliographic_production_image(id, type_):
    SCRIPT_SQL = """
        UPDATE bibliographic_production
        SET has_image = false
        WHERE id = %(id)s AND type = %(type_)s;
        """
    db.execScript_db(sql=SCRIPT_SQL, params={"id": id, "type_": type_})


def delete_software_image(id):
    SCRIPT_SQL = """
    UPDATE software
    SET has_image = false
    WHERE id = %(id)s;
    """
    db.execScript_db(sql=SCRIPT_SQL, params={"id": id})


def add_relevance(id, type_):
    bibliographic_production = [
        "BOOK",
        "BOOK_CHAPTER",
        "ARTICLE",
        "WORK_IN_EVENT",
        "TEXT_IN_NEWSPAPER_MAGAZINE",
    ]
    if type_ in bibliographic_production:
        add_bibliographic_production_relevance(id, type_)
    if type_ == "SOFTWARE":
        add_software_relevance(id)
    if type_ == "PATENT":
        add_patent_relevance(id)
    if type_ == "BRAND":
        add_brand_relevance(id)


def add_brand_relevance(id):
    SCRIPT_SQL = """
    UPDATE brand
    SET relevance = true
    WHERE id = %(id)s;
    """
    db.execScript_db(sql=SCRIPT_SQL, params={"id": id})


def add_patent_relevance(id):
    SCRIPT_SQL = """
    UPDATE patent
    SET relevance = true
    WHERE id = %(id)s;
    """
    db.execScript_db(sql=SCRIPT_SQL, params={"id": id})


def add_bibliographic_production_relevance(id, type_):
    SCRIPT_SQL = """
        UPDATE bibliographic_production
        SET relevance = true
        WHERE id = %(id)s AND type = %(type_)s;
    """
    db.execScript_db(sql=SCRIPT_SQL, params={"id": id, "type_": type_})


def add_software_relevance(id):
    SCRIPT_SQL = """
    UPDATE software
    SET relevance = true
    WHERE id = %(id)s;
    """
    db.execScript_db(sql=SCRIPT_SQL, params={"id": id})


def get_relevant_bibliographic_production(researcher_id, type_):
    params = {}
    SCRIPT_SQL = """
        SELECT id, type, has_image 
        FROM bibliographic_production
        WHERE relevance = true
        """

    if researcher_id:
        SCRIPT_SQL += "AND researcher_id = %(researcher_id)s"
        params["researcher_id"] = researcher_id

    if type_:
        SCRIPT_SQL += "AND type = %(type_)s"
        params["type_"] = type_

    result = db.consultar_db(sql=SCRIPT_SQL, params=params)
    df = pd.DataFrame(result, columns=["production_id", "type_", "has_image"])
    return df


def get_relevant_patent(researcher_id, type_):
    params = {}
    SCRIPT_SQL = """
    SELECT id, 'PATENT', has_image 
    FROM patent
    WHERE relevance = true
    """
    if researcher_id:
        SCRIPT_SQL += "AND researcher_id = %(researcher_id)s"
        params["researcher_id"] = researcher_id

    result = db.consultar_db(sql=SCRIPT_SQL, params=params)
    df = pd.DataFrame(result, columns=["production_id", "type_", "has_image"])
    return df


def get_relevant_software(researcher_id, type_):
    params = {}
    SCRIPT_SQL = """
    SELECT id, 'SOFTWARE', has_image 
    FROM software
    WHERE relevance = true
    """
    if researcher_id:
        SCRIPT_SQL += "AND researcher_id = %(researcher_id)s"
        params["researcher_id"] = researcher_id

    result = db.consultar_db(sql=SCRIPT_SQL, params=params)
    df = pd.DataFrame(result, columns=["production_id", "type_", "has_image"])
    return df


def get_relevant_brand(researcher_id, type_):
    params = {}
    SCRIPT_SQL = """
    SELECT id, 'BRAND', has_image 
    FROM brand
    WHERE relevance = true
    """

    if researcher_id:
        SCRIPT_SQL += "AND researcher_id = %(researcher_id)s"
        params["researcher_id"] = researcher_id

    result = db.consultar_db(sql=SCRIPT_SQL, params=params)
    df = pd.DataFrame(result, columns=["production_id", "type_", "has_image"])
    return df


def get_relevant_list(researcher_id, type_):
    relevant_production = []
    bibliographic_production = [
        "BOOK",
        "BOOK_CHAPTER",
        "ARTICLE",
        "WORK_IN_EVENT",
        "TEXT_IN_NEWSPAPER_MAGAZINE",
    ]
    if type_ in bibliographic_production:
        relevant_production.append(
            get_relevant_bibliographic_production(researcher_id, type_)
        )
    if type_ == "SOFTWARE":
        relevant_production.append(get_relevant_software(researcher_id, type_))
    if type_ == "PATENT":
        relevant_production.append(get_relevant_patent(researcher_id, type_))
    if type_ == "BRAND":
        relevant_production.append(get_relevant_brand(researcher_id, type_))

    df = pd.concat(relevant_production, ignore_index=True)
    return df.to_dict(orient="records")


def delete_relevance(id, type_):
    bibliographic_production = [
        "BOOK",
        "BOOK_CHAPTER",
        "ARTICLE",
        "WORK_IN_EVENT",
        "TEXT_IN_NEWSPAPER_MAGAZINE",
    ]
    if type_ in bibliographic_production:
        delete_bibliographic_production_relevance(id, type_)
    if type_ == "SOFTWARE":
        delete_software_relevance(id)
    if type_ == "PATENT":
        delete_patent_relevance(id)
    if type_ == "BRAND":
        delete_brand_relevance(id)


def delete_brand_relevance(id):
    SCRIPT_SQL = """
    UPDATE brand
    SET relevance = false
    WHERE id = %(id)s;
    """
    db.execScript_db(sql=SCRIPT_SQL, params={"id": id})


def delete_patent_relevance(id):
    SCRIPT_SQL = """
    UPDATE patent
    SET relevance = false
    WHERE id = %(id)s;
    """
    db.execScript_db(sql=SCRIPT_SQL, params={"id": id})


def delete_bibliographic_production_relevance(id, type_):
    SCRIPT_SQL = """
        UPDATE bibliographic_production
        SET relevance = false
        WHERE id = %(id)s AND type = %(type_)s;
    """
    db.execScript_db(sql=SCRIPT_SQL, params={"id": id, "type_": type_})


def delete_software_relevance(id):
    SCRIPT_SQL = """
    UPDATE software
    SET relevance = false
    WHERE id = %(id)s;
    """
    db.execScript_db(sql=SCRIPT_SQL, params={"id": id})
