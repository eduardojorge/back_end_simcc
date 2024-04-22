import nltk
import unidecode
from nltk.tokenize import RegexpTokenizer


# Função para consultar a lista de pesquisadores por palavras existentes na sua frequência
def filterSQLRank(text, split, attribute_2):

    text = text.replace("-", " ")
    if (len(text.split(split))) == 3:
        text = clean_stopwords(text)
    if (len(text.split("|"))) == 2:
        t = []
        t = text.split("|")
        filter = ""
        filter = (
            """ ts_rank(to_tsvector(unaccent(LOWER(%s))), websearch_to_tsquery( '%s')) > %s    """
            % (attribute_2, unidecode.unidecode(t[0]), 0.04)
        )
        filter = (
            "AND ("
            + filter
            + " OR ts_rank(to_tsvector(unaccent(LOWER(%s))), websearch_to_tsquery( '%s')) > %s    "
            "" % (attribute_2, unidecode.unidecode(t[1]), 0.04) + ")"
        )
        return filter

    filter = " "
    if text != "":
        t = []
        t = text.split(split)
        filter = ""

        if (len(t)) == 1:
            filter = (
                """ ts_rank(to_tsvector(unaccent(LOWER(%s))), websearch_to_tsquery( '%s')) > %s    """
                % (attribute_2, unidecode.unidecode(text), 0.04)
            )
            print("Rank" + text)
            x = len(filter)
            filter = filter[0 : x - 3]
            filter = " AND (" + filter + ")"
            text = text.strip().replace(" ", "&")
            filter = (
                filter
                + """ AND  (translate(unaccent(LOWER(%s)),'-\.:;''',' ') ::tsvector@@ unaccent(LOWER( '%s'))::tsquery)=TRUE """
                % (attribute_2, unidecode.unidecode(text))
            )

        else:
            filter = (
                """ ts_rank(to_tsvector(translate(unaccent(LOWER(%s)),'-\.:;''',' ')), websearch_to_tsquery( '%s<->%s')) > %s    """
                % (
                    attribute_2,
                    unidecode.unidecode(t[0]),
                    unidecode.unidecode(t[1]),
                    0.04,
                )
            )
            x = len(filter)
            filter = filter[0 : x - 3]
            filter = " AND (" + filter + ")"
    return filter


def filterSQLRank2(text, split, attribute_2):
    if (len(text.split(split))) == 3:
        text = clean_stopwords(text)

    filter = " "
    if text != "":
        t = []
        t = text.split(split)
        filter = ""
        i = 0

        if (len(t)) == 1:
            # filter = """ (translate(unaccent(LOWER(%s)),\':\',\'\') ::tsvector@@ '%s'::tsquery)=true   """ % (attribute,text)
            filter = (
                """ ts_rank(to_tsvector(unaccent(LOWER(%s))), websearch_to_tsquery( '%s')) > %s    """
                % (attribute_2, unidecode.unidecode(text), 0.02)
            )
            x = len(filter)
            filter = filter[0 : x - 3]
            filter = " AND (" + filter + ")"
            text = text.strip().replace(" ", "&")
            filter = (
                filter
                + """ AND  (translate(unaccent(LOWER(%s)),'\.:;''','') ::tsvector@@ unaccent(LOWER( '%s'))::tsquery)=TRUE """
                % (attribute_2, unidecode.unidecode(text))
            )
            print("Rank2" + text)
        else:
            filter = (
                """ ts_rank(to_tsvector(unaccent(LOWER(%s))), websearch_to_tsquery( '%s<->%s')) > %s    """
                % (
                    attribute_2,
                    unidecode.unidecode(t[0]),
                    unidecode.unidecode(t[1]),
                    0.02,
                )
            )
            x = len(filter)
            filter = filter[0 : x - 3]
            filter = " AND (" + filter + ")"
            t[1] = t[1].strip().replace(" ", "&")
            filter = (
                filter
                + """ AND  (translate(unaccent(LOWER(%s)),'\.:;''','') ::tsvector@@ unaccent(LOWER( '%s'))::tsquery)=TRUE """
                % (attribute_2, unidecode.unidecode(t[1]))
            )

    return filter


# Função para consultar a lista de pesquisadores por palavras existentes na sua frequência
def filterSQL(text, split, booleanOperator, attribute):
    filter = " "
    if text != "":
        t = []
        t = text.split(split)
        filter = ""
        i = 0
        for word in t:
            filter = (
                " unaccent(LOWER("
                + attribute
                + "))='"
                + unidecode.unidecode(word.lower())
                + "' "
                + booleanOperator
                + ""
                + filter
            )
            i = i + 1
        x = len(filter)
        filter = filter[0 : x - 3]
        filter = " AND (" + filter + ")"
    return filter


def filterSQLLike(text, split, booleanOperator, attribute):
    filter = " "
    if text != "":
        t = []
        t = text.split(split)
        filter = ""
        i = 0
        for word in t:
            filter = (
                " unaccent(LOWER("
                + attribute
                + ")) LIKE '%"
                + unidecode.unidecode(word.lower())
                + "%' "
                + booleanOperator
                + ""
                + filter
            )
            i = i + 1
        x = len(filter)
        filter = filter[0 : x - 3]
        filter = " AND (" + filter + ")"
    return filter


def unidecodelower(text1, text2):
    if unidecode.unidecode(text1.lower()) == unidecode.unidecode(text2.lower()):
        return True
    else:
        return False


def clean_stopwords(text):
    stopwords_portuguese = nltk.corpus.stopwords.words("portuguese")
    stopwords_english = nltk.corpus.stopwords.words("english")
    tokenize = RegexpTokenizer(r"\w+")
    tokens = []
    tokens = tokenize.tokenize(text)

    text_new = ""
    for word in tokens:
        if not (
            (word.lower() in stopwords_portuguese)
            or (word.lower() in stopwords_english)
        ):
            text_new = text_new + word.lower() + ";"
    return text_new[0 : len(text_new) - 1]


def web_search_filter(string_of_terms, table):

    position_to_skip = 0
    term = str()
    web_search_filter = str()

    def __add_parse(term):
        return f"""ts_rank(to_tsvector(translate(unaccent(LOWER({table})),'-\.:;''',' ')), websearch_to_tsquery('"{term}"')) > 0.04 \nAND\n """

    def __or_parse(term):
        return f"""ts_rank(to_tsvector(translate(unaccent(LOWER({table})),'-\.:;''',' ')), websearch_to_tsquery('"{term}"')) > 0.04 \nOR\n """

    def __not_parse(term):
        return f"""ts_rank(to_tsvector(translate(unaccent(LOWER({table})),'-\.:;''',' ')), websearch_to_tsquery('"{term}"')) > 0.04 \nAND NOT\n """

    def __priority(term):
        end_of_priority = string_of_terms.find(")", position)
        return web_search_filter(string_of_terms[position + 1 : end_of_priority], table)

    sintax_simbols = [",", ".", ";", "("]

    grammatic = {",": __add_parse, ".": __not_parse, ";": __or_parse, "(": __priority}

    for position, char in enumerate(string_of_terms):
        if char in sintax_simbols:
            web_search_filter += grammatic[char](term)
            term = str()
            if char == str("("):
                position_to_skip = string_of_terms.find(")", position)
            if position_to_skip:
                position_to_skip -= 1
                break
        else:
            term += char
    if term:
        web_search_filter += f"""ts_rank(to_tsvector(translate(unaccent(LOWER({table})),'-\.:;''',' ')), websearch_to_tsquery('{term}')) > 0.04"""
        term = str()
    return f"""({web_search_filter})"""
