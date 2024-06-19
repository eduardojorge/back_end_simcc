import nltk
import unidecode
from nltk.tokenize import RegexpTokenizer

# fmt: off
def filterSQLRank(text, split, attribute_2):
    text = text.replace("-", " ")
    
    if len(text.split(split)) == 3:
        text = clean_stopwords(text)
    
    if len(text.split("|")) == 2:
        t = text.split("|")
        filter = """ ts_rank(to_tsvector(unaccent(LOWER(%s))), websearch_to_tsquery('%s')) > %s""" % (attribute_2, unidecode.unidecode(t[0]), 0.04)
        filter = "AND (" + filter + " OR ts_rank(to_tsvector(unaccent(LOWER(%s))), websearch_to_tsquery('%s')) > %s" % (
            attribute_2, unidecode.unidecode(t[1]), 0.04) + ")"
        return filter

    filter = " "
    
    if text != "":
        t = text.split(split)
        filter = ""

        if len(t) == 1:
            filter = """ ts_rank(to_tsvector(unaccent(LOWER(%s))), websearch_to_tsquery('%s')) > %s""" % (attribute_2, unidecode.unidecode(text), 0.04)
            print("Rank" + text)
            x = len(filter)
            filter = filter[0 : x - 3]
            filter = " AND (" + filter + ")"
            text = text.strip().replace(" ", "&")
            filter = filter + """ AND (translate(unaccent(LOWER(%s)),'-\\.:;''',' ')::tsvector@@unaccent(LOWER('%s'))::tsquery)=TRUE """ % (
                attribute_2, unidecode.unidecode(text))
        else:
            filter = """ ts_rank(to_tsvector(translate(unaccent(LOWER(%s)),'-\\.:;''',' ')), websearch_to_tsquery('%s<->%s')) > %s""" % (
                attribute_2, unidecode.unidecode(t[0]), unidecode.unidecode(t[1]), 0.04)
            x = len(filter)
            filter = filter[0 : x - 3]
            filter = " AND (" + filter + ")"
    
    return filter
# fmt: on


def filterSQLRank2(text, split, attribute_2):
    if len(text.split(split)) == 3:
        text = clean_stopwords(text)

    filter_query = ""

    if text != "":
        t = text.split(split)

        if len(t) == 1:
            filter_query = (
                r"""ts_rank(to_tsvector(unaccent(LOWER(%s))), websearch_to_tsquery('%s')) > %s"""
                % (attribute_2, unidecode.unidecode(text), 0.02)
            )
            filter_query = " AND (" + filter_query + ")"
            text = text.strip().replace(" ", "&")
            filter_query += (
                r""" AND (translate(unaccent(LOWER(%s)), '\.:;''', '')::tsvector @@ unaccent(LOWER('%s'))::tsquery) = TRUE"""
                % (attribute_2, unidecode.unidecode(text))
            )
        else:
            filter_query = (
                r"""ts_rank(to_tsvector(unaccent(LOWER(%s))), websearch_to_tsquery('%s<->%s')) > %s"""
                % (
                    attribute_2,
                    unidecode.unidecode(t[0]),
                    unidecode.unidecode(t[1]),
                    0.02,
                )
            )
            filter_query = " AND (" + filter_query + ")"
            t[1] = t[1].strip().replace(" ", "&")
            filter_query += (
                r""" AND (translate(unaccent(LOWER(%s)), '\.:;''', '')::tsvector @@ unaccent(LOWER('%s'))::tsquery) = TRUE"""
                % (attribute_2, unidecode.unidecode(t[1]))
            )

    return filter_query


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


def web_search_filter(string_of_terms, column):

    skip = 0
    term = str()
    filter_terms = str()
    sintax_simbols = [";", ".", "|", "("]
    grammatic = {";": " AND \n\n", ".": " AND NOT \n\n", "|": " OR \n\n"}

    for position, char in enumerate(string_of_terms):

        if skip:
            skip -= 1
            continue

        if char in sintax_simbols:
            if char == "(":
                start = position + 1
                end = string_of_terms.find(")", start)
                part = string_of_terms[start:end]
                skip = len(part) + 1
                filter_terms += web_search_filter(part, column)
                continue
            elif term:
                term = unidecode.unidecode(term.lower())
                filter_terms += rf"""ts_rank(to_tsvector(translate(unaccent(LOWER({column})),'-\.:;''',' ')), websearch_to_tsquery('"{term}"')) > 0.04"""
            filter_terms += grammatic[char]
            term = str()
        else:
            term += char
    if term:
        term = unidecode.unidecode(term.lower())
        filter_terms += rf"""ts_rank(to_tsvector(translate(unaccent(LOWER({column})),'-\.:;''',' ')), websearch_to_tsquery('"{term}"')) > 0.04"""
    return f"""({filter_terms})"""
