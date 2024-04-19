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


def mk_string_ftx(string_of_terms):
    low_level_filter = [str(), string_of_terms]

    def parse_comma(low_level_filter, parse):
        filter_part = f'"{low_level_filter[1][:parse]}" & '
        low_level_filter[0] += filter_part
        low_level_filter[1] = low_level_filter[1][parse + 1 :]
        return low_level_filter

    def parse_dot(low_level_filter, parse):
        filter_part = f'-"{low_level_filter[1][:parse]}" '
        low_level_filter[0] += filter_part
        low_level_filter[1] = low_level_filter[1][parse + 1 :]
        return low_level_filter

    def parse_semicolon(low_level_filter, parse):
        filter_part = f'"{low_level_filter[1][:parse]}" or '
        low_level_filter[0] += filter_part
        low_level_filter[1] = low_level_filter[1][parse + 1 :]
        return low_level_filter

    grammar = {
        ",": parse_comma,
        ".": parse_dot,
        ";": parse_semicolon,
    }

    def make_string_filter(low_level_filter):
        characters = ["(", ",", ";", "."]

        if order := [
            value
            for value in [low_level_filter[1].find(char) for char in characters]
            if value >= 0
        ]:

            low_level_filter = grammar[low_level_filter[1][min(order)]](
                low_level_filter, min(order)
            )
            return low_level_filter
        low_level_filter[0] += low_level_filter[1]
        low_level_filter[1] = str()
        return low_level_filter

    while low_level_filter[1]:
        low_level_filter = make_string_filter(low_level_filter)

    return low_level_filter[0]


def filterSQLRank3(text, parameter):
    text = mk_string_ftx(text)
    filter = f"AND (ts_rank(to_tsvector(translate(unaccent(LOWER({parameter})),'-\.:;''',' ')), websearch_to_tsquery('{text}')) > 0.04)"
    return filter
