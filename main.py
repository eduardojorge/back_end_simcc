def parse(string_of_terms, table):

    position_to_skip = 0
    term = str()
    web_search_filter = str()

    def __add_parse(term):
        return f"""ts_rank(to_tsvector(translate(unaccent(LOWER({table})),'-\.:;''',' ')), websearch_to_tsquery('{term}')) > 0.04 \nAND\n """

    def __or_parse(term):
        return f"""ts_rank(to_tsvector(translate(unaccent(LOWER({table})),'-\.:;''',' ')), websearch_to_tsquery('{term}')) > 0.04 \nOR\n """

    def __not_parse(term):
        return f"""ts_rank(to_tsvector(translate(unaccent(LOWER({table})),'-\.:;''',' ')), websearch_to_tsquery('{term}')) > 0.04 \nAND NOT\n """

    def __priority(term):
        end_of_priority = string_of_terms.find(")", position)
        return parse(string_of_terms[position + 1 : end_of_priority], table)

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


print(parse("saude,bahia;(dengue,entropia)", "title"))
