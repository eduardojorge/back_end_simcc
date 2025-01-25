from unidecode import unidecode


def web_search_param(string_of_terms):
    operator_map = {';': ' ', '.': ' -', '|': ' | ', '(': '(', ')': ')'}

    sanitized_terms = []
    for char in string_of_terms:
        if char in operator_map:
            sanitized_terms.append(operator_map[char])
        else:
            sanitized_terms.append(char)

    sanitized_terms = ''.join(sanitized_terms)
    sanitized_terms = unidecode(sanitized_terms)
    return sanitized_terms


def pagination(page, lenght):
    return f'OFFSET {lenght * (page - 1)} LIMIT {lenght}'


def web_search_filter(table):
    SCRIPT_SQL = rf"""
    AND ts_rank(to_tsvector(translate(unaccent(LOWER({table})),'-\.:;''',' ')),
        websearch_to_tsquery(%(terms)s)) > 0.04
    """  # noqa: E501
    return SCRIPT_SQL


def new_web_search_filter(string_of_terms, column):
    skip = 0
    term = str()
    filter_terms = str()
    sintax_simbols = [';', '.', '|', '(']
    grammatic = {';': ' AND \n\n', '.': ' AND NOT \n\n', '|': ' OR \n\n'}

    for position, char in enumerate(string_of_terms):
        if skip:
            skip -= 1
            continue

        if char in sintax_simbols:
            if char == '(':
                start = position + 1
                end = string_of_terms.find(')', start)
                part = string_of_terms[start:end]
                skip = len(part) + 1
                filter_terms += web_search_filter(part, column)
                continue
            elif term:
                term = unidecode.unidecode(term.lower())
                filter_terms += rf"""
                ts_rank(to_tsvector(translate(unaccent(LOWER({column})),'-\.:;''',' ')),
                websearch_to_tsquery('"{term}"')) > 0.04
                """
            filter_terms += grammatic[char]
            term = str()
        else:
            term += char
    if term:
        term = unidecode.unidecode(term.lower())
        filter_terms += rf"""
            ts_rank(to_tsvector(translate(unaccent(LOWER({column})),'-\.:;''',' ')),
            websearch_to_tsquery('"{term}"')) > 0.04
            """
    return f"""({filter_terms})"""
