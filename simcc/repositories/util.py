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
