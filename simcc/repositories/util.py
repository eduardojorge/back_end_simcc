from unidecode import unidecode


def pagination(page, lenght):
    return f'OFFSET {lenght * (page - 1)} LIMIT {lenght}'


def parse_terms(string_of_terms):
    operator_map = {';': 'AND', '.': 'AND NOT', '|': 'OR', '(': '(', ')': ')'}
    terms = []
    term = ''

    for char in string_of_terms:
        if char in operator_map:
            if term.strip():
                terms.append(term.strip())
                term = ''
            terms.append(operator_map[char])
        else:
            term += char
    if term.strip():
        terms.append(term.strip())

    return terms


def sanitize_terms(terms):
    sanitized = []
    for term in terms:
        if term not in {'AND', 'OR', 'AND NOT', '(', ')'}:
            sanitized.append(unidecode(term.lower()))
        else:
            sanitized.append(term)
    return sanitized


def build_query_terms(sanitized_terms, column):
    terms_dict = {}
    query_parts = []
    term_counter = 1

    for term in sanitized_terms:
        if term in {'AND', 'OR', 'AND NOT', '(', ')'}:
            query_parts.append(term)
        else:
            placeholder = f'term{term_counter}'
            terms_dict[placeholder] = term
            SCRIPT_SQL = f"""
                ts_rank(
                to_tsvector(
                translate(
                unaccent(
                LOWER({column})), '-\\.:;''',' ')),
                websearch_to_tsquery(%({placeholder})s)) > 0.04
                """
            query_parts.append(SCRIPT_SQL)
            term_counter += 1

    return ' '.join(query_parts), terms_dict


def webseatch_filter(column, string_of_terms):
    terms = parse_terms(string_of_terms)
    sanitized_terms = sanitize_terms(terms)
    query_terms, terms_dict = build_query_terms(sanitized_terms, column)

    filter_sql = f'AND ({query_terms})'
    return filter_sql, terms_dict
