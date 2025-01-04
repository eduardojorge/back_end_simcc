def web_search_filter(string_of_terms):
    operator_map = {';': ' ', '.': ' -', '|': ' OR ', '(': '(', ')': ')'}

    sanitized_terms = []
    for char in string_of_terms:
        if char in operator_map:
            sanitized_terms.append(operator_map[char])
        else:
            sanitized_terms.append(char)

    sanitized_terms = ''.join(sanitized_terms)
    return sanitized_terms


def pagination(page, lenght):
    return f'OFFSET {lenght * (page - 1)} LIMIT {lenght}'
