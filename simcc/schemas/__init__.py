from enum import Enum


class ResearcherOptions(str, Enum):
    ARTICLE = 'ARTICLE'
    ABSTRACT = 'ABSTRACT'


class ArticleOptions(str, Enum):
    ARTICLE = 'ARTICLE'
    ABSTRACT = 'ABSTRACT'


class QualisOptions(str, Enum):
    A1: str = 'A1'
    A2: str = 'A2'
    A3: str = 'A3'
    A4: str = 'A4'
    B1: str = 'B1'
    B2: str = 'B2'
    B3: str = 'B3'
    B4: str = 'B4'
    C: str = 'C'
    SQ: str = 'SQ'
