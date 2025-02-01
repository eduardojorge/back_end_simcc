from enum import Enum


class ResearcherOptions(str, Enum):
    ARTICLE = 'ARTICLE'
    ABSTRACT = 'ABSTRACT'


class ArticleOptions(str, Enum):
    ARTICLE = 'ARTICLE'
    ABSTRACT = 'ABSTRACT'


class QualisOptions(str, Enum):
    A1: int = 'A1'
    A2: int = 'A2'
    A3: int = 'A3'
    A4: int = 'A4'
    B1: int = 'B1'
    B2: int = 'B2'
    B3: int = 'B3'
    B4: int = 'B4'
    C: int = 'C'
    SQ: int = 'SQ'
