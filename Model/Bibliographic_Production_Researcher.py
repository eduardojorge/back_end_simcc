class Bibliographic_Production_Researcher(object):
    id = ""
    title = ""
    year = ""
    type = ""
    doi = ""
    qualis = ""
    magazine = ""
    researcher = ""
    lattes_10_id = ""
    lattes_id = ""
    jif = ""
    jcr_link = ""
    researcher_id = ""
    article_institution = ""
    issn = ""
    authors_institution = ""
    abstract = ""
    authors = ""
    language = ""
    citations_count = ""
    pdf = ""
    landing_page_url = ""
    keywords = ""

    def __init__(self):
        self.id = ""

    def getJson(self):
        Bibliographic_Production_Researcher = {
            "id": self.id,
            "title": self.title,
            "year": self.year,
            "type": self.type,
            "doi": self.doi,
            "qualis": self.qualis,
            "magazine": self.magazine,
            "researcher": self.researcher,
            "lattes_10_id": self.lattes_10_id,
            "lattes_id": self.lattes_id,
            "jif": self.jif,
            "jcr_link": self.jcr_link,
            "researcher_id": self.researcher_id,
            "article_institution": self.article_institution,
            "issn": self.issn,
            "authors_institution": self.authors_institution,
            "abstract": self.abstract,
            "authors": self.authors,
            "language": self.language,
            "citations_count": self.citations_count,
            "pdf": self.pdf,
            "landing_page_url": self.landing_page_url,
            "keywords": self.keywords,
        }
        return Bibliographic_Production_Researcher
