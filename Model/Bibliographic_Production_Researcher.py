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
        }
        return Bibliographic_Production_Researcher
