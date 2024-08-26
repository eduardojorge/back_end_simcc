class Patent_Researcher(object):

    id = ""
    title = ""
    year = ""
    grant_date = ""
    researcher_name = ""

    def __init__(self):
        self.id = ""

    def getJson(self):

        Patent_Researcher = {
            "id": self.id,
            "title": self.title,
            "year": self.year,
            "grant_date": self.grant_date,
            "researcher_name": self.researcher_name,
        }
        return Patent_Researcher
