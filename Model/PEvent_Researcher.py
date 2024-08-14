class PEvent_Researcher(object):
    name = ""
    id = ""
    event_name = ""
    year = ""
    nature = ""
    participation = ""

    def __init__(self):
        self.id = ""

    def getJson(self):

        PEvent_Researcher = {
            'name': self.name,
            'id': self.id,
            'event_name': self.event_name,
            'year': self.year,
            'participation': self.participation,
            'nature': self.nature
        }
        return  PEvent_Researcher
