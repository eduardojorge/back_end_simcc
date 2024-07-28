class GraduateProgram_Production(object):
    id = ""
    year = ""
    patent = ""
    software = ""
    brand = ""
    book = ""
    article = ""
    book_chapter = ""
    work_in_event = ""
    researcher = ""

    doctors = ""
    masters = ""
    graduate = ""
    pos_doctors = ""
    specialization = ""

    def __init__(self):
        self.id = ""

    def getJson(self):
        graduateProgram_Production = {
            "id": self.id,
            "patent": self.patent,
            "software": self.software,
            "brand": self.brand,
            "book": self.book,
            "article": self.article,
            "book_chapter": self.book_chapter,
            "work_in_event": self.work_in_event,
            "researcher": self.researcher,
            "doctors": self.doctors,
            "masters": self.masters,
            "graduate": self.graduate,
            "pos_doctors": self.pos_doctors,
            "specialization": self.specialization,
        }
        return graduateProgram_Production
