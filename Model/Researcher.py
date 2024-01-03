class Researcher(object):
    id = ""
    name = ""
    among = ""
    articles = ""
    book_chapters = ""
    book = ""
    patent = ""
    software = ""
    brand = ""
    university = ""
    lattes_id = ""
    lattes_10_id = ""
    abstract = ""
    area = ""
    city = ""
    orcid = ""
    image_university = ""
    graduation = ""
    lattes_update = ""

    def __init__(self):
        self.id = ""

    def getJson(self):
        researcher = {
            "id": str(self.id),
            "name": str(self.name),
            "among": str(self.among),
            "articles": str(self.articles),
            "book_chapters": str(self.book_chapters),
            "book": str(self.book),
            "patent": str(self.patent),
            "software": str(self.software),
            "brand": str(self.brand),
            "university": str(self.university),
            "lattes_id": str(self.lattes_id),
            "lattes_10_id": str(self.lattes_10_id),
            "abstract": str(self.abstract),
            "area": str(self.area),
            "city": str(self.city),
            "orcid": str(self.orcid),
            "image": str(self.image_university),
            "graduation": str(self.graduation),
            "lattes_update": str(self.lattes_update),
        }
        return researcher
