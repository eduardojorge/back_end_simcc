class Book_Researcher(object):
    name = ""
    id = ""
    title = ""
    year = ""
    isbn = ""
    publishing_company = ""

    def __init__(self):
        self.id = ""

    def getJson(self):

        Book_Researcher = {
            "name": self.name,
            'id': self.id,
            'title': self.title,
            'year': self.year,
            'isbn': self.isbn,
            'publishing_company': self.publishing_company
        }
        return Book_Researcher
