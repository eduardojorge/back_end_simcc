from dataclasses import dataclass, asdict


@dataclass
class Researcher:
    id: str = str()
    name: str = str()
    lattes_id: str = str()
    among: str = str()
    articles: str = str()
    book_chapters: str = str()
    book: str = str()
    patent: str = str()
    software: str = str()
    brand: str = str()
    university: str = str()
    abstract: str = str()
    area: str = str()
    city: str = str()
    orcid: str = str()
    image_university: str = str()
    graduation: str = str()
    lattes_update: str = str()

    def getJson(self):
        return asdict(self)
