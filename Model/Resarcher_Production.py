class Resarcher_Production(object):
    id = ""

    year = ""

    patent = ""
    software = ""
    brand = ""
    book = ""
    article_A1 = ""
    article_A2 = ""
    article_A3 = ""
    article_A4 = ""
    article_B1 = ""
    article_B2 = ""
    article_B3 = ""
    article_B4 = ""
    article_C = ""
    article_SQ = ""
    book_chapter = ""
    work_in_event = ""
    researcher = ""

    guidance_ic_a = ""
    guidance_ic_c = ""

    guidance_m_a = ""
    guidance_m_c = ""

    guidance_d_a = ""
    guidance_d_c = ""

    guidance_g_a = ""
    guidance_g_c = ""

    guidance_e_a = ""
    guidance_e_c = ""

    lattes_10_id = ""
    graduation = ""
    event_organization = ""
    participation_event = ""

    def __init__(self):
        self.id = ""

    def getJson(self):
        resarcher_Production = {
            "id": self.id,
            "patent": self.patent,
            "software": self.software,
            "brand": self.brand,
            "book": self.book,
            "article_A1": self.article_A1,
            "article_A2": self.article_A2,
            "article_A3": self.article_A3,
            "article_A4": self.article_A4,
            "article_B1": self.article_B1,
            "article_B2": self.article_B2,
            "article_B3": self.article_B3,
            "article_B4": self.article_B4,
            "article_C": self.article_C,
            "article_SQ": self.article_SQ,
            "book_chapter": self.book_chapter,
            "work_in_event": self.work_in_event,
            "researcher": self.researcher,
            "graduation": self.graduation,
            "guidance_ic_a": self.guidance_ic_a,
            "guidance_ic_c": self.guidance_ic_c,
            "guidance_m_a": self.guidance_m_a,
            "guidance_m_c": self.guidance_m_c,
            "guidance_d_a": self.guidance_d_a,
            "guidance_d_c": self.guidance_d_c,
            "guidance_g_a": self.guidance_g_a,
            "guidance_g_c": self.guidance_g_c,
            "guidance_e_a": self.guidance_e_a,
            "guidance_e_c": self.guidance_e_c,
            "lattes_10_id": self.lattes_10_id,
            "event_organization": self.event_organization,
            "participation_event": self.participation_event,
        }
        return resarcher_Production
