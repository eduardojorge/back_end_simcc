class GraduateProgram(object):
    graduate_program_id = ""
    code = ""
    name = ""
    area = ""
    modality = ""
    type = ""
    rating = ""
    state = ""
    city = ""
    instituicao = ""
    url_image = ""
    region = ""
    sigla = ""
    visible = ""
    qtd_colaborador = 0
    qtd_discente = 0
    qtd_permanente = 0

    def __init__(self):
        self.id = ""

    def getJson(self):
        graduate_program = {
            "graduate_program_id": str(self.graduate_program_id),
            "code": str(self.code),
            "name": str(self.name),
            "area": str(self.area),
            "modality": str(self.modality),
            "type": str(self.type),
            "rating": str(self.rating),
            "state": str(self.state),
            "city": str(self.city),
            "instituicao": str(self.instituicao),
            "url_image": self.url_image,
            "region": self.region,
            "sigla": str(self.sigla),
            "visible": str(self.visible),
            "qtd_colaborador": self.qtd_colaborador,
            "qtd_discente": self.qtd_discente,
            "qtd_permanente": self.qtd_permanente,
        }
        return graduate_program
