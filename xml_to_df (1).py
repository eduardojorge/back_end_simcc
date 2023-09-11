import os
import xml.etree.ElementTree as ET
import pandas as pd


class BaseExtractor:
    def __init__(self, xml_dir) -> None:
        self.xml_dir = xml_dir

    def _parse_xml(self, xml) -> tuple[str, ET.ElementTree]:
        identificador = str(xml.split(".")[0].zfill(16))
        tree = ET.parse(f"{self.xml_dir}/{xml}")
        return identificador, tree


class DadosGeraisExtractor(BaseExtractor):
    def __init__(self, xml_dir):
        self.xml_dir = xml_dir

    def extract(self):
        xmls = os.listdir(self.xml_dir)
        data = []

        for xml in xmls:
            identificador, tree = self._parse_xml(xml)
            tree = ET.parse(f"{self.xml_dir}/{xml}")
            root = tree.getroot()

            for geral in root.findall(".//DADOS-GERAIS"):
                row = {"cnpq_id": identificador}
                for k, v in geral.attrib.items():
                    row["data_atualizacao"] = pd.to_datetime(
                        root.attrib["DATA-ATUALIZACAO"]
                        + root.attrib["HORA-ATUALIZACAO"],
                        format="%d%m%Y%H%M%S",
                    )
                    key = k.replace(" ", "_").replace("-", "_").lower()
                    row[key] = v
                data.append(row)

        return pd.DataFrame(data)


class MestradoExtractor(BaseExtractor):
    def extract(self):
        xmls = os.listdir(self.xml_dir)
        data = []

        for xml in xmls:
            identificador, tree = self._parse_xml(xml)
            for mest in tree.findall(".//MESTRADO") + tree.findall(
                ".//MESTRADO-PROFISSIONALIZANTE"
            ):
                row = {"cnpq_id": identificador}
                row["tipo_curso"] = mest.tag
                for k, v in mest.attrib.items():
                    key = k.replace(" ", "_").replace("-", "_").lower()
                    row[key] = v
                data.append(row)
        df = pd.DataFrame(data)

        return df

class ArtigosPublicadosExtractor(BaseExtractor):
    def extract(self):
        xmls = os.listdir(self.xml_dir)
        data = []

        for xml in xmls:
            identificador, tree = self._parse_xml(xml)
            for artigo in tree.findall(".//ARTIGO-PUBLICADO"):
                row = {"cnpq_id": identificador}

                dados_basicos = artigo.find(".//DADOS-BASICOS-DO-ARTIGO")
                detalhamento = artigo.find(".//DETALHAMENTO-DO-ARTIGO")

                for k, v in dados_basicos.attrib.items():
                    key = k.replace(" ", "_").replace("-", "_").lower()
                    row[key] = v

                for k, v in detalhamento.attrib.items():
                    key = k.replace(" ", "_").replace("-", "_").lower()
                    row[key] = v

                data.append(row)

        return pd.DataFrame(data)

class DoutoradoExtractor(BaseExtractor):
    def extract(self):
        xmls = os.listdir(self.xml_dir)
        data = []

        for xml in xmls:
            identificador, tree = self._parse_xml(xml)
            for doutorado in tree.findall(".//DOUTORADO"):
                row = {"cnpq_id": identificador}

                for k, v in doutorado.attrib.items():
                    key = k.replace(" ", "_").replace("-", "_").lower()
                    row[key] = v

                data.append(row)
        
        return pd.DataFrame(data)

class PatenteExtractor(BaseExtractor):
    def extract(self):
        xmls = os.listdir(self.xml_dir)
        data = []

        for xml in xmls:
            identificador, tree = self._parse_xml(xml)
            for patente in tree.findall(".//REGISTRO-OU-PATENTE"):
                row = {"cnpq_id": identificador}

                for k, v in patente.attrib.items():
                    key = k.replace(" ", "_").replace("-", "_").lower()
                    row[key] = v

                data.append(row)
        
        return pd.DataFrame(data)

class AreaConhecimentoExtrator(BaseExtractor):
    def extract(self):
        xmls = os.listdir(self.xml_dir)
        data = []

        for xml in xmls:
            identificador, tree = self._parse_xml(xml)
            for area in tree.findall(".//AREA-DO-CONHECIMENTO-3"):
                row = {"cnpq_id": identificador}

                for k, v in area.attrib.items():
                    key = k.replace(" ", "_").replace("-", "_").lower()
                    row[key] = v

                data.append(row)
        
        return pd.DataFrame(data)

class SetoresAtividade(BaseExtractor):
    def extract(self):
        xmls = os.listdir(self.xml_dir)
        data = []

        for xml in xmls:
            identificador, tree = self._parse_xml(xml)
            for setor in tree.findall(".//SETORES-DE-ATIVIDADE"):
                row = {"cnpq_id": identificador}

                for k, v in setor.attrib.items():
                    key = k.replace(" ", "_").replace("-", "_").lower()
                    row[key] = v

                data.append(row)
        
        return pd.DataFrame(data)

class Graduacao(BaseExtractor):
    def extract(self):
        xmls = os.listdir(self.xml_dir)
        data = []

        for xml in xmls:
            identificador, tree = self._parse_xml(xml)
            for graduacao in tree.findall(".//GRADUACAO"):
                row = {"cnpq_id": identificador}

                for k, v in graduacao.attrib.items():
                    key = k.replace(" ", "_").replace("-", "_").lower()
                    row[key] = v

                data.append(row)
        
        return pd.DataFrame(data)

class PosDoutorado(BaseExtractor):
    def extract(self):
        xmls = os.listdir(self.xml_dir)
        data = []

        for xml in xmls:
            identificador, tree = self._parse_xml(xml)
            for pos_doutorado in tree.findall(".//POS-DOUTORADO"):
                row = {"cnpq_id": identificador}

                for k, v in pos_doutorado.attrib.items():
                    key = k.replace(" ", "_").replace("-", "_").lower()
                    row[key] = v

                data.append(row)
        
        return pd.DataFrame(data)
    

class LinhaPesquisa(BaseExtractor):
    def extract(self):
        xmls = os.listdir(self.xml_dir)
        data = []

        for xml in xmls:
            identificador, tree = self._parse_xml(xml)
            for linha in tree.findall(".//LINHA-DE-PESQUISA"):
                row = {"cnpq_id": identificador}

                for k, v in linha.attrib.items():
                    key = k.replace(" ", "_").replace("-", "_").lower()
                    row[key] = v

                data.append(row)
        
        return pd.DataFrame(data)
    
class ProjetoPesquisa(BaseExtractor):
    def extract(self):
        xmls = os.listdir(self.xml_dir)
        data = []

        for xml in xmls:
            identificador, tree = self._parse_xml(xml)
            for projeto in tree.findall(".//PROJETO-DE-PESQUISA"):
                row = {"cnpq_id": identificador}

                for k, v in projeto.attrib.items():
                    key = k.replace(" ", "_").replace("-", "_").lower()
                    row[key] = v

                data.append(row)
        
        return pd.DataFrame(data)

class AtuacaoProfissional(BaseExtractor):
    def extract(self):
        xmls = os.listdir(self.xml_dir)
        data = []

        for xml in xmls:
            identificador, tree = self._parse_xml(xml)
            for atuacao in tree.findall(".//ATUACAO-PROFISSIONAL"):
                row = {"cnpq_id": identificador}
                

                for k, v in atuacao.attrib.items():
                    key = k.replace(" ", "_").replace("-", "_").lower()
                    row[key] = v

                data.append(row)
        
        return pd.DataFrame(data)
    
class Vinculos(BaseExtractor):
    def extract(self):
        xmls = os.listdir(self.xml_dir)
        data = []

        for xml in xmls:
            identificador, tree = self._parse_xml(xml)
            for vinculo in tree.findall(".//VINCULOS"):
                row = {"cnpq_id": identificador}

                for k, v in vinculo.attrib.items():
                    key = k.replace(" ", "_").replace("-", "_").lower()
                    row[key] = v

                data.append(row)
        
        return pd.DataFrame(data)
    
class TrabalhosEventos(BaseExtractor):
    def extract(self):
        xmls = os.listdir(self.xml_dir)
        data = []

        for xml in xmls:
            identificador, tree = self._parse_xml(xml)
            for trabalho in tree.findall(".//TRABALHO-EM-EVENTOS"):
                row = {"cnpq_id": identificador}
                dados_basicos = trabalho.find(".//DADOS-BASICOS-DO-TRABALHO")
                detalhes_trabalho = trabalho.find(".//DETALHAMENTO-DO-TRABALHO")
            
                for k, v in dados_basicos.attrib.items():
                    key = k.replace(" ", "_").replace("-", "_").lower()
                    row[key] = v

                for k, v in detalhes_trabalho.attrib.items():
                    key = k.replace(" ", "_").replace("-", "_").lower()
                    row[key] = v

                data.append(row)

        return pd.DataFrame(data)
    
class LivrosPublicados(BaseExtractor):
    def extract(self):
        xmls = os.listdir(self.xml_dir)
        data = []

        for xml in xmls:
            identificador, tree = self._parse_xml(xml)
            for livro in tree.findall(".//LIVRO-PUBLICADO-OU-ORGANIZADO"):
                row = {"cnpq_id": identificador}
                dados_basicos = livro.find(".//DADOS-BASICOS-DO-LIVRO")
                detalhes_livro = livro.find(".//DETALHAMENTO-DO-LIVRO")
            
                for k, v in dados_basicos.attrib.items():
                    key = k.replace(" ", "_").replace("-", "_").lower()
                    row[key] = v

                for k, v in detalhes_livro.attrib.items():
                    key = k.replace(" ", "_").replace("-", "_").lower()
                    row[key] = v

                data.append(row)

        return pd.DataFrame(data)

class CapituloPublicado(BaseExtractor):
    def extract(self):
        xmls = os.listdir(self.xml_dir)
        data = []

        for xml in xmls:
            identificador, tree = self._parse_xml(xml)
            for capitulo in tree.findall(".//CAPITULO-DE-LIVRO-PUBLICADO"):
                row = {"cnpq_id": identificador}
                dados_basicos = capitulo.find(".//DADOS-BASICOS-DO-CAPITULO")
                detalhes_capitulo = capitulo.find(".//DETALHAMENTO-DO-CAPITULO")
            
                for k, v in dados_basicos.attrib.items():
                    key = k.replace(" ", "_").replace("-", "_").lower()
                    row[key] = v

                for k, v in detalhes_capitulo.attrib.items():
                    key = k.replace(" ", "_").replace("-", "_").lower()
                    row[key] = v

                data.append(row)

        return pd.DataFrame(data)

class TextoJornal(BaseExtractor):
    def extract(self):
        xmls = os.listdir(self.xml_dir)
        data = []

        for xml in xmls:
            identificador, tree = self._parse_xml(xml)
            for texto in tree.findall(".//TEXTO-EM-JORNAL-OU-REVISTA"):
                row = {"cnpq_id": identificador}
                dados_basicos = texto.find(".//DADOS-BASICOS-DO-TEXTO")
                detalhes_texto = texto.find(".//DETALHAMENTO-DO-TEXTO")
            
                for k, v in dados_basicos.attrib.items():
                    key = k.replace(" ", "_").replace("-", "_").lower()
                    row[key] = v

                for k, v in detalhes_texto.attrib.items():
                    key = k.replace(" ", "_").replace("-", "_").lower()
                    row[key] = v

                data.append(row)

        return pd.DataFrame(data)
    
class OutrasProducoes(BaseExtractor):
    def extract(self):
        xmls = os.listdir(self.xml_dir)
        data = []

        for xml in xmls:
            identificador, tree = self._parse_xml(xml)
            for producao in tree.findall(".//OUTRA-PRODUCAO-BIBLIOGRAFICA"):
                row = {"cnpq_id": identificador}
                dados_basicos = producao.find(".//DADOS-BASICOS-DE-OUTRA-PRODUCAO")
                detalhes_producao = producao.find(".//DETALHAMENTO-DE-OUTRA-PRODUCAO")
            
                for k, v in dados_basicos.attrib.items():
                    key = k.replace(" ", "_").replace("-", "_").lower()
                    row[key] = v

                for k, v in detalhes_producao.attrib.items():
                    key = k.replace(" ", "_").replace("-", "_").lower()
                    row[key] = v

                data.append(row)

        return pd.DataFrame(data)
    
class SoftwareExtractor(BaseExtractor):
    def extract(self):
        xmls = os.listdir(self.xml_dir)
        data = []

        for xml in xmls:
            identificador, tree = self._parse_xml(xml)
            for software in tree.findall(".//SOFTWARE"):
                row = {"cnpq_id": identificador}
                dados_basicos = software.find(".//DADOS-BASICOS-DO-SOFTWARE")
                detalhes_software = software.find(".//DETALHAMENTO-DO-SOFTWARE")
            
                for k, v in dados_basicos.attrib.items():
                    key = k.replace(" ", "_").replace("-", "_").lower()
                    row[key] = v

                for k, v in detalhes_software.attrib.items():
                    key = k.replace(" ", "_").replace("-", "_").lower()
                    row[key] = v

                data.append(row)

        return pd.DataFrame(data)

class ProdutoTecnologico(BaseExtractor):
    def extract(self):
        xmls = os.listdir(self.xml_dir)
        data = []

        for xml in xmls:
            identificador, tree = self._parse_xml(xml)
            for produto in tree.findall(".//PRODUTO-TECNOLOGICO"):
                row = {"cnpq_id": identificador}
                dados_basicos = produto.find(".//DADOS-BASICOS-DO-PRODUTO-TECNOLOGICO")
                detalhes_produto = produto.find(".//DETALHAMENTO-DO-PRODUTO-TECNOLOGICO")
            
                for k, v in dados_basicos.attrib.items():
                    key = k.replace(" ", "_").replace("-", "_").lower()
                    row[key] = v

                for k, v in detalhes_produto.attrib.items():
                    key = k.replace(" ", "_").replace("-", "_").lower()
                    row[key] = v

                data.append(row)

        return pd.DataFrame(data)

class PatenteExtractor(BaseExtractor):
    def extract(self):
        xmls = os.listdir(self.xml_dir)
        data = []

        for xml in xmls:
            identificador, tree = self._parse_xml(xml)
            for patente in tree.findall(".//PATENTE"):
                row = {"cnpq_id": identificador}
                dados_basicos = patente.find(".//DADOS-BASICOS-DA-PATENTE")
                detalhes_patente = patente.find(".//DETALHAMENTO-DA-PATENTE")
            
                for k, v in dados_basicos.attrib.items():
                    key = k.replace(" ", "_").replace("-", "_").lower()
                    row[key] = v

                for k, v in detalhes_patente.attrib.items():
                    key = k.replace(" ", "_").replace("-", "_").lower()
                    row[key] = v

                data.append(row)

        return pd.DataFrame(data)


class DesenhoIndustrial(BaseExtractor):
    def extract(self):
        xmls = os.listdir(self.xml_dir)
        data = []

        for xml in xmls:
            identificador, tree = self._parse_xml(xml)
            for desenho in tree.findall(".//DESENHO-INDUSTRIAL"):
                row = {"cnpq_id": identificador}
                dados_basicos = desenho.find(".//DADOS-BASICOS-DO-DESENHO-INDUSTRIAL")
                detalhes_desenho = desenho.find(".//DETALHAMENTO-DO-DESENHO-INDUSTRIAL")
            
                for k, v in dados_basicos.attrib.items():
                    key = k.replace(" ", "_").replace("-", "_").lower()
                    row[key] = v

                for k, v in detalhes_desenho.attrib.items():
                    key = k.replace(" ", "_").replace("-", "_").lower()
                    row[key] = v

                data.append(row)

        return pd.DataFrame(data)

class Marca(BaseExtractor):
    def extract(self):
        xmls = os.listdir(self.xml_dir)
        data = []

        for xml in xmls:
            identificador, tree = self._parse_xml(xml)
            for marca in tree.findall(".//MARCA"):
                row = {"cnpq_id": identificador}
                dados_basicos = marca.find(".//DADOS-BASICOS-DA-MARCA")
                detalhes_marca = marca.find(".//DETALHAMENTO-DA-MARCA")
            
                for k, v in dados_basicos.attrib.items():
                    key = k.replace(" ", "_").replace("-", "_").lower()
                    row[key] = v

                for k, v in detalhes_marca.attrib.items():
                    key = k.replace(" ", "_").replace("-", "_").lower()
                    row[key] = v

                data.append(row)

        return pd.DataFrame(data)

class Topografia(BaseExtractor):
    def extract(self):
        xmls = os.listdir(self.xml_dir)
        data = []

        for xml in xmls:
            identificador, tree = self._parse_xml(xml)
            for topografia in tree.findall(".//TOPOGRAFIA-DE-CIRCUITO-INTEGRADO"):
                row = {"cnpq_id": identificador}
                dados_basicos = topografia.find(".//DADOS-BASICOS-DE-TOPOGRAFIA-DE-CIRCUITO-INTEGRADO")
                detalhes_topografia = topografia.find(".//DETALHAMENTO-DE-TOPOGRAFIA-DE-CIRCUITO-INTEGRADO")
            
                for k, v in dados_basicos.attrib.items():
                    key = k.replace(" ", "_").replace("-", "_").lower()
                    row[key] = v

                for k, v in detalhes_topografia.attrib.items():
                    key = k.replace(" ", "_").replace("-", "_").lower()
                    row[key] = v

                data.append(row)

        return pd.DataFrame(data)

class ProcessosTecnicas(BaseExtractor):
    def extract(self):
        xmls = os.listdir(self.xml_dir)
        data = []

        for xml in xmls:
            identificador, tree = self._parse_xml(xml)
            for processo in tree.findall(".//PROCESSOS-OU-TECNICAS"):
                row = {"cnpq_id": identificador}
                dados_basicos = processo.find(".//DADOS-BASICOS-DO-PROCESSOS-OU-TECNICAS")
                detalhes_processo = processo.find(".//DETALHAMENTO-DO-PROCESSOS-OU-TECNICAS")
            
                for k, v in dados_basicos.attrib.items():
                    key = k.replace(" ", "_").replace("-", "_").lower()
                    row[key] = v

                for k, v in detalhes_processo.attrib.items():
                    key = k.replace(" ", "_").replace("-", "_").lower()
                    row[key] = v

                data.append(row)

        return pd.DataFrame(data)
