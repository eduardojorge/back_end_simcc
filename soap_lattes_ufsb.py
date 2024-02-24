from zeep import Client
import pandas as pd
import project as project
from Dao import sgbdSQL as db


def get_id_cnpq(name: str = str(), date: str = str(), CPF: str = str()):
    CPF = extract_int(CPF)
    resultado = client.service.getIdentificadorCNPq(
        nomeCompleto=name, dataNascimento=date, cpf=CPF
    )
    if resultado:
        return resultado.zfill(16)
    else:
        return resultado


def extract_int(string: str) -> str:
    sanitized_string = str()

    for character in string:
        if character.isdigit():
            sanitized_string += character

    return sanitized_string


if __name__ == "__main__":

    client = Client("http://servicosweb.cnpq.br/srvcurriculo/WSCurriculo?wsdl")

    quant_loss = 0
    curriculos_perdidos = list
    df = pd.read_excel("Files/researcher_ufsb.xlsx")

    for Index, Data in df.iterrows():
        print(f"Loading: {Index}")
        lattes_id = get_id_cnpq(CPF=extract_int(str(Data["CPF"])))

        if lattes_id:
            script_sql = f"""
                INSERT INTO public.researcher(
                	name, lattes_id, institution_id)
                	VALUES ('{Data["Nome"]}', '498cadc8-b8f6-4008-902e-76281109187d', '{lattes_id}');
                """
            db.consultar_db()
        else:
            quant_loss += 1
            curriculos_perdidos.append(str(Data["CPF"]))

    print(f"Fim!\nCurriculos perdidos: {quant_loss}")
