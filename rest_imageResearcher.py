from Dao import sgbdSQL as db
import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv(override=True)


def download_image(id: str = None, name: str = None, lattes_id: str = None) -> None:
    if not name and not lattes_id:
        researcher_data = pd.DataFrame(
            db.consultar_db(
                "SELECT id, name, lattes_10_id FROM researcher WHERE id = '{filter}';".format(
                    filter=id
                )
            ),
            columns=["id", "name", "lattes_10_id"],
        )
        id = researcher_data["id"][0]
        name = researcher_data["name"][0]
        lattes_id = researcher_data["lattes_10_id"][0]

    url = "http://servicosweb.cnpq.br/wspessoa/servletrecuperafoto?tipo=1&id="

    response = requests.get(url + lattes_id)

    if response.status_code != 200:
        print("Erro para o pesquisador: {name}".format(name=name))

    image_name = f"Files/image_researcher/{id}.jpg".format(id=id)

    with open(image_name, "wb") as file:
        file.write(response.content)
        print("Sucesso para o psequisador: {name}".format(name=name))


if __name__ == "__main__":
    researcher_data = pd.DataFrame(
        db.consultar_db("SELECT id, name, lattes_10_id FROM researcher "),
        columns=["id", "name", "lattes_10_id"],
    )

    for Index, researcher in researcher_data.iterrows():
        download_image(
            id=researcher["id"],
            name=researcher["name"],
            lattes_id=researcher["lattes_10_id"],
        )
