import requests
import pandas as pd
from Dao import sgbdSQL as db

import project

project.project_ = "8"


def downloadImage(id: str, name: str, lattes_id: str) -> None:
    url = "http://servicosweb.cnpq.br/wspessoa/servletrecuperafoto?tipo=1&id="

    response = requests.get(url + lattes_id)

    if response.status_code != 200:
        print("Erro para o pesquisador: {name}".format(name=name))

    image_name = "files/image_researcher/{id}.jpg".format(id=id)

    with open(image_name, "wb") as file:
        file.write(response.content)
        print("Sucesso para o psequisador: {name}".format(name=name))


if __name__ == "__main__":
    researcher_df = pd.DataFrame(
        db.consultar_db("SELECT id, name, lattes_10_id FROM researcher"),
        columns=["id", "name", "lattes_10_id"],
    )

    for Index, researcher in researcher_df.iterrows():
        downloadImage(
            id=researcher["id"],
            name=researcher["name"],
            lattes_id=researcher["lattes_10_id"],
        )
