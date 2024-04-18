import json
import time

import requests

import project
from Dao import sgbdSQL

project.project_env = "4"

url = "https://api.openalex.org/works/https://doi.org/"

doi = sgbdSQL.consultar_db(
    "SELECT doi, id FROM public.bibliographic_production WHERE doi IS NOT NULL OFFSET 100"
)
for Data in doi:
    response = requests.get(f"{url}{Data[0]}")

    if response.status_code == 200:
        with open(f"Files/json_doi/{Data[1]}", "w") as arquivo:
            json.dump(response.json(), arquivo)
        print("OK")
    else:
        print(Data[0])
    time.sleep(15)
