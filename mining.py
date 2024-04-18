import json
import os
import time

import requests

import project
from Dao import sgbdSQL

project.project_env = "4"

url = "https://api.openalex.org/works/https://doi.org/"

doi = sgbdSQL.consultar_db(
    "SELECT doi, id FROM public.bibliographic_production WHERE doi IS NOT NULL OFFSET 100"
)
downloaded_productions = os.listdir("Files/json_doi/")

print(doi, downloaded_productions)
for Data in doi:
    if f"{Data[1]}.json" in downloaded_productions:
        response = requests.get(f"{url}{Data[0]}")
        if response.status_code == 200:
            with open(f"Files/json_doi/{Data[1]}.json", "w") as arquivo:
                json.dump(response.json(), arquivo)
            print("OK")
        else:
            print(Data[0])
        time.sleep(12)
    else:
        print(".")
