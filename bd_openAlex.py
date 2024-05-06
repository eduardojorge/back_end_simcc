import json
import os
from pprint import pprint

import pandas as pd

import project
from Dao import sgbdSQL

if __name__ == "__main__":

    project.project_env = "4"
    i = 0
    for json_path in os.listdir("Files/json_doi"):
        if json_path.endswith(".json"):
            with open(f"Files/json_doi/{json_path}", "r") as f:
                try:
                    data = json.load(f)
                    pprint(data)
                    # sgbdSQL.consultar_db(
                    #     f"SELECT researcher_id FROM public.bibliographic_production WHERE id = '{json_path[:-5]}'"
                    # )
                    break
                except:
                    print(json_path)
