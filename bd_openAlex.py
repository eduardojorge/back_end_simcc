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
                    for item in data["authorships"]:
                        pprint(item)
                    # for item in data["authorships"].items():
                    #     print(item)

                    break
                except:
                    print(json_path)
