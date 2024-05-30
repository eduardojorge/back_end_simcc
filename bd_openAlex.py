import json
import os
from pprint import pprint
import pandas as pd

import project
from Dao import sgbdSQL


def extract_institutions():
    project.project_env = "4"
    for json_path in os.listdir("Files/json_doi"):
        if json_path.endswith(".json"):
            with open(f"Files/json_doi/{json_path}", "r") as f:
                data = json.load(f)
                for author in data["authorships"]:
                    for institution in author["institutions"]:
                        pprint(institution)
                        print("\n")
            break


if __name__ == "__main__":
    extract_institutions()
