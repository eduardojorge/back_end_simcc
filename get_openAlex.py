import os
import time
import json
import requests
from Dao import sgbdSQL as db


def scrapping_article_data():
    openAlex_url = "https://api.openalex.org/works/https://doi.org/"

    script_sql = """
        SELECT 
            doi, 
            id 
        FROM 
            public.bibliographic_production 
        WHERE 
            doi IS NOT NULL;
        """

    doi_list = db.consultar_db(script_sql)
    json_articles = os.listdir("Files/openAlex_article/")

    for data in doi_list:
        if f"{data[1]}.json" not in json_articles:
            response = requests.get(f"{openAlex_url}{data[0]}")
            if response.status_code == 200:
                with open(f"Files/openAlex_article/{data[1]}.json", "w") as arquivo:
                    json.dump(response.json(), arquivo)
                print("[201] - CREATED")
            else:
                with open(f"Files/json_doi/{data[1]}.json", "w") as arquivo:
                    arquivo.write(str(response.status_code))
                print("[404] - NOT FOUND")
            time.sleep(7)
        else:
            print("[200] - OK")


def scrapping_researcher_data():
    openAlex_url = "https://api.openalex.org/authors/orcid:"

    script_sql = """
        SELECT 
            orcid,
            id 
        FROM 
            researcher 
        WHERE 
            orcid IS NOT NULL;
        """

    doi_list = db.consultar_db(script_sql)
    json_articles = os.listdir("Files/openAlex_researcher/")

    for data in doi_list:
        if f"{data[1]}.json" not in json_articles:
            response = requests.get(f"{openAlex_url}{data[0]}")
            if response.status_code == 200:
                with open(f"Files/openAlex_researcher/{data[1]}.json", "w") as arquivo:
                    json.dump(response.json(), arquivo)
                print("[201] - CREATED")
            else:
                with open(f"Files/json_doi/{data[1]}.json", "w") as arquivo:
                    arquivo.write(str(response.status_code))
                print("[404] - NOT FOUND")
            time.sleep(7)
        else:
            print("[200] - OK")


if __name__ == "__main__":
    scrapping_researcher_data()
    scrapping_article_data()
