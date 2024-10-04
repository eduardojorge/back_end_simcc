# %%
import requests
import pandas as pd
import collections

collections.Callable = collections.abc.Callable


def make_requests_and_save_to_csv(total_results=21522, results_per_request=200):
    base_url = "https://jcr-clarivate.ez428.periodicos.capes.gov.br/api/jcr3/bwjournal/v1/search-result"
    cookies = {
        "_ga": "GA1.1.420722305.1690491034",
        "_ga_7SCLETN8D8": "GS1.1.1690491033.1.1.1690491733.0.0.0",
        "f8d43d5899660e032f00f861565869ba": "c72c13d77d673f6cf1d26cecf42805c0",
        "busca": "baseportitulo",
        "ML_SESSION_ID": "XXXB2XP6NMKBEB7734HGKDV76EEM2GPRRCEMQVF2SVQHJV6ACL",
        "_shibsession_64656661756c7468747470733a2f2f7777772e706572696f6469636f732e63617065732e676f762e62722f73686962626f6c657468": "_3242ecb948b8baac163e9877c64ace4e",
        "ezproxyn": "gQTOVFfOEqMbhzw",
        "ezproxy": "gQTOVFfOEqMbhzw",
        "ezproxyl": "gQTOVFfOEqMbhzw",
        "_ga_DSV7MH35Q7": "GS1.1.1690891602.2.1.1690891724.11.0.0",
        "clearStatus": "yes",
        "PSSID": "H3-rx2FfLb4F22f1AOM4q34b0gqxx3Tx2BG4x2BXXq-18x2dOVOPWwIVQFLXs0XqhOfBNgx3Dx3DsioBmzi4OTZBbXTmE56iiwx3Dx3D-qBgNuLRjcgZrPm66fhjx2Fmwx3Dx3D-h9tQNJ9Nv4eh45yLvkdX3gx3Dx3D",
        "_sp_ses.226f": "*",
        "_sp_id.226f": "d3ba35fc-160f-4214-b077-0e2f11982efa.1690891745.1.1690891814.1690891745.f813644d-54e6-499c-acb5-6fa18bb0b4fe",
    }
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "Content-Type": "application/json",
        "DNT": "1",
        "Origin": "https://jcr-clarivate.ez428.periodicos.capes.gov.br",
        "Pragma": "no-cache",
        "Referer": "https://jcr-clarivate.ez428.periodicos.capes.gov.br/jcr/browse-journals",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36 Edg/115.0.1901.188",
        "X-1P-INC-SID": "H3-rx2FfLb4F22f1AOM4q34b0gqxx3Tx2BG4x2BXXq-18x2dOVOPWwIVQFLXs0XqhOfBNgx3Dx3DsioBmzi4OTZBbXTmE56iiwx3Dx3D-qBgNuLRjcgZrPm66fhjx2Fmwx3Dx3D-h9tQNJ9Nv4eh45yLvkdX3gx3Dx3D",
        "sec-ch-ua": '"Not/A)Brand";v="99", "Microsoft Edge";v="115", "Chromium";v="115"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
    }
    json_data = {
        "journalFilterParameters": {
            "query": "",
            "journals": [],
            "categories": [],
            "publishers": [],
            "countryRegions": [],
            "citationIndexes": [
                "SCIE",
                "SSCI",
                "AHCI",
                "ESCI",
            ],
            "jcrYear": 2022,
            "categorySchema": "WOS",
            "openAccess": "N",
            "jifQuartiles": [],
            "jifRanges": [],
            "jifPercentileRanges": [],
            "jciRanges": [],
            "oaRanges": [],
            "issnJ20s": [],
        },
        "retrievalParameters": {
            "start": 1,
            "count": results_per_request,
            "sortBy": "jif2019",
            "sortOrder": "DESC",
        },
    }

    all_results = []
    num_requests = (total_results - 1) // results_per_request + 1

    for request_number in range(num_requests):
        start_index = request_number * results_per_request
        json_data["retrievalParameters"]["start"] = start_index

        response = requests.post(
            base_url, cookies=cookies, headers=headers, json=json_data
        )

        if response.status_code == 200:
            data = response.json()["data"]
            all_results.extend(data)
            print(f"Request {request_number + 1}/{num_requests} successful.")
        else:
            print(f"Request {request_number + 1}/{num_requests} failed.")

    df = pd.DataFrame(all_results)
    df.to_csv("Files/all_results_jcr.csv", index=False)
    print("All results saved to all_results_jcr.csv")


make_requests_and_save_to_csv()
