import requests
import os
import json


def api_to_storage(**kwargs):

    base_url = "https://api.punkapi.com/v2/"
    per_page = 80
    directory = "object_storage"
    file_path = os.path.join(directory, "beers.jsonl")

    page = 1
    # while True:
    #     response = requests.get(base_url + f"beers?page={page}&per_page={per_page}")
    #     beers = response.json()
    #
    #     if len(beers) == 0:
    #         break
    #
    #     with open(file_path, "a", encoding="utf-8") as file:
    #         for beer in beers:
    #             json_data = json.dumps(beer, ensure_ascii=False)
    #             file.write(json_data + "\n")
    #
    #     page += 1
    with open(file_path, "w", encoding="utf-8") as file:
        while True:
            response = requests.get(base_url + f"beers?page={page}&per_page={per_page}")
            beers = response.json()

            if len(beers) == 0:
                break

            for beer in beers:
                json_data = json.dumps(beer, ensure_ascii=False)
                file.write(json_data + "\n")

            page += 1


if __name__ == '__main__':
    api_to_storage()
