import requests


def get_abgeordnete(page):
    params = {"pager_limit": 1000, "page": page}
    r = requests.get(
        "https://www.abgeordnetenwatch.de/api/v2/politicians", params=params
    )
    r_json = r.json()
    return r_json["data"], r_json["meta"]


