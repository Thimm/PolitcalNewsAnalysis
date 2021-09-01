import json
from pathlib import Path
import typing as t


def load_party_names_json(path):
    party_names = json.loads(Path(path).read_text())
    return party_names


def filter_party_names_json(path, short_party_names_list):
    return [
        x
        for x in load_party_names_json(path)
        if x["short_name"] in short_party_names_list
    ]


def check_if_string_contains_words(string, words):
    for word in words:
        if word in string:
            return True
    return False


def get_partyname_wordlist(
    json_path: Path,
    filter_full_name: t.List = [],
    name_keys: t.List = ["full_name", "label", "short_name", "other_names"],
    lowercase: bool = False,
    add_spaces: bool = False,
):
    """Create a set of all possible and abreviatoions of party names"""
    parties = json.loads(Path(json_path).read_text())
    if filter_full_name:
        parties = [x for x in parties if x.get("full_name") in filter_full_name]

    party_names = set()
    for party in parties:
        for key in name_keys:
            name = party.get(key)
            if isinstance(name, str):
                party_names.add(name)
            elif isinstance(name, list):
                for n in name:
                    party_names.add(n)

    if lowercase:
        party_names = {x.lower() for x in party_names}
    if add_spaces:
        party_names = {f" {x} " for x in party_names}
    return party_names


if __name__ == "__main__":
    print(
        get_partyname_wordlist(
            json_path="resources/party_keywords.json",
            filter_full_name=[
                "SPD",
                "CDU",
                "FDP",
                "Bündnis 90/Die Grünen",
                "DIE LINKE",
                "AfD",
            ],
        )
    )
