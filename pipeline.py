from party_news.common_crawl import (
    download_warc_file,
    add_key_to_already_processed_list,
    list_already_processed_keys,
    get_unprocessed_s3_keys,
)
from party_news.process_warc_file import (
    iterate_warc_file,
    WarcRecordModel,
)
from party_news.partykeywords import get_partyname_wordlist
from sqlmodel import create_engine, SQLModel, Session
from tqdm.auto import tqdm

YEAR = "2021"
DOWNLOAD_FOLDER = "s3_data"
PARTY_NAMES_JSON = "./resources/party_names.json"
PROCESSED_KEYS_FILE = "./resources/processed.txt"
PARTIES = [
    "SPD",
    "CDU",
    "FDP",
    "Bündnis 90/Die Grünen",
    "DIE LINKE",
    "AfD",
]
DB_URI = "sqlite:///resources/database.sqlite"

engine = create_engine(DB_URI)
SQLModel.metadata.create_all(engine)


s3_keys = get_unprocessed_s3_keys(year=YEAR, processed_file_path=PROCESSED_KEYS_FILE)
# s3_keys = ["crawl-data/CC-NEWS/2021/01/CC-NEWS-20210101014736-01421.warc.gz"]
already_processed = list_already_processed_keys(PROCESSED_KEYS_FILE)

for s3_key in tqdm(s3_keys):
    if s3_key not in already_processed:
        warc_file_path = download_warc_file(s3_key, DOWNLOAD_FOLDER)

        warc_iter = iterate_warc_file(
            warc_file_path=warc_file_path,
            blacklist_domains={"aktiencheck", "onvista"},
            filtering_word_list=get_partyname_wordlist(
                PARTY_NAMES_JSON, filter_full_name=PARTIES
            ),
        )
        with Session(engine) as session:
            for record in warc_iter:
                session.add(record)
            session.commit()

        warc_file_path.unlink()
        add_key_to_already_processed_list(s3_key, PROCESSED_KEYS_FILE)
