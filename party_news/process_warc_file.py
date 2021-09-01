from sqlalchemy.sql.selectable import ReturnsRows
from warcio.archiveiterator import ArchiveIterator
from pathlib import Path
import typing as t
import tldextract
from readability import Document
from datetime import datetime
from selectolax.parser import HTMLParser

from sqlmodel import Field, SQLModel, select
from party_news.partykeywords import check_if_string_contains_words


class WarcRecordModel(SQLModel, table=True):
    WARC_Record_ID: str = Field(primary_key=True)
    WARC_Date: datetime
    WARC_Target_URI: str
    registered_domain: str
    tld_suffix: str
    raw_html: str
    article_title: str
    article_summary: str


def get_text_selectolax(html):
    tree = HTMLParser(html)

    if tree.body is None:
        return None

    for tag in tree.css("script"):
        tag.decompose()
    for tag in tree.css("style"):
        tag.decompose()

    text = tree.body.text(separator="\n")
    return text


def iterate_warc_file(
    warc_file_path: Path,
    tld_suffixes: t.Iterable[str] = {"de"},
    blacklist_domains: t.Iterable = {"aktiencheck"},
    filtering_word_list: t.Iterable = set(),
) -> WarcRecordModel:
    tld_suffixes, blacklist_domains, filtering_word_list = (
        set(tld_suffixes),
        set(blacklist_domains),
        set(filtering_word_list),
    )

    with open(warc_file_path, "rb") as stream:
        for record in ArchiveIterator(stream):
            if record.rec_type == "response":
                uri = record.rec_headers.get_header("WARC-Target-URI")
                tld_extract = tldextract.extract(uri)
                if (
                    tld_extract.suffix in tld_suffixes
                    and tld_extract.domain not in blacklist_domains
                ):
                    try:
                        raw_html = record.content_stream().read().decode()
                    except UnicodeDecodeError:
                        raw_html = record.content_stream().read().decode("latin")
                    if check_if_string_contains_words(
                        raw_html.lower(), filtering_word_list
                    ):
                        record_dict = dict(record.rec_headers.headers)
                        doc = Document(raw_html)

                        yield WarcRecordModel(
                            WARC_Record_ID=record_dict["WARC-Record-ID"],
                            WARC_Date=record_dict["WARC-Date"],
                            WARC_Target_URI=record_dict["WARC-Target-URI"],
                            registered_domain=tld_extract.registered_domain,
                            tld_suffix=tld_extract.suffix,
                            raw_html=raw_html,
                            article_title=doc.title(),
                            article_summary=" ".join(
                                get_text_selectolax(doc.summary()).split()
                            ),
                        )


if __name__ == "__main__":
    word_list = {"spd", "cdu"}
    warc_file_iter = iterate_warc_file(
        "temp/crawl-data/CC-NEWS/2021/01/CC-NEWS-20210101014736-01421.warc.gz",
        filtering_word_list=word_list,
    )
    for record in warc_file_iter:
        print(record["registered_domain"])
