import boto3
from botocore import UNSIGNED
from botocore.client import Config
from pathlib import Path
from tqdm.auto import tqdm


def list_s3_common_crawl_news_dataset_keys(year="2021"):
    s3 = boto3.resource("s3", config=Config(signature_version=UNSIGNED))
    bucket = s3.Bucket("commoncrawl")
    keys = []
    for item in bucket.objects.filter(Prefix=f"crawl-data/CC-NEWS/{year}"):
        keys.append(item.key)
    return keys[::-1]


def add_key_to_already_processed_list(key, processed_file_path):
    path = Path(processed_file_path)
    if not path.exists():
        path.touch()
    with open(path, "r") as f:
        lines = f.readlines()
    lines.append(f"{key}\n")
    with open(Path(processed_file_path), "w") as f:
        f.writelines(lines)


def list_already_processed_keys(processed_file_path):
    path = Path(processed_file_path)
    if not path.exists():
        raise (FileExistsError(f"The file '{processed_file_path}' does not exist."))
    with open(path, "r") as f:
        lines = f.readlines()
    return list({x.strip() for x in lines})


def download_warc_file(s3_key, destination_folder):
    def hook(t):
        def inner(bytes_amount):
            t.update(bytes_amount)

        return inner

    destination_path = Path(f"{str(destination_folder)}/{s3_key}")
    destination_path.parent.mkdir(parents=True, exist_ok=True)

    if destination_path.exists():
        print("file has already been downloaded")
        return destination_path
    else:
        s3 = boto3.resource("s3", config=Config(signature_version=UNSIGNED))
        file_object = s3.Object("commoncrawl", s3_key)
        filesize = file_object.content_length
        with tqdm(total=filesize, unit="B", unit_scale=True, desc=s3_key) as t:
            file_object.download_file(str(destination_path), Callback=hook(t))
        return destination_path


def get_unprocessed_s3_keys(year, processed_file_path):
    keys = list_s3_common_crawl_news_dataset_keys(year)
    processed_keys = list_already_processed_keys(processed_file_path)
    return [x for x in keys if x not in processed_keys]


if __name__ == "__main__":
    # print(list_s3_common_crawl_news_dataset_keys(year="2021"))
    processed_file_path = Path("processed_testing.txt")
    try:
        print(list_already_processed_keys(processed_file_path))
    except FileExistsError as e:
        print(e)
    add_key_to_already_processed_list("123", processed_file_path)
    print(list_already_processed_keys(processed_file_path))

    processed_file_path.unlink()
    download_warc_file(
        "crawl-data/CC-NEWS/2021/01/CC-NEWS-20210101014736-01421.warc.gz", "temp"
    )
