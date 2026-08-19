import argparse
import json
from pathlib import Path
from urllib.parse import quote
from urllib.request import Request, urlopen


def create_hdfs_directory(base_url: str, path: str) -> None:
    url = f"{base_url}/webhdfs/v1{path}?op=MKDIRS"

    request = Request(url, method="PUT")

    with urlopen(request) as response:
        result = json.loads(response.read())

    if not result.get("boolean"):
        raise RuntimeError(f"Failed to create HDFS directory: {path}")


def upload_file(base_url: str, local_file: str, hdfs_path: str) -> None:
    file_path = Path(local_file)

    if not file_path.exists():
        raise FileNotFoundError(local_file)

    encoded_path = quote(hdfs_path)

    url = (
        f"{base_url}/webhdfs/v1"
        f"{encoded_path}"
        f"?op=CREATE&overwrite=false"
    )

    request = Request(
        url,
        data=file_path.read_bytes(),
        method="PUT",
    )

    with urlopen(request) as response:
        print(f"Uploaded {file_path} → {hdfs_path}")
        print(f"HTTP status: {response.status}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--namenode", default="http://namenode:9870")
    parser.add_argument("--local-file", required=True)
    parser.add_argument("--hdfs-path", required=True)

    args = parser.parse_args()

    hdfs_parent = str(Path(args.hdfs_path).parent)

    create_hdfs_directory(args.namenode, hdfs_parent)

    upload_file(
        args.namenode,
        args.local_file,
        args.hdfs_path,
    )