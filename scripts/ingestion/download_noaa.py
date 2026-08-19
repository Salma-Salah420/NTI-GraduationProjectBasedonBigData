import argparse
from pathlib import Path
from urllib.request import urlopen


def download_file(url: str, output_path: str) -> None:
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)

    if output.exists():
        print(f"File already exists: {output}")
        return

    print(f"Downloading: {url}")
    print(f"Destination: {output}")

    with urlopen(url) as response, open(output, "wb") as file:
        while chunk := response.read(1024 * 1024):
            file.write(chunk)

    print(f"Download completed: {output}")
    print(f"Size: {output.stat().st_size / (1024 * 1024):.2f} MB")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--url", required=True)
    parser.add_argument("--output", required=True)

    args = parser.parse_args()

    download_file(args.url, args.output)