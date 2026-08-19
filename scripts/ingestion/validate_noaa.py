import argparse
from pathlib import Path


def validate_file(file_path: str) -> None:
    path = Path(file_path)

    if not path.exists():
        raise FileNotFoundError(f"File does not exist: {path}")

    if path.stat().st_size == 0:
        raise ValueError(f"File is empty: {path}")

    if not path.name.endswith(".csv.zst"):
        raise ValueError(f"Unexpected file format: {path}")

    print(f"Validation passed: {path}")
    print(f"Size: {path.stat().st_size / (1024 * 1024):.2f} MB")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--file", required=True)

    args = parser.parse_args()

    validate_file(args.file)