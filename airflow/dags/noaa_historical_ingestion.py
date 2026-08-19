from datetime import datetime
from pathlib import Path

import requests

from airflow import DAG
from airflow.operators.python import PythonOperator


DOWNLOAD_DIR = Path("/tmp/maritime_noaa")
HDFS_BASE = "/maritime/raw"


def get_file_config(context):
    dag_run = context["dag_run"]
    conf = dag_run.conf or {}

    year = int(conf.get("year", 2024))
    date = conf.get("date", f"{year}-01-01")

    filename = f"ais-{date}.csv.zst"

    return year, date, filename


def download_noaa_file(**context):
    year, date, filename = get_file_config(context)

    noaa_url = (
        f"https://noaaocm.blob.core.windows.net/"
        f"ais/csv2/csv{year}/{filename}"
    )

    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

    output_file = DOWNLOAD_DIR / filename

    response = requests.get(
        noaa_url,
        stream=True,
        timeout=120,
    )
    response.raise_for_status()

    with output_file.open("wb") as file:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            if chunk:
                file.write(chunk)

    print(f"Downloaded NOAA file to: {output_file}")
    print(f"Source URL: {noaa_url}")
    print(f"File size: {output_file.stat().st_size} bytes")


def upload_to_hdfs(**context):
    year, date, filename = get_file_config(context)

    local_file = DOWNLOAD_DIR / filename
    hdfs_directory = f"{HDFS_BASE}/{year}"
    hdfs_file = f"{hdfs_directory}/{filename}"

    if not local_file.exists():
        raise FileNotFoundError(
            f"Downloaded file not found: {local_file}"
        )

    import subprocess

    subprocess.run(
        [
            "curl",
            "-s",
            "-X",
            "PUT",
            "-L",
            "-H",
            "Content-Type: application/octet-stream",
            f"http://namenode:9870/webhdfs/v1{hdfs_file}"
            f"?op=CREATE&overwrite=true",
            "--data-binary",
            f"@{local_file}",
        ],
        check=True,
    )

    print(f"Uploaded to HDFS: {hdfs_file}")


with DAG(
    dag_id="noaa_historical_ingestion",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["maritime", "member1", "noaa"],
) as dag:

    download = PythonOperator(
        task_id="download_noaa_file",
        python_callable=download_noaa_file,
    )

    upload = PythonOperator(
        task_id="upload_to_hdfs",
        python_callable=upload_to_hdfs,
    )

    download >> upload
