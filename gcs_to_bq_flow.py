from pathlib import Path

import pandas as pd
from prefect import flow, task
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket


@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS (data lake)"""
    gcs_path = f'data/{color}/{color}_tripdata_{year}-{month:02}.parquet'
    gcs_block = GcsBucket.load('zoom-gcs')
    gcs_block.get_directory(from_path=gcs_path, local_path=f'/')

    return Path(f'{gcs_path}')

@task(retries=3)
def write_bq(df: pd.DataFrame) -> None:
    gcp_credentials_block = GcpCredentials.load('zoom-gcp-creds')

    """Write DataFrame into BigQuery"""
    df.to_gbq(
        destination_table = 'dezoomcamp.rides',
        project_id = 'dtc-de-course-398512',
        credentials = gcp_credentials_block.get_credentials_from_service_account(),
        chunksize = 500_000,
        if_exists = 'append'
    )

@flow(log_prints=True)
def gcs_to_bq_parent_flow(
    months: list[int], year: int, color: str 
):
    rows = 0
    for month in months:
        local_path = extract_from_gcs(color, year, month)
        df = pd.read_parquet(local_path)
        rows += len(df)
        write_bq(df)
    print(f"rows processed: {rows}")


if __name__ == '__main__':
    months = [2, 3]
    year = 2019
    color = 'yellow'
    gcs_to_bq_parent_flow(months, year, color)