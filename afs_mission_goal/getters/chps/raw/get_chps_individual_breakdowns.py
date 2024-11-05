import pandas as pd
from nesta_ds_utils.loading_saving.S3 import download_obj
from afs_mission_goal import S3_BUCKET


def get_chps_data_ethnicity() -> pd.DataFrame:
    """Get the development concerns breakdown by ethnicity.Data is from the CHPS data.
    Returns:
        pd.DataFrame: A dataframe of the raw, unprocessed developmental concerns broken down by ethnicity.
    """
    path = "scotland/data/chps_aggregated/raw/chps_data_2024_t4_eth.csv"
    return download_obj(S3_BUCKET, path, download_as="dataframe")


def get_chps_data_lac() -> pd.DataFrame:
    """Get the development concerns breakdown by LAC (Looked After Children). Data is from the CHPS data.
    Returns:
        pd.DataFrame: A dataframe of the raw, unprocessed developmental concerns broken down by LAC.
    """
    path = "scotland/data/chps_aggregated/raw/chps_data_2024_t6_lac.csv"
    return download_obj(S3_BUCKET, path, download_as="dataframe")


def get_chps_data_eng() -> pd.DataFrame:
    """Get the development concerns breakdown by English as a first language. Data is from the CHPS data.
    Returns:
        pd.DataFrame: A dataframe of the raw, unprocessed developmental concerns broken down by English as a first language.
    """
    path = "scotland/data/chps_aggregated/raw/chps_data_2024_t8_eal.csv"
    return download_obj(S3_BUCKET, path, download_as="dataframe")
