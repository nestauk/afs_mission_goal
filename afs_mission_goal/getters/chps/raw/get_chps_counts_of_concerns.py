import pandas as pd
from nesta_ds_utils.loading_saving.S3 import download_obj
from afs_mission_goal import S3_BUCKET


def get_chps_data_simd_counts_of_concerns() -> pd.DataFrame:
    """Get the SIMD (Scottish Indices of Multiple Deprivation) broken down by counts of developmental concerns. Data is from the CHPS data.
    Returns:
        pd.DataFrame: A dataframe of the raw, unprocessed counts of developmental concerns broken down by SIMD.
    """
    path = "scotland/data/chps_aggregated/raw/chps_data_2024_t12_simd_counts_concerns_UPDATED.csv"
    return download_obj(S3_BUCKET, path, download_as="dataframe")


def get_chps_data_sex_counts_of_concerns() -> pd.DataFrame:
    """Get the sex data broken down by counts of developmental concerns. Data is from the CHPS data.
    Returns:
        pd.DataFrame: A dataframe of the raw, unprocessed counts of developmental concerns for children broken down by sex.
    """
    path = (
        "scotland/data/chps_aggregated/raw/chps_data_2024_t13_sex_counts_concerns.csv"
    )
    return download_obj(S3_BUCKET, path, download_as="dataframe")


def get_chps_data_ethnicity_counts_of_concerns() -> pd.DataFrame:
    """Get the ethnicity data broken down by counts of developmental concerns. Data is from the CHPS data.
    Returns:
        pd.DataFrame: A dataframe of the raw, unprocessed counts of developmental concerns for children broken down by ethnicity.
    """
    path = (
        "scotland/data/chps_aggregated/raw/chps_data_2024_t14_eth_counts_concerns.csv"
    )
    return download_obj(S3_BUCKET, path, download_as="dataframe")


def get_chps_data_eng_counts_concerns() -> pd.DataFrame:
    """Get the English as a first language data broken down by counts of developmental concerns. Data is from the CHPS data.
    Returns:
        pd.DataFrame: A dataframe of the raw, unprocessed counts of concerns for children with English as a first language.
    """
    path = (
        "scotland/data/chps_aggregated/raw/chps_data_2024_t15_eal_counts_concerns.csv"
    )
    return download_obj(S3_BUCKET, path, download_as="dataframe")
