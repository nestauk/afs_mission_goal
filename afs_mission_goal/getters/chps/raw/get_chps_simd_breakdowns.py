import pandas as pd
from nesta_ds_utils.loading_saving.S3 import download_obj
from afs_mission_goal import S3_BUCKET


def get_chps_data_la_simd() -> pd.DataFrame:
    """Get the development concerns breakdown by LA and SIMD (Scottish Indices of Multiple Deprivation) data. Data is from the CHPS data.
    Returns:
        pd.DataFrame: A dataframe of the raw, unprocessed data of the developmental concerns broken down by LA and SIMD.
    """
    path = "scotland/data/chps_aggregated/raw/chps_data_2024_t1_la_simd.csv"
    return download_obj(S3_BUCKET, path, download_as="dataframe")


def get_chps_data_simd_sex() -> pd.DataFrame:
    """Get the development concerns breakdown by SIMD and sex data. Data is from the CHPS data.
    Returns:
        pd.DataFrame: A dataframe of the raw, unprocessed data of the developmental concerns broken down by SIMD and sex.
    """
    path = "scotland/data/chps_aggregated/raw/chps_data_2024_t2_simd_sex.csv"
    return download_obj(S3_BUCKET, path, download_as="dataframe")


def get_chps_data_simd_ethnicity() -> pd.DataFrame:
    """Get the development concerns breakdown by SIMD and ethnicity. Data is from the CHPS data.
    Returns:
        pd.DataFrame: A dataframe of the raw, unprocessed data of the developmental concerns broken down by SIMD and ethnicity.
    """
    path = "scotland/data/chps_aggregated/raw/chps_data_2024_t3_simd_eth.csv"
    return download_obj(S3_BUCKET, path, download_as="dataframe")


def get_chps_data_simd_lac() -> pd.DataFrame:
    """Get the development concerns breakdown by SIMD and LAC (Looked After Children). Data is from the CHPS data.
    Returns:
        pd.DataFrame: A dataframe of the raw, unprocessed data of the developmental concerns broken down by SIMD and LAC.
    """
    path = "scotland/data/chps_aggregated/raw/chps_data_2024_t5_simd_lac.csv"
    return download_obj(S3_BUCKET, path, download_as="dataframe")


def get_chps_data_simd_eng() -> pd.DataFrame:
    """Get the development concerns breakdown by SIMD and English as a first language. Data is from the CHPS data.a.
    Returns:
        pd.DataFrame: A dataframe of the raw, unprocessed data of the developmental concerns broken down by SIMD and English as a first language.
    """
    path = "scotland/data/chps_aggregated/raw/chps_data_2024_t7_simd_eal.csv"
    return download_obj(S3_BUCKET, path, download_as="dataframe")


def get_chps_data_simd_primary_carer_smoking() -> pd.DataFrame:
    """Get the development concerns breakdown by SIMD and exposure to primary carer smoke. Data is from the CHPS data.
    Returns:
        pd.DataFrame: A dataframe of the raw, unprocessed data of the developmental concerns breakdown by SIMD and exposure to primary carer smoke.
    """
    path = "scotland/data/chps_aggregated/raw/chps_data_2024_t9_simd_smok1.csv"
    return download_obj(S3_BUCKET, path, download_as="dataframe")


def get_chps_data_simd_secondhand_smoke() -> pd.DataFrame:
    """Get the development concerns breakdown by SIMD and exposure to second hand smoke. Data is from the CHPS data.
    Returns:
        pd.DataFrame: A dataframe of the raw, unprocessed data of the developmental concerns breakdown by SIMD and exposure to second hand smoke
    """
    path = "scotland/data/chps_aggregated/raw/chps_data_2024_t10_simd_smok2.csv"
    return download_obj(S3_BUCKET, path, download_as="dataframe")


def get_chps_data_simd_childcare() -> pd.DataFrame:
    """Get the development concerns breakdown by SIMD and childcare attendance. Data is from the CHPS data.
    Returns:
        pd.DataFrame: A dataframe of the raw, unprocessed of the developmental concerns broken down by childcare attendance and SIMD.
    """
    path = "scotland/data/chps_aggregated/raw/chps_data_2024_t11_simd_childcare.csv"
    return download_obj(S3_BUCKET, path, download_as="dataframe")
