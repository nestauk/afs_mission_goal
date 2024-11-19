import pandas as pd
from nesta_ds_utils.loading_saving.S3 import download_obj
from afs_mission_goal import S3_BUCKET


def get_chps_lookup() -> pd.DataFrame:
    """Get the CHPS lookup data.
    Returns:
        pd.DataFrame: A dataframe of the raw, unprocessed CHPS lookup data
    """
    path = "scotland/data/chps_aggregated/raw/chps_data_2024_lookups.csv"
    return download_obj(S3_BUCKET, path, download_as="dataframe")
