import pandas as pd
from nesta_ds_utils.loading_saving.S3 import download_obj
from afs_mission_goal import S3_BUCKET


def get_clean_chps_lookup() -> pd.DataFrame:
    """Retrieve the clean and processed data for the CHPS lookup data.

    Returns:
        pd.DataFrame: Returns a dataframe of the clean and processed data for the CHPS lookup data.
    """
    return download_obj(
        S3_BUCKET,
        "scotland/data/chps_aggregated/processed/chps_lookup.csv",
        download_as="dataframe",
    )
