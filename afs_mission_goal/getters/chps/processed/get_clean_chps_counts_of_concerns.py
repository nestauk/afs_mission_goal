import pandas as pd
from nesta_ds_utils.loading_saving.S3 import download_obj
from afs_mission_goal.pipeline.cleaning_functions_chps import change_dtype
from afs_mission_goal import S3_BUCKET


def get_chps_data_counts_of_concerns(
    characteristic: str, keep_suppression=False
) -> pd.DataFrame:
    """Retrieve the clean and processed data for the specified characteristic. The individual table options are 'ethnicity', 'lac' or 'eng' where 'lac' stands for Looked After Children and 'eng' is English as a first language.

    Args:
        characteristic (str): Options are 'simd', 'sex', 'ethnicity' or 'eng'.
        keep_suppression (bool): Whether to keep suppressed values or not. If True, the suppressed values will stay as a string "<5", if False, they will be converted to 5.

    Returns:
        pd.DataFrame: Returns a dataframe of the clean and processed data for the specified characteristic.
    """
    df = download_obj(
        S3_BUCKET,
        f"scotland/data/chps_aggregated/processed/{characteristic}_counts_of_concerns.csv",
        download_as="dataframe",
    )
    return change_dtype(df, keep_suppression=keep_suppression)
