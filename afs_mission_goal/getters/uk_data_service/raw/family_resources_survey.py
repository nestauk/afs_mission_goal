import pandas as pd
from afs_mission_goal.utils.load_s3 import load_from_s3
from afs_mission_goal import DS_BUCKET


def get_raw_frs_data(dataset: str) -> pd.DataFrame:
    """Function to load the Family Resources Survey data from the UK Data Service.
    Args:
        dataset (str): The dataset to load. They're stored in a frs_datasets config.
    Returns:
        pd.DataFrame: Family Resources Survey data.
    """
    path = f"raw/family_resources_survey/2022/{dataset}.dta"
    return load_from_s3(path, bucket=DS_BUCKET)
