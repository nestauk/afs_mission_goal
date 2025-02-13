import pandas as pd
from afs_mission_goal import DS_BUCKET


def get_raw_inflation() -> pd.DataFrame:
    """
    Function to read in the inflation data

    Returns:
        pd.DataFrame: The inflation data
    """

    prefix = "data/raw/understanding_society/"
    file_path = f"s3://{DS_BUCKET}/{prefix}series-170424.csv"

    data = pd.read_csv(file_path, skiprows=7)

    return data
