import pandas as pd
from afs_mission_goal import DS_BUCKET
import s3fs

fs = s3fs.S3FileSystem()


def get_raw_usoc_peach() -> pd.DataFrame:
    """
    Function to read in the peach data from the Understanding Society dataset

    Returns:
        pd.DataFrame: The peach data
    """

    # Get all the relevant paths
    prefix = "data/raw/understanding_society/UKDA_9075_tab/"
    file_path = f"s3://{DS_BUCKET}/{prefix}xwavepeach.tab"

    # read in the data #
    with fs.open(file_path, "rb") as f:
        data = pd.read_csv(f, sep="\t", na_filter=False)

    return data
