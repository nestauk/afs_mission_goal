import pandas as pd
from afs_mission_goal import DS_BUCKET
import s3fs

fs = s3fs.S3FileSystem()


def get_raw_slxwaveid_xwav(filename: str) -> pd.DataFrame:
    """
    Function to read in the cross-wave data from the Understanding Society dataset

    Args:
        filename (str): The filename of the data to be read in

    Returns:
        pd.DataFrame: The data read in and processed
    """

    # Read in the data
    file_path = f"s3://{DS_BUCKET}/data/raw/understanding_society/UKDA_6931_tab/{filename}_protect.tab"
    with fs.open(file_path, "rb") as f:
        data = pd.read_csv(f, sep="\t", na_filter=False)

    return data
