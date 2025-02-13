import pandas as pd
import s3fs
from afs_mission_goal import DS_BUCKET

fs = s3fs.S3FileSystem(anon=False)


def get_raw_usoc_sllsoa_long_paths() -> list:
    """
    Function to read in the Understanding Society Special License LSOA data

    Returns:
        list: The paths to the data for different waves
    """

    # Get all the relevant paths
    prefix = "data/raw/understanding_society/UKDA_9169_tab/"

    paths = fs.glob(f"s3://{DS_BUCKET}/{prefix}[a-m]_lsoa21_protect.tab")

    return paths


def get_raw_usoc_sllsoa_long() -> dict[pd.DataFrame]:
    """
    Function to read in the Understanding Society Special License LSOA data and return a dictionary of dataframes

    Returns:
        dict[pd.DataFrame]: The data read in and put into a dictionary
    """

    # Get relevant file paths
    paths = get_raw_usoc_sllsoa_long_paths()

    # Read in the data as a dictionary of dataframes
    data = {}
    for path in paths:
        wave = path.split("_")[0][-1]
        with fs.open(path, "rb") as f:
            data[wave] = pd.read_csv(f, sep="\t", na_filter=False)

    return data
