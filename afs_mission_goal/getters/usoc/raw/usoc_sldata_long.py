import pandas as pd
import s3fs
from afs_mission_goal import DS_BUCKET
import s3fs

fs = s3fs.S3FileSystem()


def get_raw_usoc_sldata_long_paths(filename: str) -> list:
    """
    Function to get the paths to the Understanding Society data

    Args:
        filename (str): The filename of the data to be read in

    Returns:
        list: The paths to the data for different waves
    """

    prefix = "data/raw/understanding_society/UKDA_6931_tab/"
    fs = s3fs.S3FileSystem(anon=False)

    # Get relevant file paths
    paths = fs.glob(f"s3://{DS_BUCKET}/{prefix}[a-m]_{filename}_protect.tab")

    return paths


def get_raw_usoc_sldata_long(filename: str) -> dict[pd.DataFrame]:
    """
    Function to read in all wave specific data from a given file and return a dictionary of dataframes

    Args:
        filename (str): The filename of the data to be read in

    Returns:
        dict[pd.DataFrame]: The data read in and put into a dictionary
    """

    # Get relevant file paths
    paths = get_raw_usoc_sldata_long_paths(filename)

    # Read in the data as a dictionary of dataframes
    data = {}
    for path in paths:
        wave = path.split("_")[0][-1]
        with fs.open(path, "rb") as f:
            data[wave] = pd.read_csv(f, sep="\t", na_filter=False)

    return data
