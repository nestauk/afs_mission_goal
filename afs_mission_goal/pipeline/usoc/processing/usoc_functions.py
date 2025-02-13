"""
Functions to read in raw USOC data


Created on Thu Sep  5 16:18:03 2024

@author: jones, edited: gillam
"""

import re
import pandas as pd
from afs_mission_goal.getters.usoc.raw.slxwaveid_xwav import get_raw_slxwaveid_xwav
from afs_mission_goal.getters.usoc.raw.usoc_sldata_long import (
    get_raw_usoc_sldata_long_paths,
)
from afs_mission_goal.getters.usoc.raw.usoc_sllsoa_long import (
    get_raw_usoc_sllsoa_long_paths,
)
from afs_mission_goal.getters.usoc.raw.usoc_peach import get_raw_usoc_peach
import s3fs

fs = s3fs.S3FileSystem()


def collect_columns(data: pd.DataFrame) -> pd.DataFrame:
    """
    Function to collect the id variables from the data

    Args:
        data (pd.DataFrame): The data to be processed

    Returns:
        pd.DataFrame: The data with the id variables converted to numeric
    """

    # Make sure all  vars are in the correct format
    # Collect the id variables
    idvars = (
        data.columns[
            data.columns.str.contains("pidp")
            | data.columns.str.contains("hidp")
            | data.columns.str.endswith("pno")
        ]
        .unique()
        .tolist()
    )

    # Collect the variables ending in 'pno'
    pno_cols = [col for col in data.columns if col.endswith("pno")]

    # Combine the id variables, getting the unique ones
    vars = list(set(idvars + pno_cols))

    # Convert id variables to numeric
    data[vars] = data[vars].apply(pd.to_numeric, errors="coerce")

    return data


def filter_columns(
    data: pd.DataFrame,
    fixed_cols: list = [],
    start_cols: list = [],
    end_cols: list = [],
) -> pd.DataFrame:
    """
    Function to filter the columns of the data

    Args:
        data (pd.DataFrame): The data to be processed
        fixed_cols (list): A list of the fixed columns to be kept
        start_cols (list): A list of the columns to be kept that start with a certain string
        end_cols (list): A list of the columns to be kept that end with a certain string

    Returns:
        pd.DataFrame: The data with the columns filtered
    """

    # Determine which cols to keep
    if start_cols:
        start = [col for col in data.columns if col.startswith(tuple(start_cols))]
    else:
        start = []
    if end_cols:
        end = [col for col in data.columns if col.endswith(tuple(end_cols))]
    else:
        end = []

    # Keep only relevant cols
    keep_cols = fixed_cols + start + end
    if keep_cols:
        keep_cols = data.columns.intersection(keep_cols)
    else:
        keep_cols = data.columns

    data = data[keep_cols]

    return data


def get_usoc_slxwaveid_xwav(
    filename: str, fixed_cols: list = [], start_cols: list = [], end_cols: list = []
) -> pd.DataFrame:
    """
    Function to read in the cross-wave data from the Understanding Society dataset

    Args:
        filename (str): The filename of the data to be read in
        fixed_cols (list): A list of the fixed columns to be kept
        start_cols (list): A list of the columns to be kept that start with a certain string
        end_cols (list): A list of the columns to be kept that end with a certain string

    Returns:
        pd.DataFrame: The data read in and processed
    """

    # Read in the data
    data = get_raw_slxwaveid_xwav(filename)

    # Filter the columns
    data = filter_columns(data, fixed_cols, start_cols, end_cols)

    # Make sure all  vars are in the correct format
    # Collect the id variables
    data = collect_columns(data)

    return data


def get_usoc_sldata_long(
    filename: str, fixed_cols: list = [], start_cols: list = [], end_cols: list = []
) -> pd.DataFrame:
    """
    Function to read in all wave specific data from a given file and return a merged longfile

    Args:
        filename (str): The filename of the data to be read in
        fixed_cols (list): A list of the fixed columns to be kept
        start_cols (list): A list of the columns to be kept that start with a certain string
        end_cols (list): A list of the columns to be kept that end with a certain string

    Returns:
        pd.DataFrame: The data read in and processed
    """
    # Get relevant file paths
    paths = get_raw_usoc_sldata_long_paths(filename)

    # Process and combine files
    file_list = []
    for file in paths:
        # Read data
        with fs.open(file, "rb") as f:
            data = pd.read_csv(f, sep="\t", low_memory=False)

        # Extract wave info
        wave_str = re.search(r"/[a-m]_" + re.escape(filename), file).group()[1:3]
        data["wave"] = wave_str[0]

        # Clean column names
        data.columns = [
            re.sub(f"^{wave_str}", "", col) if col.startswith(wave_str) else col
            for col in data.columns
        ]

        # Filter columns
        data = filter_columns(data, fixed_cols, start_cols, end_cols)

        # Convert idvars and "pno" columns to numeric
        data = collect_columns(data)

        file_list.append(data)

    return pd.concat(file_list, ignore_index=True)


def get_usoc_sllsoa_long() -> pd.DataFrame:
    """
    Function to read in the Understanding Society Special License LSOA data and return a longfile

    Returns:
        pd.DataFrame: The data read in and processed
    """

    # Get relevant file paths
    paths = get_raw_usoc_sllsoa_long_paths()

    file_list = []
    for file in paths:
        # Read the file
        with fs.open(file, "rb") as f:
            data = pd.read_csv(f, sep="\t", header=0, low_memory=False)

            # Extract wave and create wave variable
            wave_str = re.search(r"/[a-m]_", file).group()[1:3]
            data["wave"] = wave_str[0]

            # Rename columns by removing wave prefix
            data.columns = [
                col.replace(wave_str, "", 1) if col.startswith(wave_str) else col
                for col in data.columns
            ]

            # Ensure all vars are in the correct format
            data = collect_columns(data)

            file_list.append(data)

    # Combine all dataframes in the list
    longfile = pd.concat(file_list, ignore_index=True, sort=False)

    return longfile


def get_usoc_peach(
    wave_variable: str,
    fixed_cols: list = [],
    start_cols: list = [],
    end_cols: list = [],
) -> pd.DataFrame:
    """
    Function to read in the Peach data from the Understanding Society dataset

    Args:
        wave_variable (str): The name of the wave variable
        fixed_cols (list): The fixed columns to be kept
        start_cols (list): The columns to be kept that start with certain strings
        end_cols (list): The columns to be kept that end with certain strings

    Returns:
        pd.DataFrame: The data read in and processed
    """

    data = get_raw_usoc_peach()

    # Filter the columns
    data = filter_columns(data, fixed_cols, start_cols, end_cols)

    # Make sure all  vars are in the correct format
    # Collect the id variables
    data = collect_columns(data)

    # Create the correct wave joining variable and filter only relevant observations
    data = data[data[wave_variable] > 0]
    wvdf = pd.DataFrame(
        {
            wave_variable: range(1, 14),
            "wave": [chr(i) for i in range(ord("a"), ord("n"))],
        }
    )

    data = data.merge(wvdf, on=wave_variable, how="left")
    return data
