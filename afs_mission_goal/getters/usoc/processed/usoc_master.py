import pandas as pd
from afs_mission_goal import PROJECT_DIR


def get_usoc_processed(filename: str, extension: str = "parquet") -> pd.DataFrame:
    """
    Function to read in the data processed in 01_create_enriched_spine.py

    Args:
        filename (str): The filename of the data to be read in
        extension (str): The extension of the data to be read in, options are 'parquet' or 'csv'

    Returns:
        pd.DataFrame: The data read in
    """

    file_path = f"{PROJECT_DIR}/outputs/data/{filename}.{extension}"

    if extension == "csv":
        data = pd.read_csv(file_path)
    elif extension == "parquet":
        data = pd.read_parquet(file_path, engine="pyarrow")
    else:
        raise ValueError("Extension must be 'csv' or 'parquet'")

    return data
