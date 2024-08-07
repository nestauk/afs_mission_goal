from nesta_ds_utils.loading_saving.S3 import download_obj
import pandas as pd
from afs_mission_goal import config, DS_BUCKET


def get_filtered_datasets() -> dict:
    """
    Function to load all datasets from the Family Resources Survey from the UK Data Service.
    Returns:
        dict: Dictionary of all datasets (in the format of dataframes) from the Family Resources Survey.
    """
    frs_datasets = config["frs_datasets"]
    dictionary_of_datasets = {}
    for dataset in frs_datasets:
        try:
            path = f"data/processed/filtered_dataframes/{dataset}_df.csv"
            dictionary_of_datasets[dataset] = download_obj(
                DS_BUCKET,
                path_from=path,
                download_as="dataframe",
            )
        except:
            print(f"Dataset {dataset} not found in variables of interest.")
            continue
    return dictionary_of_datasets


def get_base_df() -> pd.DataFrame:
    """
    Function to load the base dataframe with the child and adult data.
    Returns:
        pd.DataFrame: Base dataframe with the child and adult data.
    """
    path = "data/processed/filtered_dataframes/base_df.csv"
    return download_obj(DS_BUCKET, path_from=path, download_as="dataframe")


def get_lowincome_0_5() -> pd.DataFrame:
    """
    Function to load the low income households with children under 5 dataframe.
    Returns:
        pd.DataFrame: Low income households with children under 5 dataframe.
    """
    path = "data/processed/filtered_dataframes/lowincome_0_5.csv"
    return download_obj(DS_BUCKET, path_from=path, download_as="dataframe")
