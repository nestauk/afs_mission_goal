"""
To read in the Family Resources Survey datasets from the UK Data Service, you have the option of two functions. One reads in every dataset into a dictionary where the key is the dataset name and the value is the pd.DataFrame. The second function allows you to read in individual datasets, with an argument to say which dataset you want to read in.
"""

import pandas as pd
from nesta_ds_utils.loading_saving.S3 import download_obj


BUCKET = "afs-uk-data-service"
frs_datasets = [
    "accounts",
    "adult",
    "assets",
    "benefits",
    "benefit_unit",
    "care",
    "child",
    "childcare",
    "dictionary",
    "endowment",
    "ext_child",
    "frs2223",
    "gov_pay",
    "household",
    "job",
    "maint",
    "mort_cont",
    "mortgage",
    "odd_job",
    "owner",
    "pension_provider",
    "pension",
    "rent_cont",
    "renter",
    "tables",
]


def get_all_datasets() -> dict:
    """
    Function to load all datasets from the Family Resources Survey from the UK Data Service.
    Returns:
        dict: Dictionary of all datasets (in the format of dataframes) from the Family Resources Survey.
    """
    dictionary_of_datasets = {}
    for dataset in frs_datasets:
        path = f"data/processed/family_resources_survey_{dataset}.csv"
        dictionary_of_datasets[dataset] = download_obj(
            BUCKET, path_from=path, download_as="dataframe"
        )
    return dictionary_of_datasets


def get_individual_dataset(dataset: str) -> pd.DataFrame:
    """
    Function to load a specific Family Resources Survey dataset from the UK Data Service.

    Args:
        dataset (str): Any of the following strings - "accounts", "adult", "assets", "benefits", "benefit_unit", "care", "child", "childcare", "dictionary", "endowment", "ext_child", "frs2223", "gov_pay", "household", "job", "maint", "mort_cont", "mortgage", "odd_job", "owner", "pension_provider","pension","rent_cont", "renter","tables"

    Returns:
        pd.DataFrame: A dataframe of the specified dataset.
    """
    path = f"data/processed/family_resources_survey_{dataset}.csv"
    return download_obj(BUCKET, path_from=path, download_as="dataframe")
