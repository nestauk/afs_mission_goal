import pandas as pd
from nesta_ds_utils.loading_saving.S3 import download_obj

BUCKET = "afs-uk-data-service"


def get_wealth_and_assets_survey(wave: int, granularity: str, **kwargs) -> pd.DataFrame:
    """Function to load the Wealth and Assets Survey data from the UK Data Service.
    Args:
        wave (int): The wave of the Wealth and Assets Survey. Must be between 1 and 7.
        granularity (str): The granularity of the data, must be either "person" or "household".
        kwargs:
            wave_5_household_month (str): The month of the wave 5 household data to load. Must be either "feb" or "sept".
    Returns:
        pd.DataFrame: Wealth and Assets Survey data.
    """
    kwargs.get("wave_5_household_month", None)

    if wave == 5 and granularity == "household" and wave_5_household_month is not None:
        path = f"data/processed/wealth_and_assets_survey_{granularity}_wave_{wave}_{wave_5_household_month}.csv"
    else:
        path = f"data/processed/wealth_and_assets_survey_{granularity}_wave_{wave}.csv"
    return download_obj(BUCKET, path_from=path, download_as="dataframe")
