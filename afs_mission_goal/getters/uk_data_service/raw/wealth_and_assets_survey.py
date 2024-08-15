import pandas as pd
from afs_mission_goal.utils.load_s3 import load_from_s3
from afs_mission_goal.getters.uk_data_service.misc.get_wealth_and_assets_survey_dict import (
    get_wealth_and_assets_survey_dict,
)
from afs_mission_goal import DS_BUCKET

dictionary = get_wealth_and_assets_survey_dict()


def get_wealth_and_assets_survey(
    wave=1, granularity="person", **kwargs
) -> pd.DataFrame:
    """Function to load the Wealth and Assets Survey data from the UK Data Service.
    Args:
        wave (int): Wave of the survey. Default is 1. Can be any number from 1 to 7.
        granularity (str): Granularity of the data. Default is "person". Can be "person" or "household".
        wave_5_household_month (str): Month of the wave 5 household data. Default is None. Can be "feb" or "sept".
    Returns:
        pd.DataFrame: Wealth and Assets Survey data.
    """
    wave_5_household_month = kwargs.get("wave_5_household_month", None)
    if wave == 5 and granularity == "household" and wave_5_household_month is not None:
        fname_from_dictionary = dictionary[f"wave_5_household_{wave_5_household_month}"]
        filename = f"{fname_from_dictionary}.dta"
    else:
        fname = dictionary[f"wave_{wave}_{granularity}"]
        filename = f"{fname}.dta"
    path = "raw/wealth_and_assets_survey/" + filename
    return load_from_s3(path, bucket=DS_BUCKET)
