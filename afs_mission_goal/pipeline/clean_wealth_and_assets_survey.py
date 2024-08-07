from afs_mission_goal.getters.uk_data_service.raw.wealth_and_assets_survey import (
    get_wealth_and_assets_survey,
)
from afs_mission_goal.utils.preprocessing import preprocess_strings
import pandas as pd
import numpy as np
from nesta_ds_utils.loading_saving import S3
from afs_mission_goal import DS_BUCKET


def clean_wealth_and_assets_survey(wealth_and_assets_survey_data: pd.DataFrame):
    """
    Cleans the Wealth and Assets Survey data by processing the column names.

    Args:
        wealth_and_assets_survey_data (pd.DataFrame): The raw dataframe from the Wealth and Assets Survey.
    """

    wealth_and_assets_survey_data.columns = preprocess_strings(
        pd.Series(wealth_and_assets_survey_data.columns)
    )

    return wealth_and_assets_survey_data


if __name__ == "__main__":
    # Loading raw data at person level
    for i in range(1, 8):
        wealth_and_assets_person = get_wealth_and_assets_survey(
            wave=i, granularity="person"
        )
        wealth_and_assets_person = clean_wealth_and_assets_survey(
            wealth_and_assets_person
        )

        # Saving cleaned data
        S3.upload_obj(
            obj=wealth_and_assets_person,
            bucket=DS_BUCKET,
            path_to=f"data/processed/wealth_and_assets_survey_person_wave_{i}.csv",
            kwargs_writing={"index": False},
        )

    for i in range(1, 8):
        if i != 5:
            wealth_and_assets_household = get_wealth_and_assets_survey(
                wave=i, granularity="household"
            )
            wealth_and_assets_household = clean_wealth_and_assets_survey(
                wealth_and_assets_household
            )
            # Saving cleaned data
            S3.upload_obj(
                obj=wealth_and_assets_household,
                bucket=DS_BUCKET,
                path_to=f"data/processed/wealth_and_assets_survey_household_wave_{i}.csv",
                kwargs_writing={"index": False},
            )
        else:
            for month in ["feb", "sept"]:
                wealth_and_assets_household = get_wealth_and_assets_survey(
                    wave=i, granularity="household", wave_5_household_month=month
                )
                wealth_and_assets_household = clean_wealth_and_assets_survey(
                    wealth_and_assets_household
                )
                # Saving cleaned data
                S3.upload_obj(
                    obj=wealth_and_assets_household,
                    bucket=DS_BUCKET,
                    path_to=f"data/processed/wealth_and_assets_survey_household_wave_{i}_{month}.csv",
                    kwargs_writing={"index": False},
                )
