import pandas as pd
from afs_mission_goal.getters.uk_data_service.raw.family_resources_survey import *
from afs_mission_goal.getters.uk_data_service.misc.get_family_resources_survey_dict import (
    get_family_resources_survey_dict,
)
from afs_mission_goal.utils.preprocessing import preprocess_strings

from nesta_ds_utils.loading_saving import S3

frs_columns = get_family_resources_survey_dict()


def clean_family_resources_survey(frs_data: pd.DataFrame) -> pd.DataFrame:
    new_columns = []
    count = 0

    for col in frs_data.columns:
        try:
            new_columns.append(frs_columns[col])
        except:
            new_columns.append(col)
    frs_data.columns = new_columns
    frs_data.columns = list(preprocess_strings(pd.Series(frs_data.columns)))

    return frs_data


if __name__ == "__main__":
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

    for dataset in frs_datasets:
        frs_data = globals()[f"get_{dataset}_data"]()
        frs_data = clean_family_resources_survey(frs_data)
        S3.upload_obj(
            obj=frs_data,
            bucket="afs-uk-data-service",
            path_to=f"data/processed/family_resources_survey_{dataset}.csv",
            kwargs_writing={"index": False},
        )
