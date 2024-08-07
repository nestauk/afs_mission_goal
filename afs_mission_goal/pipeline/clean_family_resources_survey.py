import pandas as pd
from afs_mission_goal.getters.uk_data_service.raw.family_resources_survey import *
from afs_mission_goal.getters.uk_data_service.misc.get_family_resources_survey_dict import (
    get_family_resources_survey_dict,
)
from afs_mission_goal import DS_BUCKET, config
from afs_mission_goal.utils.preprocessing import preprocess_strings
from nesta_ds_utils.loading_saving import S3

frs_datasets = config["frs_original_names"]
frs_dataset_new_names = config["frs_datasets"]


def clean_family_resources_survey(
    frs_data: pd.DataFrame, frs_columns: dict
) -> pd.DataFrame:
    """
    Clean the family resources survey by renaming the columns to a more readable format.
    Args:
        frs_data (pd.DataFrame): The dataframe to clean.
        frs_columns (dict): The dictionary of column names.

    Returns:
        pd.DataFrame: A dataframe with the columns renamed.
    """
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
    frs_columns = get_family_resources_survey_dict()
    old_and_new_names = dict(zip(frs_datasets, frs_dataset_new_names))
    for original_dataset, new_dataset in old_and_new_names.items():
        frs_data = get_raw_frs_data(original_dataset)
        frs_data = clean_family_resources_survey(frs_data, frs_columns)
        S3.upload_obj(
            obj=frs_data,
            bucket=DS_BUCKET,
            path_to=f"data/processed/family_resources_survey_{new_dataset}.csv",
            kwargs_writing={"index": False},
        )
