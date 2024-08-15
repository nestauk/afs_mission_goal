from nesta_ds_utils.loading_saving.S3 import download_obj
from afs_mission_goal import DS_BUCKET


def get_family_resources_survey_dict() -> dict:
    """Function to load the Family Resources Survey dictionary that converts the column names to readable columns
    Returns:
        dict: Family resources survey column dictionary.
    """
    path = "data/aux/frs.json"
    return download_obj(DS_BUCKET, path_from=path, download_as="dict")
