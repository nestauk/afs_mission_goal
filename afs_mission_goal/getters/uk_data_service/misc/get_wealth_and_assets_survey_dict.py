from nesta_ds_utils.loading_saving.S3 import download_obj
from afs_mission_goal import DS_BUCKET


def get_wealth_and_assets_survey_dict() -> dict:
    """Function to load the Wealth and Assets Survey data from the UK Data Service.
    Returns:
        dict: Wealth and Assets Survey data.
    """
    path = "data/aux/wealth_and_assets_survey_dict.json"
    return download_obj(DS_BUCKET, path_from=path, download_as="dict")
