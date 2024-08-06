from nesta_ds_utils.loading_saving.S3 import download_obj

BUCKET = "afs-uk-data-service"


def get_family_resources_survey_dict() -> dict:
    """Function to load the Family Resources Survey dictionary that converts the column names to readable columns
    Returns:
        dict: Family resources survey column dictionary.
    """
    path = "data/aux/frs.json"
    return download_obj(BUCKET, path_from=path, download_as="dict")
