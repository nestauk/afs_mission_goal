from nesta_ds_utils.loading_saving.S3 import download_obj
from afs_mission_goal import DS_BUCKET, config


def get_frs_variables_dict() -> dict:
    """Function to load the Family Resources Survey dictionary that converts the column names to readable columns
    Returns:
        dict: Family resources survey column dictionary.
    """
    dictionary_of_datasets = {}
    for variables, name in zip(config["frs_original_names"], config["frs_datasets"]):
        path = f"data/aux/frs_variables/{variables}_variables.json"
        try:
            dictionary_of_datasets[name] = download_obj(
                DS_BUCKET, path_from=path, download_as="dict"
            )
        except:
            print(f"Dictionary for {variables} not found.")

    return dictionary_of_datasets
