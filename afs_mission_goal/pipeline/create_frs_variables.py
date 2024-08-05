from afs_mission_goal.getters.uk_data_service.raw.family_resources_survey import (
    get_raw_frs_data,
)
from afs_mission_goal.getters.uk_data_service.misc.get_frs_variables import (
    get_frs_variables_dict,
)
from afs_mission_goal.utils.google_utils import access_google_sheet
import numpy as np
import pandas as pd
from nesta_ds_utils.loading_saving import S3
from afs_mission_goal import config
from typing import Dict
from afs_mission_goal import DS_BUCKET


def create_frs_dataframes(
    frs_datasets: list,
    frs_original_names: list,
    all_vars: pd.DataFrame,
    raw_frs_dict: Dict[str, pd.DataFrame],
    frs_variables: Dict[str, Dict[str, str]],
) -> Dict[str, pd.DataFrame]:
    """
    Function to create the FRS dataframes with the variables of interest.
    Args:
        frs_datasets (list): List of the wanted FRS dataset names.
        frs_original_names (list): List of the original FRS dataset names.
        all_vars (pd.DataFrame): DataFrame with the variables of interest taken from the google sheets.
        raw_frs_dict (Dict[pd.DataFrame]): Dictionary with the raw FRS dataframes.
        frs_variables (Dict[Dict[str,str]]): Dictionary with the values to replace in the FRS dataframes.
    Returns:
        Dict[pd.DataFrame]: Dictionary with the FRS dataframes.
    """

    dict_keys = dict(zip(frs_datasets, frs_original_names))

    frs_vars = {}
    for key in all_vars.Dataset.unique().tolist():
        if dict_keys[key] in raw_frs_dict.keys():
            raw_data = raw_frs_dict[dict_keys[key]]
            raw_data.columns = raw_data.columns.str.upper()
            cols_of_interest = ["SERNUM"] + all_vars[
                all_vars.Dataset == key
            ].Original.tolist()
            raw_data = raw_data[cols_of_interest]
            frs_vars[key] = raw_data.replace(np.nan, "").replace("nan", "")

    # Convert all numeric columns to floats
    for key in frs_vars.keys():
        for column in frs_vars[key].columns:
            try:
                frs_vars[key][column] = pd.Series(frs_vars[key][column], dtype="float")
            except:
                continue

    dictionary = raw_frs_dict["dictnary"][["VARIABLE", "LABEL"]].copy()
    dictionary["VARIABLE"] = dictionary["VARIABLE"].str.upper()
    dictionary["LABEL"] = (
        dictionary["LABEL"]
        .str.replace(r"[^\w\s]", "", regex=True)
        .str.replace(r"\s+", "_", regex=True)
        .str.lower()
    )
    dictionary_dict = dictionary.set_index("VARIABLE")["LABEL"].to_dict()

    frs_vars_final = frs_vars.copy()

    for key in frs_vars_final.keys():
        if key in frs_variables.keys():
            frs_variables[key] = {
                var_key.upper(): value for var_key, value in frs_variables[key].items()
            }

            for vars in frs_variables[key].keys():
                if vars in frs_vars_final[key].columns:
                    frs_vars_final[key][vars] = (
                        pd.Series(frs_vars_final[key][vars], dtype="string")
                        .replace(frs_variables[key][vars])
                        .str.capitalize()
                        .str.strip()
                    )
            frs_vars_final[key] = frs_vars_final[key].rename(columns=dictionary_dict)

    return frs_vars_final


if __name__ == "__main__":
    # Get the dataset names and the original names
    frs_datasets = config["frs_datasets"]
    frs_original_names = config["frs_original_names"]
    dict_keys = dict(zip(frs_datasets, frs_original_names))

    # Get the raw data with the original names
    print("Getting the raw data")
    raw_frs_dict = {}
    for names, dataset in zip(frs_datasets, frs_original_names):
        raw_frs_dict[dataset] = get_raw_frs_data(dataset)

    # Get the longer form variables
    print("Getting the variables")
    frs_variables = get_frs_variables_dict()

    # Load the google sheets with the variables of interest
    print("Getting the google sheets")
    incomings = access_google_sheet(
        "1Ld3TYH-8YOSBL9K-BlOnd7JELDZGtdkk77F-l75Tlqc", "Incomings", row_names=False
    )
    outgoings = access_google_sheet(
        "1Ld3TYH-8YOSBL9K-BlOnd7JELDZGtdkk77F-l75Tlqc", "Outgoings", row_names=False
    )
    demographics = access_google_sheet(
        "1Ld3TYH-8YOSBL9K-BlOnd7JELDZGtdkk77F-l75Tlqc", "Demographics", row_names=False
    )
    average_stats = access_google_sheet(
        "1Ld3TYH-8YOSBL9K-BlOnd7JELDZGtdkk77F-l75Tlqc", "Average_Stats", row_names=False
    )

    # Combine the google sheets into one
    all_vars = (
        pd.concat([incomings, outgoings, demographics], axis=0, ignore_index=True)[
            ["Variable", "Dataset", "Original"]
        ]
        .replace("", np.nan)
        .dropna(subset=["Original", "Dataset"])
    )
    all_vars = all_vars[all_vars.Original != "SERNUM"]

    # Create the FRS dataframes
    print("Creating the FRS dataframes")
    frs_vars_final = create_frs_dataframes(
        frs_datasets, frs_original_names, all_vars, raw_frs_dict, frs_variables
    )

    # Save the dataframes
    print("Saving the dataframes")
    for key in frs_vars_final.keys():
        S3.upload_obj(
            frs_vars_final[key],
            bucket=DS_BUCKET,
            path_to=f"data/processed/filtered_dataframes/{key}_df.csv",
            kwargs_writing={"index": False},
        )
