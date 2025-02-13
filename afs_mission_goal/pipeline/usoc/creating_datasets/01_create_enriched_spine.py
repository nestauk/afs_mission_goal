"""
Create enriched spine dataset for the Understanding Society dataset by merging multiple data sources 
from the Understanding Society dataset.

@author: jones, gillam
"""

import pandas as pd
from afs_mission_goal.pipeline.usoc.processing.usoc_functions import (
    get_usoc_slxwaveid_xwav,
    get_usoc_sldata_long,
    get_usoc_sllsoa_long,
    get_usoc_peach,
)
from afs_mission_goal.pipeline.usoc.processing.external_functions import get_inflation
from afs_mission_goal import PROJECT_DIR, usoc_config_01


def create_enriched_spine(
    ukhls_file: pd.DataFrame, dict_df_vars: dict[str, list[pd.DataFrame]]
) -> list[pd.DataFrame]:
    """
    Creates an enriched long-format dataset by merging multiple data sources.

    This includes:
    - Renaming columns so that wave indicators appear as suffixes.
    - Converting the dataset from wide to long format.
    - Logging the origin of each variable.
    - Merging additional datasets into the main dataset based on specified key columns.
    - Ensuring no duplicate variables are included from the child dataset.

    Args:
        ukhls_file (pd.DataFrame): The main UKHLS dataset in wide format.
        list_other_dfs (list[pd.DataFrame]): A list of other datasets to merge with the main dataset.
        list_vars_merge (list[list[str]]): A list of lists specifying the columns used for merging each dataset.

    Returns:
        list[pd.DataFrame]: A list containing:
            - ukhls_master (pd.DataFrame): The enriched dataset in long format.
            - master_vars (pd.DataFrame): A dataframe logging the origin of each variable.
    """

    # First reorder the column names so that wave indicator are suffixes rather than prefixes #
    temp = ukhls_file.rename(
        columns=lambda col: (
            "_".join(col.split("_")[::-1])
            if col.split("_")[-1] in ["hidp", "pno", "ivfio", "ivfho"]
            else col
        )
    )

    # Convert from wide to long
    ukhls_master = pd.wide_to_long(
        temp.reset_index(drop=True),
        stubnames=["hidp", "pno", "ivfio", "ivfho"],
        sep="_",
        i="pidp",
        j="wave",
        suffix=".+",
    ).reset_index()

    # Log variable origin
    master_vars = pd.DataFrame({"origin": "xwave", "variable": ukhls_master.columns})

    #### Select all the variables we need from the other dataframes ####
    for key, value in dict_df_vars.items():

        df, merge_cols = value

        if key == "child":
            common_vars = ukhls_master.columns.intersection(df.columns)
            common_vars = common_vars.drop(["pidp", "wave"])
            df = df[[col for col in df.columns if col not in common_vars]]

        vars_temp = pd.DataFrame(
            {
                "origin": key,
                "variable": [col for col in df.columns if col not in merge_cols],
            }
        )

        master_vars = pd.concat([master_vars, vars_temp], ignore_index=True)
        ukhls_master = pd.merge(ukhls_master, df, on=merge_cols, how="left")

    return ukhls_master, master_vars


if __name__ == "__main__":
    # Note: To add more data sources, you will need to add them to the dict_df_vars dictionary
    # See the config file for the specific variables to be included from each data source

    # Get xwave data
    print("Getting xwave data")
    # XWAVEID
    xwaveid = get_usoc_slxwaveid_xwav(
        "xwaveid",
        fixed_cols=usoc_config_01["xwaveid_fixed_cols"],
        end_cols=usoc_config_01["xwaveid_end_cols"],
    )

    # XWAVEDAT
    xwavedat = get_usoc_slxwaveid_xwav(
        "xwavedat",
        fixed_cols=usoc_config_01["xwavedat_fixed_cols"],
    )

    # Merge xwave files
    ukhls_file = pd.merge(xwaveid, xwavedat, on="pidp", how="left")

    # Get indall data
    print("Getting indall data")
    indall = get_usoc_sldata_long(
        "indall",
        fixed_cols=usoc_config_01["indall_fixed_cols"],
        start_cols=usoc_config_01["indall_start_cols"],
        end_cols=usoc_config_01["indall_end_cols"],
    )

    # Get indresp data
    print("Getting indresp data")
    indresp = get_usoc_sldata_long(
        "indresp",
        fixed_cols=usoc_config_01["indresp_fixed_cols"],
        start_cols=usoc_config_01["indresp_start_cols"],
    )

    # Get hhresp data
    print("Getting hhresp data")
    hhresp = get_usoc_sldata_long(
        "hhresp",
        fixed_cols=usoc_config_01["hhresp_fixed_cols"],
        start_cols=usoc_config_01["hhresp_start_cols"],
    )

    # Get lsoa data
    print("Getting lsoa data")
    lsoa = get_usoc_sllsoa_long()

    print("Getting peach data")
    # Get peach pregnancy data
    peachpreg = get_usoc_peach(
        "wave_pregnancy",
        fixed_cols=usoc_config_01["peachpreg_fixed_cols"],
        start_cols=usoc_config_01["peachpreg_start_cols"],
    )
    # Get peach newborn data
    peachnb = get_usoc_peach(
        "wave_newborn",
        fixed_cols=usoc_config_01["peachnb_fixed_cols"],
        start_cols=usoc_config_01["peachnb_start_cols"],
    )
    # Get peach parenting style data
    peachparstyle = get_usoc_peach(
        "wave_parstyle",
        fixed_cols=usoc_config_01["peachparstyle_fixed_cols"],
        start_cols=usoc_config_01["peachparstyle_start_cols"],
    )

    # Get child data
    print("Getting child data")
    child = get_usoc_sldata_long(
        "child",
        fixed_cols=usoc_config_01["child_fixed_cols"],
        start_cols=usoc_config_01["child_start_cols"],
        end_cols=usoc_config_01["child_end_cols"],
    )

    print("Getting inflation data")
    inflation = get_inflation()

    # For creating the enriched spine,
    # we need to know what variables we wish to merge on
    pidp_wave = [
        "pidp",
        "wave",
    ]  # for indall, indresp, hhresp, lsoa, peachpreg, peachnb, peachparstyle, child
    hidp_wave = ["hidp", "wave"]  # for hhresp
    inflat = ["intdaty_dv"]  # for inflation data

    dict_df_vars = {
        "indall": [indall, pidp_wave],
        "indresp": [indresp, pidp_wave],
        "hhresp": [hhresp, hidp_wave],
        "lsoa": [lsoa, hidp_wave],
        "peachpreg": [peachpreg, pidp_wave],
        "peachnb": [peachnb, pidp_wave],
        "peachparstyle": [peachparstyle, pidp_wave],
        "child": [child, pidp_wave],
        "inflation": [inflation, inflat],
    }

    # Create enriched spine
    ukhls_master, master_vars = create_enriched_spine(ukhls_file, dict_df_vars)

    # Save the enriched spine locally for use on the SecureLab
    ukhls_master.to_parquet(
        f"{PROJECT_DIR}/outputs/data/ukhls_master_enriched_spine.parquet",
        engine="pyarrow",
        index=False,
    )

    master_vars.to_parquet(
        f"{PROJECT_DIR}/outputs/data/ukhls_master_vars.parquet",
        engine="pyarrow",
        index=False,
    )
