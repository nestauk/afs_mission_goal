"""
Using the Understanding Society dataset enriched spine, derive additional variables including 
income, benefits, and childcare related variables.

@author: jones, gillam
"""

import pandas as pd
import numpy as np
import re
from afs_mission_goal.getters.usoc.processed.usoc_master import get_usoc_processed
from afs_mission_goal.pipeline.usoc.processing.usoc_functions import (
    get_usoc_sldata_long,
)
from afs_mission_goal.utils.usoc_functions import (
    calculate_binary_childcare,
    create_date_columns,
)
from afs_mission_goal.pipeline.usoc.creating_datasets._02_variables.benefits import (
    calculate_benefits,
)
from afs_mission_goal.pipeline.usoc.creating_datasets._02_variables.childcare_hours import (
    process_ukhls_data,
    categorize_formal_hours,
)
from afs_mission_goal import PROJECT_DIR


def derive_additional_variables(
    ukhls_master: pd.DataFrame, master_vars: pd.DataFrame, income: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Derives and adds various new variables to the USoc dataset, including income, benefits, and childcare related variables.

    This includes:
    - Calculates real income variables adjusted for inflation.
    - Derives benefit related variables using both the income and indresp datasets.
    - Derives childcare related variables, including binary indicators for childcare use and continuous variables for hours spent.
    - Categorizes childcare use based on weekly hours.
    - Creates date related variables such as interview date and date of birth.
    - Updates the `master_vars` DataFrame with the origin of each newly derived variable.

    Args:
        ukhls_master (pd.DataFrame): The main dataset with initial variables.
        master_vars (pd.DataFrame): The DataFrame that logs the origin of variables.
        income (pd.DataFrame): The income data containing benefits and other financial information.

    Returns:
        tuple: A tuple containing:
            - ukhls_master (pd.DataFrame): The enriched dataset with derived variables.
            - master_vars (pd.DataFrame): The updated master variable log with origins for new variables.
    """

    # Save original variable names
    oldvars = list(ukhls_master.columns)

    # Derive income variables
    print("Deriving income variables")
    index_2023 = (
        ukhls_master[ukhls_master["intdaty_dv"] == 2023]["inflation_index"]
        .drop_duplicates()
        .tolist()[0]
    )

    ukhls_master = ukhls_master.assign(
        fihhmnnet1_real=lambda df: df["fihhmnnet1_dv"]
        * index_2023
        / df["inflation_index"],
        fihhmnlabnet_real=lambda df: df["fihhmnlabnet_dv"]
        * index_2023
        / df["inflation_index"],
        fihhmnsben_real=lambda df: df["fihhmnsben_dv"]
        * index_2023
        / df["inflation_index"],
        fihhmngrs_real=lambda df: df["fihhmngrs_dv"]
        * index_2023
        / df["inflation_index"],
        fihhmnlabgrs_real=lambda df: df["fihhmnlabgrs_dv"]
        * index_2023
        / df["inflation_index"],
        equivinc=lambda df: df["fihhmnnet1_dv"] / df["ieqmoecd_dv"],
        equivinc_real=lambda df: df["fihhmnnet1_real"] / df["ieqmoecd_dv"],
    )

    #### DERIVE THE BENEFITS VARIABLES #####
    ## Note from Laura: I DO THIS TWO WAYS (ONCE USING THE VARIABLES FROM THE INDRESP FILE AND ONCE USING THE INCOME FILE)AND THEN CHECK THE ANSWERS AGAINST EACH OTHER
    ## THIS IS BECAUSE THERE ARE ISSUES WITH USING EITHER METHOD
    ## THE WAY THAT BENEFITS HAVE BEEN CAPTURED IN THE INDRESP FILE HAS CHANGED CONSIDERABLY ACROSS WAVES MAKING IT HARD TO BE CONFIDENT THAT EVERYTHING RELEVANT IS CAPTURED
    ## THE WAY THAT BENEFTS ARE CAPTURED IN THE INCOME FILE IS CONSISTENT ACROSS WAVES BUT IT IS NOT POSSIBLE TO DISTINGUISH BETWEEN THOSE WHO DON'T RECEIVE A GIVEN BENEFIT AND THOSE WITH MISSING INFORMATION
    ## ULTIMATELY, I USE THE VARIABLE CAPTURED USING THE INDRESP FILE AS THE MASTER (AS WE CAN DISTINGUISH NOS FROM MISSINGS) BUT SUB IN SOME INFORMATION FROM THE VARIABLE CAPTURED USING THE INCOME FILE WHERE RELEVANT

    ###### DERIVE THE BENEFITS VARIABLES USING INCOME FILE#######
    print("Deriving benefits variables using income file")
    ukhls_master = ukhls_master.rename(columns={"benctc": "benctcorig"})

    income = (
        income.query("frmnth_dv > 0")
        .groupby(["pidp", "wave"], group_keys=True)
        .apply(
            lambda g: pd.Series(
                {
                    "benuc": (g["ficode"] == 40).sum(),
                    "benhouse": (g["ficode"] == 22).sum(),
                    "benincsup": (g["ficode"] == 15).sum(),
                    "benincjsa": (g["ficode"] == 16).sum(),
                    "benincesa": (g["ficode"] == 33).sum(),
                    "benwtc": (g["ficode"] == 20).sum(),
                    "bencb": (g["ficode"] == 18).sum(),
                    "benctc": (g["ficode"] == 19).sum(),
                }
            ),
            include_groups=False,
        )
        .reset_index()
    )

    income = (
        income.filter(
            items=["wave", "pidp"]
            + [col for col in income.columns if col.startswith("ben")]
        )
        .drop_duplicates()
        .reset_index(drop=True)
    )

    ukhls_master = ukhls_master.merge(income, on=["pidp", "wave"], how="left")

    # ##### DERIVE THE BENEFITS VARIABLES USING INDRESP VARIABLES #####
    print("Deriving benefits variables using indresp file")
    ukhls_master = calculate_benefits(ukhls_master)

    #### DERIVE THE CHILDCARE VARIABLES  #####
    print("Deriving childcare variables")
    #### DERIVE BINARY VARIABLE WHETHER OR NOT USES DIFFERENT TYPES OF CHILDCARE #####
    ##### NB HOW CHILDCARE IS RECORDED VARIES CONSIDERABLY ACROSS WAVES CROSS WAVE COMPARISON SHOULD BE APPROACHED WITH CAUTION AFTER CONSIDERING THE DIFFERENCES
    #### IN WAVES 1-12 THE VARIABLES WRKCH2A* RECORD USE OF USUAL TYPE OF CHILDCARE DURING TERM TIME
    #### IN WAVE 13 THE VARIABLES CCPROVIDER* RECORDS USE OF DIFFERENT TYPES OF CHILDCARE IN THE LAST WEEK

    # Formal childcare use waves 1-12
    formal_pattern = r"wrkch2a([1-9]|10|16|17)$"
    formalusevars = [
        col for col in ukhls_master.columns if re.match(formal_pattern, col)
    ]

    ukhls_master = calculate_binary_childcare(ukhls_master, "use_formal", formalusevars)

    # Formal childcare use wave 13
    formal_pattern_13 = r"ccprovider([1-9]|14|15|17)$"
    formalusevars_13 = [
        col for col in ukhls_master.columns if re.match(formal_pattern_13, col)
    ]

    ukhls_master = calculate_binary_childcare(
        ukhls_master, "use_formal_13", formalusevars_13
    )

    # Informal childcare use waves 1-12
    informalusevars = ["wrkch2a11", "wrkch2a12", "wrkch2a13", "wrkch2a14", "wrkch2a15"]
    ukhls_master = calculate_binary_childcare(
        ukhls_master, "use_informal", informalusevars
    )

    # Informal childcare use wave 13
    informalusevars_13 = [
        "ccprovider10",
        "ccprovider11",
        "ccprovider12",
        "ccprovider13",
        "ccprovider16",
        "ccprovider17",
    ]
    ukhls_master = calculate_binary_childcare(
        ukhls_master, "use_informal_13", informalusevars_13
    )

    # MERGE THE VARIABLES
    ukhls_master = ukhls_master.assign(
        use_formal=ukhls_master.apply(
            lambda row: (
                row["use_formal_13"] if row["wave"] == "m" else row["use_formal"]
            ),
            axis=1,
        ),
        use_informal=ukhls_master.apply(
            lambda row: (
                row["use_informal_13"] if row["wave"] == "m" else row["use_informal"]
            ),
            axis=1,
        ),
    ).drop(columns=["use_formal_13", "use_informal_13"])

    # BE WARY OF THE CHILDCARE VARIABLES, low percentages of 3yos, when should be ~90%

    #### DERIVE CONTINUOUS VARIABLE MEASURING HOURS SPENT IN DIFFERENT TYPES OF CHILDCARE #####
    ##### NB HOW CHILDCARE IS RECORDED VARIES CONSIDERABLY ACROSS WAVES SO THE ORIGINAL VARIABLES SHOULD BE APPROACHED WITH CAUTION
    #### IN WAVES 1-5 there are three hours variables - wrkch31, wrkch32 and wrkch32 which reflect the time spent in the three most used types of childcare of whatever type
    #### IN WAVES 6-12 there are now 17 variables which each reflect time spent in a particular type of provider. wrkch31, 32 and 33 remain BUT CHANGE MEANING(!) to mean time spent in
    #### NURSERY SCHOOL/CLASS, SE NURSERY AND DAY NURSERY/CRECHE
    #### IN WAVE 13 AN ENTIRELY NEW SYSTEM IS INTRODUCED

    # Initialize formal and informal variables for each wave
    # Waves 1-5
    formal_vars_15 = formalusevars
    informal_vars_15 = informalusevars

    # Waves 6-12

    formal_vars_612 = [
        col for col in ukhls_master.columns if re.match("wrkch3([1-9]|10|16|17)$", col)
    ]
    informal_vars_612 = ["wrkch311", "wrkch312", "wrkch313", "wrkch314", "wrkch315"]

    # Wave 13
    formal_vars_13 = [
        col for col in ukhls_master.columns if re.match("cchours([1-9]|14|15|17)$", col)
    ]
    informal_vars_13 = [
        "cchours10",
        "cchours11",
        "cchours12",
        "cchours13",
        "cchours16",
        "cchours17",
    ]

    ukhls_master = process_ukhls_data(
        ukhls_master=ukhls_master,
        formal_vars_15=formal_vars_15,
        informal_vars_15=informal_vars_15,
        formal_vars_612=formal_vars_612,
        informal_vars_612=informal_vars_612,
        formal_vars_13=formal_vars_13,
        informal_vars_13=informal_vars_13,
    )

    # CREATE CATEGORICAL VARIABLE ##
    # follow SEED definitions FORMAL CHILDCARE https://assets.publishing.service.gov.uk/media/5e4e5c10e90e074dcd5bd213/SEED_AGE_5_REPORT_FEB.pdf
    # high use - using over 20 hours per week
    #  medium use - using 10-20 hours per week
    # low use - using 0-10 hours per week
    # not using - not using childcare

    # Apply the categorization function
    ukhls_master["formal_cchrs_cat"] = ukhls_master["formal_hours"].apply(
        categorize_formal_hours
    )

    # Map the numerical categories to descriptive labels
    formal_cchrs_labels = {
        0: "no formal childcare use",
        1: "1-10 hrs formal childcare use",
        2: "11-20 hrs formal childcare use",
        3: "20+ hours formal childcare use",
    }

    ukhls_master["formal_cchrs_cat"] = ukhls_master["formal_cchrs_cat"].map(
        formal_cchrs_labels
    )

    # Convert 'wave' variable from letter to number (assuming 'wave' is a letter)
    ukhls_master["wave"] = ukhls_master["wave"].apply(lambda x: ord(x) - ord("a") + 1)

    # Create 'interview date' and 'dob date' variables
    ukhls_master["int_date"] = create_date_columns(
        ukhls_master, ["intdaty_dv", "intdatm_dv"]
    )
    ukhls_master["dob_date"] = create_date_columns(ukhls_master, ["doby_dv", "dobm_dv"])

    # Add the new variables to the master_vars DataFrame
    newvars = pd.DataFrame(
        {
            "origin": [np.nan] * len(ukhls_master.columns),
            "variable": ukhls_master.columns.astype(str),
        }
    )

    # Filter newvars to exclude variables in oldvars
    newvars = newvars[~newvars["variable"].isin(oldvars)]

    # Define the variable groups
    incomevars = ["equivinc"] + [var for var in newvars["variable"] if "real" in var]
    benefitvars = [var for var in newvars["variable"] if "ben" in var]
    childcarevars = [var for var in newvars["variable"] if "formal" in var]

    # Update the origin column based on conditions
    newvars["origin"] = np.select(
        [
            newvars["variable"].isin(incomevars),
            newvars["variable"].isin(benefitvars),
            newvars["variable"].isin(childcarevars),
            newvars["variable"] == "int_date",
            newvars["variable"] == "dob_date",
        ],
        ["hhresp", "indresp", "child", "hhresp", "xwave"],
        default=np.nan,
    )

    # Combine newvars with master_vars
    master_vars = pd.concat([master_vars, newvars], ignore_index=True)

    return ukhls_master, master_vars


if __name__ == "__main__":

    # Load the data
    ukhls_master = get_usoc_processed("ukhls_master_enriched_spine")
    master_vars = get_usoc_processed("ukhls_master_vars")
    income = get_usoc_sldata_long("income")

    ukhls_master, master_vars = derive_additional_variables(
        ukhls_master, master_vars, income
    )

    # Save the data locally for use in the SecureLab
    master_vars.to_parquet(
        f"{PROJECT_DIR}/outputs/data/ukhls_master_vars_dv.parquet", engine="pyarrow"
    )
    ukhls_master.to_parquet(
        f"{PROJECT_DIR}/outputs/data/ukhls_master_enriched_spine_dv.parquet",
        engine="pyarrow",
    )
