"""
Creates a family dataset that combines child, parent, and household information for the USoc cohort we are interested in.

@author: jones, gillam
"""

import pandas as pd
import numpy as np
from afs_mission_goal.getters.usoc.processed.usoc_master import get_usoc_processed
from afs_mission_goal.utils.usoc_functions import negate_in, process_parents
from afs_mission_goal import PROJECT_DIR, usoc_config_03


def create_family_dataset(
    ukhls_master: pd.DataFrame, master_vars: pd.DataFrame
) -> pd.DataFrame:
    """
    Creates a family dataset that combines child, parent, and household information for the USoc cohort we are interested in.

    This includes the following steps:
    - Extracts relevant variables for parents, children, and households from different sources (xwave, indall, indresp).
    - Constructs datasets for children and parents (mother and father).
    - Merges child and parent datasets, ensuring that parent information is available in waves prior to the childs birth.
    - Calculates additional variables such as 'months_since_birth' and identifies whether the mother was pregnant.
    - Creates a complete family dataset by combining information from children, mothers, and fathers.

    Args:
        ukhls_master (pd.DataFrame): The main USoc dataset containing individual and household data.
        master_vars (pd.DataFrame): A dataset containing information about the variables, including their origins.

    Returns:
        pd.DataFrame: A DataFrame containing the family dataset, including variables for children, mothers, fathers, and household information.
    """
    # Define variable groups
    weights = [col for col in ukhls_master.columns if col.endswith(("xw", "lw"))]

    # Variables for xwave data
    parents_xwave = usoc_config_03["parents_xwave"]
    both_xwave = usoc_config_03["both_xwave"]
    hhvars_xwave = usoc_config_03["hhvars_xwave"]

    # Variables for indall data
    parents_indall = usoc_config_03["parents_indall"]
    child_indall = usoc_config_03["child_indall"]
    both_indall = usoc_config_03["both_indall"] + weights

    # Get household variables for indall (excluding those in both_indall, parents_indall, and child_indall)
    hhvars_indall = master_vars[
        (master_vars["origin"] == "indall")
        & (
            negate_in(
                master_vars["variable"], both_indall + parents_indall + child_indall
            )
        )
    ]["variable"].tolist()

    # Variables for indresp data (parents)
    parents_rest = master_vars[master_vars["origin"] == "indresp"]["variable"].tolist()

    # Combine all parents variables (xwave, indall, and indresp)
    parents = list(set(parents_xwave + parents_indall + parents_rest))

    # Variables for child-related data
    child_rest = master_vars[
        (master_vars["origin"].isin(["child", "peachpreg", "peachnb", "peachparstyle"]))
    ]["variable"].tolist()

    # Combine all child variables (indall and child-related)
    child = list(set(child_indall + child_rest))

    # Combine both xwave and indall variables
    both = list(set(both_xwave + both_indall))

    # Variables for household-related data (including hhresp, lsoa, and external_inflation)
    hh_rest = master_vars[
        (master_vars["origin"].isin(["hhresp", "lsoa", "external_inflation"]))
    ]["variable"].tolist()

    # Combine all household variables (xwave, indall, and household-related)
    hh = list(set(hhvars_xwave + hhvars_indall + hh_rest))

    #### Create Child Dataset
    child_pidps = ukhls_master[ukhls_master["age_dv"] <= 5]["pidp"].drop_duplicates()
    children_vars = list(dict.fromkeys(["pidp", "wave"] + both + child + hh))

    children = ukhls_master[ukhls_master["pidp"].isin(child_pidps)][children_vars]

    # Add Time-Invariant Variables
    children[["wave_pregnancy", "wave_newborn", "wave_parstyle"]] = children.groupby(
        "pidp"
    )[["wave_pregnancy", "wave_newborn", "wave_parstyle"]].transform("max")

    # Parent 1 and Parent 2 Variables for the Parenting Style Data
    children["who_par1"] = np.select(
        [
            children["mnspid"] == children["pidp_par1"],
            children["fnspid"] == children["pidp_par1"],
            children["pidp_par1"] < 0,
            children["pidp_par1"]
            > 0
            & ~children["pidp_par1"].isna()
            & (children["mnspid"] != children["pidp_par1"])
            & (children["fnspid"] != children["pidp_par1"]),
        ],
        ["mum", "dad", "missing", "other"],
        default="missing",
    )
    children["who_par2"] = np.select(
        [
            children["mnspid"] == children["pidp_par2"],
            children["fnspid"] == children["pidp_par2"],
            children["pidp_par2"] < 0,
            children["pidp_par2"]
            > 0
            & ~children["pidp_par2"].isna()
            & (children["mnspid"] != children["pidp_par2"])
            & (children["fnspid"] != children["pidp_par2"]),
        ],
        ["mum", "dad", "missing", "other"],
        default="missing",
    )

    # Process Parent Variables for the Parenting Style Data
    par1mum = process_parents(children, 1, "mum")
    par2mum = process_parents(children, 2, "mum")
    par1dad = process_parents(children, 1, "dad")
    par2dad = process_parents(children, 2, "dad")
    par1oth = process_parents(children, 1, "other", add_number=True)
    par2oth = process_parents(children, 2, "other", add_number=True)

    # Combine Parent Styles Data and Add Back to Children, to indicate which parent is answering the questions
    parmum = pd.concat([par1mum, par2mum])
    pardad = pd.concat([par1dad, par2dad])

    children = children.drop(
        columns=[
            col
            for col in children.columns
            if col.endswith(("par1", "par2"))
            and col not in ["pidp_par1", "pidp_par2", "who_par1", "who_par2"]
        ]
    )
    children = pd.merge(children, parmum, on=["pidp", "wave"], how="left")
    children = pd.merge(children, pardad, on=["pidp", "wave"], how="left")
    children = pd.merge(children, par1oth, on=["pidp", "wave"], how="left")
    children = pd.merge(children, par2oth, on=["pidp", "wave"], how="left")

    # Create Parent Dataset
    mother_vars = list(dict.fromkeys(["pidp", "wave"] + both + parents + hh))
    mothers = ukhls_master[ukhls_master["pidp"].isin(children["mnspid"])][mother_vars]
    mothers = mothers.rename(columns={col: col + "_mum" for col in mothers.columns})

    father_vars = list(dict.fromkeys(["pidp", "wave"] + both + parents))
    fathers = ukhls_master[ukhls_master["pidp"].isin(children["fnspid"])][father_vars]
    fathers = fathers.rename(columns={col: col + "_dad" for col in fathers.columns})

    # Join Parents and Children
    ### To ensure that we get information from parents available in the waves prior to those which the children were born we first need to
    ### ensure that the mnspid and fnspid variables are not missing in the waves prior to birth. To do this I will pull the first observed
    ### values of mnspid and fnspid into the waves prior to birth (because mnspid and fnspid refer to either natural/step or adopted parents their value could change across waves
    ### e.g. if natural father was present at birth, then moved out, and was then replaced by a step father)

    children["mnspid_join"] = children.groupby("pidp")["mnspid"].transform(
        lambda x: x.mask(x < 0).ffill().bfill()
    )
    children["fnspid_join"] = children.groupby("pidp")["fnspid"].transform(
        lambda x: x.mask(x < 0).ffill().bfill()
    )

    # Join Children and Mothers
    family = pd.merge(
        children,
        mothers,
        left_on=["mnspid_join", "wave"],
        right_on=["pidp_mum", "wave_mum"],
        how="left",
    )

    # Join Children and Fathers
    family = family.merge(
        fathers,
        how="left",
        left_on=["fnspid_join", "wave"],
        right_on=["pidp_dad", "wave_dad"],
    )

    # For cases, where mother and father are not present, there may still be information on the household level using the ids from the parstyle information
    # Currently, we are not using this information. If we decide to use it, we can add it here.

    # Pregnant Variable for Mothers
    family["months_since_birth"] = (
        pd.to_datetime(family["int_date"]) - pd.to_datetime(family["dob_date"])
    ).dt.days // 30
    family["mumpreg"] = (
        (family["months_since_birth"] < 0) & (family["months_since_birth"] >= -9)
    ).astype(int)

    return family


if __name__ == "__main__":

    # Load Data
    ukhls_master = get_usoc_processed("ukhls_master_enriched_spine_dv")
    master_vars = get_usoc_processed("ukhls_master_vars_dv")

    # Create the Family Dataset
    family = create_family_dataset(ukhls_master, master_vars)

    # Save the Final Dataset
    family.to_parquet(f"{PROJECT_DIR}/outputs/data/family.parquet", engine="pyarrow")
