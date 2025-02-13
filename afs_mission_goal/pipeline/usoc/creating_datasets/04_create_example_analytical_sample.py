import pandas as pd
import numpy as np
from afs_mission_goal.getters.usoc.processed.usoc_master import get_usoc_processed
from afs_mission_goal import PROJECT_DIR

# Load the dataset
family = get_usoc_processed("family")

# Create tailored variables
family["sdq_5"] = np.where(family["age_dv"] == 5, family["chsdqtd_dv"], np.nan)
family["mum_mh_preg"] = np.where(
    family["mumpreg"] == 1, family["sf12mcs_dv_mum"], np.nan
)

family["yellkid_mum_45"] = np.select(
    [
        (family["age_dv"] == 5) & family["yellkid_mum"].notna(),
        (family["age_dv"] == 4) & family["yellkid_mum"].notna(),
    ],
    [family["yellkid_mum"], family["yellkid_mum"]],
    default=np.nan,
)

family["englang_mum_nonmis"] = np.select(
    [family["bornuk_dv_mum"] == 1, family["englang_mum"] > 0],
    [1, family["englang_mum"]],
    default=np.nan,
)

# Pull tailored variables into every wave
family = (
    family.groupby("pidp")
    .apply(
        lambda group: group.assign(
            sdq_5=group["sdq_5"].max(skipna=True),
            mum_mh_preg=group["mum_mh_preg"].max(skipna=True),
            yellkid_mum_45=group["yellkid_mum_45"].max(skipna=True),
            englang_mum_nonmis=group["englang_mum_nonmis"].max(skipna=True),
            everpreg=group["mumpreg"].max(skipna=True),
        )
    )
    .reset_index(drop=True)
)

# Select only relevant waves
sample = family[family["age_dv"] == 5].reset_index(drop=True)

#### IDENTIFY THE CORRECT WEIGHTS ####
# You would then need to select the correct weights for your research question -
# this can be straightforward or fiendishly difficult. Our dataset contains every potential weight.
# To pick the correct one make sure you consult the Usoc weighting guidance and ask a question in the Usoc help forums if unsure.
# Insert the correct weights here


# Code missing values correctly
dependent = ["sdq_5"]
independent = ["mum_mh_preg", "yellkid_mum_45", "englang_mum_nonmis"]

sample[dependent + independent] = sample[dependent + independent].apply(
    lambda x: x.map(lambda y: np.nan if y < 0 else y)
)

# Check sample size
sample["nonmis"] = sample.apply(
    lambda row: (
        1
        if (
            pd.notna(row["sdq_5"])
            and pd.notna(row["mum_mh_preg"])
            and pd.notna(row["yellkid_mum_45"])
            and pd.notna(row["englang_mum_nonmis"])
        )
        else 0
    ),
    axis=1,
)

# Select relevant columns
sample = sample[["pidp", "wave"] + dependent + independent + ["nonmis", "everpreg"]]

# Examine missingness (using simple descriptive stats as `finalfit` equivalent is not directly available)
print("Overall sample:")
print(sample[dependent + independent].head())

# Among the sample observed in pregnancy
sample_everpreg = sample[sample["everpreg"] == 1]
print("Sample ever pregnant:")
print(sample_everpreg[dependent + independent].head())

# Save the processed analytical sample
sample.to_csv(f"{PROJECT_DIR}/outputs/data/analytical_sample.csv", index=False)
