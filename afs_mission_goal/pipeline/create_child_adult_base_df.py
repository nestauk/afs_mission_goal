from afs_mission_goal.getters.uk_data_service.processed.family_resources_filtered import (
    get_filtered_datasets,
)
from nesta_ds_utils.loading_saving import S3
import pandas as pd
import numpy as np
from typing import Dict, List
from afs_mission_goal import DS_BUCKET


def create_child_adult_base_df(
    filtered_data: Dict[str, pd.DataFrame]
) -> List[pd.DataFrame]:
    """
    Function to create the base dataframe with the child and adult data.
    Returns:
        pd.DataFrame: Base dataframe with the child and adult data.
    """

    frs2223 = filtered_data["frs2223"]
    child_data = filtered_data["child"]
    household_data = filtered_data["household"]

    # Calculating the income
    household_data["income"] = household_data[
        [
            "hh_gross_income_from_employment",
            "hh_gross_selfemployment_earnings",
            "hh_investment_income",
        ]
    ].sum(axis=1)
    # Calculating the benefits and merging with the income
    income_benefit = (
        household_data[["sernum", "income"]]
        .merge(frs2223[["sernum", "hh_benefit_income_gross"]], on="sernum")
        .drop_duplicates()
    )

    # Finding the number of children under 5
    children_0_5 = child_data[child_data.age_of_child_last_birthday <= 5][
        ["sernum", "age_of_child_last_birthday"]
    ]
    children_0_5 = (
        children_0_5.groupby("sernum")
        .count()
        .reset_index()
        .rename(columns={"age_of_child_last_birthday": "num_children_under_5"})
    )

    # Finding the number of adults
    adults = (
        filtered_data["adult"]
        .sernum.value_counts()
        .reset_index()
        .sort_values(by="sernum")
        .reset_index(drop=True)
        .rename(columns={"count": "num_adults"})
    )

    # Creating the base dataframe with the number of children under 5, number of adults, income and benefits for each household
    frs_base_df = (
        children_0_5.merge(income_benefit, on="sernum", how="outer")
        .merge(adults, on="sernum", how="outer")
        .replace(np.nan, 0)
    )[
        [
            "sernum",
            "num_children_under_5",
            "num_adults",
            "income",
            "hh_benefit_income_gross",
        ]
    ]

    # Creating the low income households with children under 5 dataframe
    # 409.2 is 60% of the median weekly income in the UK 2023
    lowincome_0_5 = frs_base_df[
        (frs_base_df.income <= 409.2) & (frs_base_df.num_children_under_5 > 0)
    ].reset_index(drop=True)

    return [frs_base_df, lowincome_0_5]


if __name__ == "__main__":
    print("Loading the filtered datasets")
    filtered_data = get_filtered_datasets()

    print("Creating dataframes with the child and adult data")
    base_df, lowincome_0_5 = create_child_adult_base_df(filtered_data)

    print("Uploading the dataframes to the S3 bucket")
    S3.upload_obj(
        base_df,
        bucket=DS_BUCKET,
        path_to=f"data/processed/filtered_dataframes/base_df.csv",
        kwargs_writing={"index": False},
    )

    S3.upload_obj(
        lowincome_0_5,
        bucket=DS_BUCKET,
        path_to=f"data/processed/filtered_dataframes/lowincome_0_5.csv",
        kwargs_writing={"index": False},
    )
