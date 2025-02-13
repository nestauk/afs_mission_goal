import pandas as pd
import numpy as np
from afs_mission_goal import PROJECT_DIR


# Function to get childcare type categories
def get_childcare_category(
    value: int, formal_vars: list, informal_vars: list, add_text: str = ""
) -> str:
    """Set the category of the childcare type based on the value.

    Returns:
        str: The category of the childcare type
    """

    if add_text != "" and pd.notna(value):
        value = add_text + str(int(value))

    if value in formal_vars:
        return "formal"
    elif value in informal_vars:
        return "informal"
    else:
        return None


def categorize_row(row: pd.Series) -> str:
    """Categorize a row based on the categories of the first, second, and third childcare types.

    Args:
        row (pd.Series): The row containing the categories of the first, second, and third childcare types

    Returns:
        str: The category of the row
    """
    categories = {
        val
        for val in [
            row["first_category"],
            row["second_category"],
            row["third_category"],
        ]
        if pd.notna(val)
    }

    if not categories:  # If all values were NaN, return NaN
        return np.nan
    elif categories == {"formal"}:
        return "formal"
    elif categories == {"informal"}:
        return "informal"
    else:
        return "mixed"


# Clean the data for waves 1-5
def process_wave_data_15(
    df: pd.DataFrame, wave: list, formal_vars: list, informal_vars: list, use_vars: list
) -> pd.DataFrame:
    """Processes childcare data for waves 1-5 by filtering, reshaping, ranking,
    and categorizing childcare types.

    This includes the following steps:
    - Filters the dataframe for the specified waves.
    - Selects relevant variables and reshapes the data into a long format.
    - Extracts childcare types used by individuals and assigns a rank based on usage order.
    - Pivots the data to create separate columns for the first, second, and third childcare types.
    - Maps childcare types to their respective categories (formal or informal).
    - Identifies whether all childcare types used are formal, informal, or mixed.

    Args:
        df (pd.DataFrame): The USoc dataframe to process.
        wave (list): A list of wave identifiers to process.
        formal_vars (list): A list of childcare variables classified as formal.
        informal_vars (list): A list of childcare variables classified as informal.
        use_vars (list): A list of childcare-related variables to be included in the processing.

    Returns:
        pd.DataFrame: A processed dataframe with categorized childcare usage.
    """
    # Extract and reshape the data
    id_vars = ["pidp", "wave"]
    wave_df = df[df["wave"].isin(wave)]
    wave_df = wave_df[use_vars + id_vars].melt(
        id_vars=id_vars, value_vars=use_vars, var_name="name"
    )
    wave_df = wave_df[wave_df["value"] == 1]

    # Add rank to childcare types
    wave_df["rank"] = wave_df.groupby(id_vars).cumcount() + 1

    # Pivot to create columns for first, second, third, etc., childcare types
    pivoted = (
        wave_df.pivot(index=id_vars, columns="rank", values="name")
        .reset_index()
        .rename(columns={1: "first", 2: "second", 3: "third"})
    )

    result_df = pivoted.assign(
        first=lambda x: x["first"].str.split("ch2a").str[1],
        second=lambda x: x["second"].str.split("ch2a").str[1],
        third=lambda x: x["third"].str.split("ch2a").str[1],
    )

    # Linking codes
    id_links = ["wrkch3code1", "wrkch3code2", "wrkch3code3"]

    result_df = result_df.merge(
        df[["pidp", "wave"] + id_links], on=["pidp", "wave"], how="left"
    )

    # Map to categories
    result_df["first_category"] = result_df.apply(
        lambda x: (
            get_childcare_category(
                x["first"], formal_vars, informal_vars, add_text="wrkch2"
            )
            if x["wave"] in ["a", "b", "c"]
            else get_childcare_category(
                x[id_links[0]], formal_vars, informal_vars, add_text="wrkch2"
            )
        ),
        axis=1,
    )
    result_df["second_category"] = result_df.apply(
        lambda x: (
            get_childcare_category(
                x["second"], formal_vars, informal_vars, add_text="wrkch2"
            )
            if x["wave"] in ["a", "b", "c"]
            else get_childcare_category(
                x[id_links[1]], formal_vars, informal_vars, add_text="wrkch2"
            )
        ),
        axis=1,
    )
    result_df["third_category"] = result_df.apply(
        lambda x: (
            get_childcare_category(
                x["third"], formal_vars, informal_vars, add_text="wrkch2"
            )
            if x["wave"] in ["a", "b", "c"]
            else get_childcare_category(
                x[id_links[2]], formal_vars, informal_vars, add_text="wrkch2"
            )
        ),
        axis=1,
    )

    # Create column to identify if all three categories are formal, informal, or mixed,
    result_df["formal_informal"] = result_df.apply(categorize_row, axis=1)

    # Drop unnecessary columns
    result_df = result_df.drop(columns=["first", "second", "third"] + id_links)

    return result_df


# Function to calculate hours for waves 4-5 (there is no linking for 1-3)
def calculate_hours_45(df: pd.DataFrame) -> pd.DataFrame:
    """Calculates childcare hours for waves 4-5, calculating formal and informal hours separately.

    This includes the following steps:
    - Initializes columns for formal and informal childcare hours.
    - Identifies invalid or missing values and sets corresponding hours to NaN.
    - Iterates through childcare categories to sum hours separately for formal and informal childcare.
    - Computes the total childcare hours by summing across all relevant columns.
    - Drops intermediary category columns after processing.

    Please note that this function is specific to waves 4-5 and should not be used for other waves,
    due to the lack of linking codes in waves 1-3.

    Args:
        df (pd.DataFrame): The USoc dataframe to process for waves 4-5.

    Returns:
        pd.DataFrame: A modified dataframe with new columns:
                      - 'formal_hours_45': Total hours spent on formal childcare.
                      - 'informal_hours_45': Total hours spent on informal childcare.
    """

    # Initialize the columns
    df["formal_hours_45"] = 0
    df["informal_hours_45"] = 0

    # Define categories and corresponding columns
    categories = ["first_category", "second_category", "third_category"]
    wrkch_columns = ["wrkch31", "wrkch32", "wrkch33"]

    # Set to NaN if any of the invalid conditions are met
    invalid_conditions_formal = (
        (df["wrkch31"].isin([-1, -2, -9]) & (df["first_category"] == "formal"))
        | (df["wrkch32"].isin([-1, -2, -9]) & (df["second_category"] == "formal"))
        | (df["wrkch33"].isin([-1, -2, -9]) & (df["third_category"] == "formal"))
    ) | df[wrkch_columns].isna().any(axis=1)

    invalid_conditions_informal = (
        (df["wrkch31"].isin([-1, -2, -9]) & (df["first_category"] == "informal"))
        | (df["wrkch32"].isin([-1, -2, -9]) & (df["second_category"] == "informal"))
        | (df["wrkch33"].isin([-1, -2, -9]) & (df["third_category"] == "informal"))
    ) | df[wrkch_columns].isna().any(axis=1)

    df.loc[invalid_conditions_formal, "formal_hours_45"] = np.nan
    df.loc[invalid_conditions_informal, "informal_hours_45"] = np.nan

    # Iterate through categories and columns to calculate hours
    for category, wrkch in zip(categories, wrkch_columns):
        # Formal hours
        df["formal_hours_45"] += np.where(
            (df[category] == "formal") & (df[wrkch].fillna(0) > 0), df[wrkch], 0
        )

        # Informal hours
        df["informal_hours_45"] += np.where(
            (df[category] == "informal") & (df[wrkch].fillna(0) > 0), df[wrkch], 0
        )

    # Drop unnecessary columns
    df = df.drop(columns=categories)

    return df


# Function to calculate total hours
def calculate_hours(
    df: pd.DataFrame, columns: list, output_column: str
) -> pd.DataFrame:
    """Calculates total hours by summing specified columns while handling missing values.

    This includes the following steps:
    - Initializes the output column with a default value of 0.
    - Iterates over the specified columns, summing values where they are greater than 0.
    - Identifies missing or invalid values (-9, -2, -1) and sets the output column to NaN where applicable.
    - Handles NaN values by setting the output column to NaN if all relevant columns are missing.

    Args:
        df (pd.DataFrame): The USoc data containing the number of hours to be summed.
        columns (list): A list of column names representing the hours to be summed.
        output_column (str): The name of the new column where the total hours will be stored.

    Returns:
        pd.DataFrame: The updated dataframe with the calculated total hours in `output_column`.
    """
    # Sum up hours where values are greater than 0
    df[output_column] = 0
    for col in columns:
        df[output_column] += np.where(df[col].fillna(0) > 0, df[col], 0)

    # Handle missing conditions
    missing_condition = (df[output_column] == 0) & df[columns].isin([-9, -2, -1]).any(
        axis=1
    )
    df.loc[missing_condition, output_column] = np.nan

    missing_condition_na = (df[output_column] == 0) & df[columns].isna().any(axis=1)
    df.loc[missing_condition_na, output_column] = np.nan

    return df


# Main loop for processing waves
def process_ukhls_data(
    ukhls_master: pd.DataFrame,
    formal_vars_15: list,
    informal_vars_15: list,
    formal_vars_612: list,
    informal_vars_612: list,
    formal_vars_13: list,
    informal_vars_13: list,
) -> pd.DataFrame:
    """Processes USoc childcare data across different waves (1-13), computing formal and informal childcare hours
    for the 3 most used childcare types for waves 1-5 (all thats avaiable), and all childcare types for waves 6-13.

    This includes the following steps:
    - Processes waves 1-5 by reshaping childcare variables and categorizing childcare types.
    - Merges processed data back into the main dataset.
    - Computes total childcare hours for waves 1-5.
    - Calculates formal and informal childcare hours for waves 4-5 using `calculate_hours_45`.
    This is not applicable for waves 1-3 due to the lack of linking codes between questions (the columns are NA).
    - Assigns formal and informal hours for waves 1-3 based on childcare type.
    - Computes formal and informal childcare hours for waves 6-12 using `calculate_hours`.
    - Computes formal and informal childcare hours for wave 13 using `calculate_hours`.
    - Combines childcare hours across all waves into a unified set of columns.
    - Computes total childcare hours by summing formal and informal hours where available.
    E.g. we have only have formal/informal hours for waves 1-3 where all of the options are formal/informal.
    - Cleans up the dataframe by dropping intermediate columns.

    Args:
        ukhls_master (pd.DataFrame): The master dataset containing UKHLS childcare data.
        formal_vars_15 (list): List of formal childcare variables for waves 1-5.
        informal_vars_15 (list): List of informal childcare variables for waves 1-5.
        formal_vars_612 (list): List of formal childcare variables for waves 6-12.
        informal_vars_612 (list): List of informal childcare variables for waves 6-12.
        formal_vars_13 (list): List of formal childcare variables for wave 13.
        informal_vars_13 (list): List of informal childcare variables for wave 13.

    Returns:
        pd.DataFrame: The processed dataset with computed childcare hours and categorized childcare usage.
    """
    # letters
    letters = "abcdefghijkl"
    # Process waves 1-5, 6-12, and 13
    usevars_15 = [col for col in ukhls_master.columns if col.startswith("wrkch2a")]

    # Process Waves 1-5
    long_ukhls_master_15 = process_wave_data_15(
        ukhls_master,
        wave=list(letters[0:5]),
        formal_vars=formal_vars_15,
        informal_vars=informal_vars_15,
        use_vars=usevars_15,
    )

    # Merge back to main dataset
    ukhls_master = pd.merge(
        ukhls_master, long_ukhls_master_15, on=["pidp", "wave"], how="left"
    )

    # Create total hours for waves 1-5
    ukhls_master["total_cc_hours_15"] = ukhls_master[
        ["wrkch31", "wrkch32", "wrkch33"]
    ].sum(axis=1)

    # Calculate formal and informal hours for waves 4-5
    ukhls_master_45 = calculate_hours_45(
        ukhls_master[ukhls_master.wave.isin(list(letters[3:5]))]
    )

    ukhls_master = pd.concat(
        [ukhls_master[~ukhls_master.wave.isin(list(letters[3:5]))], ukhls_master_45]
    )

    # Calculate formal and informal hours for waves 1-3, where possible
    ukhls_master["formal_hours_15"] = [
        (
            ukhls_master["formal_hours_45"][i]
            if ukhls_master["wave"][i] in list(letters[3:5])
            else (
                ukhls_master["total_cc_hours_15"][i]
                if ukhls_master["wave"][i] in list(letters[0:3])
                and ukhls_master["formal_informal"][i] == "formal"
                else np.nan
            )
        )
        for i in range(len(ukhls_master))
    ]

    ukhls_master["informal_hours_15"] = [
        (
            ukhls_master["informal_hours_45"][i]
            if ukhls_master["wave"][i] in list(letters[3:5])
            else (
                ukhls_master["total_cc_hours_15"][i]
                if ukhls_master["wave"][i] in list(letters[0:3])
                and ukhls_master["formal_informal"][i] == "informal"
                else np.nan
            )
        )
        for i in range(len(ukhls_master))
    ]

    # Calculate formal and informal hours for waves 6-12
    ukhls_master = calculate_hours(ukhls_master, formal_vars_612, "formal_hours_612")
    ukhls_master = calculate_hours(
        ukhls_master, informal_vars_612, "informal_hours_612"
    )

    # Calculate formal and informal hours for wave 13

    # Reuse the calculate_hours function to compute formal and informal hours
    ukhls_master = calculate_hours(ukhls_master, formal_vars_13, "formal_hours_13")
    ukhls_master = calculate_hours(ukhls_master, informal_vars_13, "informal_hours_13")

    # Combine final hours
    ukhls_master["formal_hours"] = [
        (
            ukhls_master["formal_hours_15"][i]
            if ukhls_master["wave"][i] in list(letters[0:5])
            else (
                ukhls_master["formal_hours_612"][i]
                if ukhls_master["wave"][i] in list(letters[5:12])
                else (
                    ukhls_master["formal_hours_13"][i]
                    if ukhls_master["wave"][i] == "m"
                    else np.nan
                )
            )
        )
        for i in range(len(ukhls_master))
    ]

    ukhls_master["informal_hours"] = [
        (
            ukhls_master["informal_hours_15"][i]
            if ukhls_master["wave"][i] in list(letters[0:5])
            else (
                ukhls_master["informal_hours_612"][i]
                if ukhls_master["wave"][i] in list(letters[5:12])
                else (
                    ukhls_master["informal_hours_13"][i]
                    if ukhls_master["wave"][i] == "m"
                    else np.nan
                )
            )
        )
        for i in range(len(ukhls_master))
    ]

    # Get total hours, based on 1-5 and 6-13
    ukhls_master["total_cc_hours"] = [
        (
            ukhls_master["total_cc_hours_15"][i]
            if ukhls_master["wave"][i] in list(letters[0:5])
            else (
                ukhls_master["formal_hours_612"][i]
                + ukhls_master["informal_hours_612"][i]
                if ukhls_master["wave"][i] in list(letters[5:12])
                else (
                    ukhls_master["formal_hours_13"][i]
                    + ukhls_master["informal_hours_13"][i]
                    if ukhls_master["wave"][i] == "m"
                    else np.nan
                )
            )
        )
        for i in range(len(ukhls_master))
    ]

    # Clean up the dataframe
    ukhls_master.drop(
        columns=[
            "formal_hours_15",
            "informal_hours_15",
            "formal_hours_45",
            "informal_hours_45",
            "total_cc_hours_15",
            "formal_hours_612",
            "informal_hours_612",
            "formal_hours_13",
            "informal_hours_13",
        ],
        inplace=True,
    )

    return ukhls_master


def categorize_formal_hours(formal_hours: int) -> int:
    """Calculate the category of formal childcare hours based on the number of hours.

    Args:
        formal_hours (int): The number of formal childcare hours

    Returns:
        int: The category of formal childcare hours, or NaN if the value is missing or unexpected
    """
    if formal_hours > 20:
        return 3
    elif 10 < formal_hours <= 20:
        return 2
    elif 0 < formal_hours <= 10:
        return 1
    elif formal_hours == 0:
        return 0
    return np.nan  # If the value is missing or unexpected
