import numpy as np
import pandas as pd


def calculate_binary_childcare(
    df: pd.DataFrame,
    new_col: str,
    formalusevars: list,
    missing_values: list = [-9, -2, -1],
):
    df[new_col] = 0

    # Count formal types of childcare
    formal_condition = df[formalusevars] == 1
    df[new_col] = formal_condition.sum(axis=1, skipna=True)

    # Add missing values
    for var_name in formalusevars:
        # Create mask for the conditions where new_col should be set to NaN
        mask_1 = (df[new_col] == 0) & df[var_name].isin(missing_values)
        mask_2 = (df[new_col] == 0) & df[var_name].isna()

        # Update the new_col column where any of the conditions are true
        df.loc[mask_1 | mask_2, new_col] = pd.NA

    # Recoding to binary
    df[new_col] = df[new_col].apply(
        lambda x: 1 if x >= 1 else (0 if pd.notna(x) else pd.NA)
    )

    return df


def clean_date_column(column):
    return column.apply(
        lambda x: (
            pd.NaT if x < 0 else pd.to_datetime(x, format="%Y%m%d", errors="coerce")
        )
    )


#### Function not currently in use ####
def create_vars(
    origin: str,
    df: pd.DataFrame,
    exclude_columns: list,
) -> pd.DataFrame:
    """Create a DataFrame to store the variables

    Args:
        origin (str): Origin of the data
        df (pd.DataFrame): DataFrame with the data
        exclude_columns (list): Columns to exclude

    Returns:
        pd.DataFrame: DataFrame with the variables
    """

    vars_df = pd.DataFrame(
        {
            "origin": origin,
            "variable": [col for col in df.columns if col not in exclude_columns],
        }
    )

    return vars_df


#### Function not currently in use ####
def create_vars_df(
    data_dict: dict,
    exclude_columns: list,
) -> pd.DataFrame:
    """Create a DataFrame with the variables from the data_dict

    Args:
        data_dict (dict): Dictionary with the dataframes
        exclude_columns (list): Columns to exclude

    Returns:
        pd.DataFrame: DataFrame with the variables
    """

    vars_df = pd.DataFrame()
    for key, value in data_dict.items():
        vars_df = pd.concat([vars_df, create_vars(key, value, exclude_columns)], axis=0)
    return vars_df


def parse_date(row):
    """Parse a date from a row with year and month.

    Args:
        row (pd.Series): Row with year/month values

    Returns:
        pd.Timestamp: Date parsed from the year and month
    """
    year, month = row
    if year < 0 or month < 1 or month > 12:
        return pd.NaT
    return pd.to_datetime(f"{year}-{month:02d}-01")


def create_date_columns(df, columns):
    """Create a date column from a list of columns (year, month), setting day as 1.

    Args:
        df (pd.DataFrame): DataFrame with the data
        columns (list): List of columns (year, month) to create the date column

    Returns:
        pd.Series: Series with the date column
    """
    df = df.copy()  # Avoid modifying original DataFrame

    for col in columns:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(-1).astype(int)

    return df[columns].apply(parse_date, axis=1)


def negate_in(x, values):
    """Negate the isin function

    Args:
        x (pd.Series): Series to check the values
        values (list): Values to check if they are in x

    Returns:
        pd.Series: Series with the negated isin function
    """
    return ~x.isin(values)


# Separate and Rename Variables for Parent 1 and Parent 2
def process_parents(children, par_number, new_label, add_number=False):
    """
    Filters and renames columns based on the specified parent number and new label.

    Args:
        children (pd.DataFrame): The input dataframe.
        par_number (str): The parent number to filter and rename (e.g., "par1", "par2").
        new_label (str): The new label to replace the parent number (e.g., "mum", "dad").
        add_number (bool): Whether to add the parent number to the new label.
    Returns:
        pd.DataFrame: Transformed dataframe.
    """
    par_col = f"who_par{par_number}"  # Column used for filtering
    suffix = f"_par{par_number}"  # Suffix to match column names

    # Ensure the filtering column exists
    if par_col not in children.columns:
        raise ValueError(f"Column '{par_col}' not found in the DataFrame.")

    if add_number:
        return (
            children[
                children[par_col] == new_label
            ]  # Filter rows where who_parX == new_label
            .filter(
                regex=rf"pidp$|wave$|{suffix}$"
            )  # Select columns: 'pidp', 'wave', and those ending with '_parX'
            .rename(
                columns=lambda x: x.replace(suffix, f"_{new_label}{par_number}")
            )  # Rename columns
            .reset_index(drop=True)
        )
    else:
        return (
            children[
                children[par_col] == new_label
            ]  # Filter rows where who_parX == new_label
            .filter(
                regex=rf"pidp$|wave$|{suffix}$"
            )  # Select columns: 'pidp', 'wave', and those ending with '_parX'
            .drop(columns=[f"pidp{suffix}"])  # Drop 'pidp_parX' if it exists
            .rename(
                columns=lambda x: x.replace(suffix, f"_{new_label}")
            )  # Rename columns
            .reset_index(drop=True)
        )
