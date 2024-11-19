import pandas as pd
from afs_mission_goal.utils.preprocessing import (
    remove_nan_rows_and_columns,
    preprocess_strings,
)
from afs_mission_goal import health_review_config


def remove_extra_rows_and_columns_chps(df):
    """
    Clean and reformat a DataFrame by identifying a new header row based on a specific marker,
    removing extraneous rows and columns, and resetting column names.

    This function locates the row in `df` containing the specified marker `"row_id"`, treats this row as the new header,
    and renames the DataFrame columns accordingly. It then removes all rows above the new header and drops any columns
    or rows that are entirely NaN. The DataFrame's index is reset after cleaning.

    Parameters:
    ----------
    df : pandas.DataFrame
        The input DataFrame containing raw data with potential extra rows, columns, and a specific row serving as
        a new header marker.

    Returns:
    -------
    pandas.DataFrame
        A cleaned DataFrame with updated column names, extraneous rows/columns removed, and an index reset.

    Notes:
    ------
    - The function searches for the row with `"row_id"` to locate the new header row.
    - Rows and columns that are entirely NaN are dropped.
    """
    # Identify the row number and column for where we want the new column names to be using "row_id" as a marker.
    find_row_for_header = [
        (index, column)
        for index, row in df.iterrows()
        for column, value in row.items()
        if value == "row_id"
    ]
    # Pull out just the row number
    row_number = find_row_for_header[0][0]
    # Pull out the row with the new column names
    header = df.iloc[row_number]
    # Remove the rows above the new column names
    df = df[row_number + 1 :]
    # Rename the columns to the new header
    df.columns = header
    # Remove the columns and rows that are all NaN
    df = remove_nan_rows_and_columns(df)
    return df.reset_index(drop=True)


def keep_only_relevant_columns_chps_individual(df) -> pd.DataFrame:
    """
    Retain only the relevant columns from the input DataFrame and rename the 'review' column to 'review_period'.

    This function takes a DataFrame containing various columns, renames the 'review' column to 'review_period', and returns a new DataFrame that includes only the 'review_period' and 'year' columns, along with all columns following 'Number of reviews'.

    Args:
        df (pd.DataFrame): The input DataFrame containing multiple columns, including 'review', 'year', and 'Number of reviews'.

    Returns:
        pd.DataFrame: A new DataFrame containing:
            -  'review_period': renamed from 'review'
            -  'year': the year associated with the reviews
            -  all columns that come after 'Number of reviews' in the original DataFrame.
    """
    df = df.rename(columns={"review": "review_period"})
    columns_ = df.columns
    index = columns_.get_loc("Number of reviews")
    review_index = columns_.get_loc("review_period")
    year_index = columns_.get_loc("year")
    df = df.iloc[:, [review_index, year_index] + list(range(index - 1, len(columns_)))]
    return df


def keep_only_relevant_columns_chps_simd(df) -> pd.DataFrame:
    """
    Retain only the relevant columns from the input DataFrame, renaming specific
    columns to standardized names.

    This function processes a DataFrame containing various columns related to reviews,
    local authorities, and other metrics. It renames columns for consistency and returns
    a new DataFrame that includes the relevant columns based on the presence of certain
    key columns. Specifically, it renames 'review' to 'review_period', 'finyr' to 'year',
    and standardizes local authority names and codes, while selecting the appropriate
    columns to return based on the presence of local authority information.

    Parameters:
    df (pd.DataFrame): The input DataFrame containing multiple columns, potentially including:
        - 'review' or 'review                          .'
        - 'finyr' or 'finyr                             .'
        - 'Council, to allow filter                            '
        - 'ca code'
        - 'Number of reviews'

    Returns:
    pd.DataFrame: A new DataFrame containing:
        - 'review_period': renamed from 'review' or 'review                          .'
        - 'year': renamed from 'finyr' or 'finyr                             .'
        - 'local_authority_name' and 'local_authority_code' if present
        - All columns following 'Number of reviews' in the original DataFrame.
    """
    if "review                          ." in df.columns:
        df = df.rename(columns={"review                          .": "review_period"})
    else:
        df = df.rename(columns={"review": "review_period"})
    if "finyr                             ." in df.columns:
        df = df.rename(columns={"finyr                             .": "year"})
    if "Council, to allow filter                            " in df.columns:
        df = df.rename(
            columns={
                "Council, to allow filter                            ": "local_authority_name"
            }
        )
        df = df.rename(columns={"ca code": "local_authority_code"})
    columns_ = df.columns
    index = columns_.get_loc("Number of reviews")
    review_index = columns_.get_loc("review_period")
    year_index = columns_.get_loc("year")
    if "local_authority_name" in columns_:
        la_index = columns_.get_loc("local_authority_name")
        la_code_index = columns_.get_loc("local_authority_code")
        df = df.iloc[
            :,
            [la_code_index, la_index, review_index, year_index]
            + list(range(index - 2, len(columns_))),
        ]
    else:
        df = df.iloc[
            :, [review_index, year_index] + list(range(index - 2, len(columns_)))
        ]
    return df


def clean_chps(df, simd=False) -> pd.DataFrame:
    """
    Clean and preprocess the input DataFrame for the CHPS data.

    This function performs a series of data cleaning operations on the input DataFrame,
    including removing unnecessary rows and columns, retaining relevant columns based on
    the specified mode, mapping review periods, and preprocessing column names for
    consistency.

    The cleaning steps are determined by the value of the `simd` parameter:
    - If `simd` is False, it retains columns specific to individual data.
    - If `simd` is True, it retains columns relevant to SIMD (Scottish Index of Multiple Deprivation) data.

    The function also checks for a review period labeled "13m" and maps it to a configuration
    defined in `health_review_config`. Finally, the column names are cleaned by removing
    punctuation and extra spaces, converting to lowercase, and replacing whitespace with
    underscores.

    Args:
        df (pd.DataFrame): The input DataFrame containing CHPS data that requires cleaning.
        simd (bool): A flag indicating whether to retain SIMD-specific columns. Defaults to False.

    Returns:
        pd.DataFrame: A cleaned DataFrame containing:
            - Relevant columns based on the specified mode (individual or SIMD).
            - Mapped review periods where applicable.
            - Preprocessed column names, with punctuation removed and
                        whitespace replaced by underscores.
    """

    df_clean = remove_extra_rows_and_columns_chps(df)
    if simd == False:
        df_clean = keep_only_relevant_columns_chps_individual(df_clean)
    else:
        df_clean = keep_only_relevant_columns_chps_simd(df_clean)
    if "13m" in df_clean.review_period.unique():
        df_clean["review_period"] = df_clean.review_period.map(health_review_config)
    df_clean.columns = list(preprocess_strings(pd.Series(df_clean.columns)))
    return df_clean


def change_dtype(df, keep_suppression=False) -> pd.DataFrame:
    """
    Change the data type of columns in a DataFrame that contain the word 'number'.

    This function searches for columns in the provided DataFrame that have
    'number' in their column name. It then removes any commas from the
    string representation of the numbers in these columns and converts
    the cleaned data to integers.

    Parameters:
    -----------
    df : pd.DataFrame
        A pandas DataFrame that contains columns potentially labeled with
        the word 'number'. The function modifies these columns in place.

    Returns:
    --------
    pd.DataFrame
        The modified DataFrame with the specified columns' data types
        changed to integers.

    Notes:
    ------
    - This function assumes that all entries in the relevant columns can
      be safely converted to integers after removing commas. If any value
      cannot be converted (e.g., due to non-numeric characters), a
      ValueError will be raised.
    - The original DataFrame is modified in place; thus, the changes
      will be reflected in the input DataFrame `df`.
    """
    columns_containing_numbers = df.columns[df.columns.str.contains("number")]
    for col in columns_containing_numbers:
        df[col] = df[col].apply(lambda x: str(x).replace(",", ""))
        if keep_suppression == True:
            df[col] = df[col].apply(lambda x: int(x) if x != "<5" else x)
        else:
            df[col] = df[col].replace("<5", 5)
            df[col] = df[col].astype(int)
    return df
