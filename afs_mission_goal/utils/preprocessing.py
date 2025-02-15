import pandas as pd
import numpy as np
import geopandas as gpd


def remove_nan_rows_and_columns(df) -> pd.DataFrame:
    """
    Remove rows and columns from a DataFrame that contain only NaN values.

    This function drops any rows or columns in the input DataFrame that are entirely filled with NaN values,
    streamlining the DataFrame to include only rows and columns with meaningful data.

    Parameters:
    ----------
    df : pandas.DataFrame
        The input DataFrame from which rows and columns with only NaN values should be removed.

    Returns:
    -------
    pandas.DataFrame
        A DataFrame with all-NaN rows and columns removed.

    Notes:
    ------
    - The function modifies the input DataFrame in place, so the original DataFrame will be affected unless it is copied beforehand.
    """
    # Remove the columns and rows that are all NaN
    df = df.dropna(axis=0, how="all")
    df = df.dropna(axis=1, how="all")
    return df


def preprocess_strings(strings: pd.Series) -> pd.Series:
    """Cleaning list of strings; removing punctuation and extra spaces,
    making the text lower case and placing _ for the remaining whitespace.

    Args:
        strings (pd.Series): Panda series of strings to clean.

    Returns:
        pd.Series: Pandas series of cleaned strings.
    """
    strings = (
        strings.str.replace(r"[/]", " ", regex=True)
        .str.replace(r"[:()\%']", "", regex=True)
        .str.replace("  ", " ", regex=True)
        .str.strip()
        .str.lower()
        .str.replace(r"[^a-zA-Z0-9_]", r"_", regex=True)
        .str.replace("___", "_", regex=True)
        .str.replace("__", "_", regex=True)
    )
    return strings


def preprocess_strings_reverse(strings: pd.Series) -> pd.Series:
    """Reverse the cleaning list of strings; removing _ from the cleaned strings and reintroduce whitespace.

    Args:
        strings (pd.Series): Panda series of strings to clean.

    Returns:
        pd.Series: Pandas series of cleaned strings.
    """
    strings = strings.str.replace("___", " ", regex=True).str.replace(
        "_", " ", regex=True
    )
    return strings


def column_txt_suppression(data, cols, suppressor="< 5") -> pd.DataFrame:
    """Supresses the numeric value in the columns specified in cols if less
    than 5.

    Args:
        data (pd.DataFrame): Dataframe containing the columns to be suppressed
        cols (list): List of column names to be suppressed
        suppressor (str, optional): String to replace the suppressed value with. Defaults to "< 5".

    Returns:
        pd.DataFrame: Dataframe with the suppressed columns
    """

    for col in cols:
        data[col + "_txt"] = data[col].apply(lambda x: suppressor if x < 5 else x)

    return data


def convert_geojson_to_gpd(geojson: dict) -> gpd.GeoDataFrame:
    """Converts a geojson to a geopandas dataframe.

    Args:
        geojson (dict): A geojson dictionary.

    Returns:
        gpd.GeoDataFrame: A geopandas dataframe.
    """
    gdf = gpd.GeoDataFrame.from_features(geojson["features"])
    return gdf
