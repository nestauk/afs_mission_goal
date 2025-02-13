import pandas as pd
from afs_mission_goal.getters.usoc.raw.inflation import get_raw_inflation


def get_inflation() -> pd.DataFrame:
    """
    Function to read and process the inflation data

    Returns:
        pd.DataFrame: The inflation data
    """

    data = get_raw_inflation()

    # Rename columns
    data.columns = ["intdaty_dv", "inflation_index"]
    # Convert to numeric
    data["intdaty_dv"] = pd.to_numeric(data["intdaty_dv"], errors="coerce")
    # Drop missing values
    data = data[data["intdaty_dv"].notna()]
    return data
