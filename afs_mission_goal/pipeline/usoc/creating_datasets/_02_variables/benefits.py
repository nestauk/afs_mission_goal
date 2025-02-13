import pandas as pd
import numpy as np


def calculate_benefits(ukhls_master: pd.DataFrame) -> pd.DataFrame:
    """
    Derive the binary variables for benefits from the UKHLS dataset.

    To see information on the benefits variables, see the UKHLS documentation:
    https://www.understandingsociety.ac.uk/documentation/mainstage
    Args:
        ukhls_master (pd.DataFrame): The UKHLS dataset

    Returns:
        pd.DataFrame: The UKHLS dataset with the derived benefits variables
    """
    # Define helper variables for letters
    letters = list("abcdefghijklmnopqrstuvwxyz")

    ukhls_master["benuc2"] = np.select(
        [
            (ukhls_master["benbase4"] == 1)
            | (ukhls_master["bendis11"] == 1)
            | (ukhls_master["benhou5"] == 1)
            | (ukhls_master["bentax6"] == 1)
            | (ukhls_master["benunemp3"] == 1),
            (ukhls_master["bendis11"].isin([0, -8]))
            & (ukhls_master["wave"].isin(letters[0:2])),
            (ukhls_master["benhou5"].isin([0, -8]))
            & (ukhls_master["bentax6"].isin([0, -8]))
            & (ukhls_master["benunemp3"].isin([0, -8]))
            & (ukhls_master["wave"] == letters[2]),
            (ukhls_master["bendis11"].isin([0, -8]))
            & (ukhls_master["benhou5"].isin([0, -8]))
            & (ukhls_master["bentax6"].isin([0, -8]))
            & (ukhls_master["benunemp3"].isin([0, -8]))
            & (ukhls_master["wave"] == letters[3]),
            (ukhls_master["bendis11"].isin([0, -8]))
            & (ukhls_master["benhou5"].isin([0, -8]))
            & (ukhls_master["benunemp3"].isin([0, -8]))
            & (ukhls_master["wave"] == letters[4]),
            (ukhls_master["benbase4"].isin([0, -8]))
            & (ukhls_master["wave"].isin(letters[5:13])),
        ],
        [1, 0, 0, 0, 0, 0],
    )

    ukhls_master["benincsup2"] = np.select(
        [
            (ukhls_master["btype2"] == 1) | (ukhls_master["benbase1"] == 1),
            (ukhls_master["btype2"].isin([0, -8]))
            & (ukhls_master["wave"].isin(letters[0:5])),
            (ukhls_master["benbase1"].isin([0, -8]))
            & (ukhls_master["wave"].isin(letters[5:13])),
        ],
        [1, 0, 0],
    )

    ukhls_master["benhouse2"] = np.select(
        [
            (ukhls_master["benhou1"] == 1) | (ukhls_master["othben8"] == 1),
            (ukhls_master["benhou1"].isin([0, -8]))
            & (ukhls_master["wave"].isin(letters[0:5])),
            (ukhls_master["othben8"].isin([0, -8]))
            & (ukhls_master["wave"].isin(letters[5:13])),
        ],
        [1, 0, 0],
    )

    ukhls_master["benincjsa2"] = np.select(
        [
            (ukhls_master["benunemp1"] == 1) | (ukhls_master["benbase2"] == 1),
            (ukhls_master["benunemp1"].isin([0, -8]))
            & (ukhls_master["wave"].isin(letters[0:5])),
            (ukhls_master["benbase2"].isin([0, -8]))
            & (ukhls_master["wave"].isin(letters[5:13])),
        ],
        [1, 0, 0],
    )

    ukhls_master["benincesa2"] = np.select(
        [(ukhls_master["bendis2"] == 1), (ukhls_master["bendis2"].isin([0, -8]))],
        [1, 0],
    )

    ukhls_master["benwtc2"] = np.select(
        [
            (ukhls_master["bentax1"] == 1) | (ukhls_master["othben5"] == 1),
            (ukhls_master["bentax1"].isin([0, -8]))
            & (ukhls_master["wave"].isin(letters[0:5])),
            (ukhls_master["othben5"].isin([0, -8]))
            & (ukhls_master["wave"].isin(letters[5:13])),
        ],
        [1, 0, 0],
    )

    ukhls_master["bencb2"] = np.select(
        [
            (ukhls_master["btype5"] == 1) | (ukhls_master["benbase3"] == 1),
            (ukhls_master["btype5"].isin([0, -8]))
            & (ukhls_master["wave"].isin(letters[0:5])),
            (ukhls_master["benbase3"].isin([0, -8]))
            & (ukhls_master["wave"].isin(letters[5:13])),
        ],
        [1, 0, 0],
    )

    ukhls_master["benctc2"] = np.select(
        [(ukhls_master["benctcorig"] == 1), (ukhls_master["benctcorig"].isin([2, -8]))],
        [1, 0],
    )

    ##### CHECK AND MERGE THE TWO VARIABLES #####
    ukhls_master = ukhls_master.assign(
        benhouse2=np.where(
            (ukhls_master["benhouse"] == 1)
            & (ukhls_master["benhouse2"].isna())
            & (ukhls_master["wave"] == "b"),
            1,
            ukhls_master["benhouse"],
        ),
        benctc2=np.where(
            (ukhls_master["benctc"] == 1) & (ukhls_master["benctc2"] == 0),
            1,
            ukhls_master["benctc2"],
        ),
        bencb2=np.where(
            (ukhls_master["bencb"] == 1) & (ukhls_master["bencb2"] == 0),
            1,
            ukhls_master["bencb2"],
        ),
        benwtc2=np.where(
            (ukhls_master["benwtc"] == 1) & (ukhls_master["benwtc2"] == 0),
            1,
            ukhls_master["benwtc2"],
        ),
    )

    # Remove the specified columns
    ukhls_master = ukhls_master.drop(
        columns=[
            "benuc",
            "benincsup",
            "benhouse",
            "benincjsa",
            "benincesa",
            "benwtc",
            "bencb",
            "benctc",
        ]
    )

    # Rename columns by removing the "2" suffix
    columns_to_rename = {
        "benuc2": "benuc",
        "benincsup2": "benincsup",
        "benhouse2": "benhouse",
        "benincjsa2": "benincjsa",
        "benincesa2": "benincesa",
        "benwtc2": "benwtc",
        "bencb2": "bencb",
        "benctc2": "benctc",
    }

    ukhls_master = ukhls_master.rename(columns=columns_to_rename)

    return ukhls_master
