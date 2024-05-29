from nesta_ds_utils.viz.altair import formatting
from typing import List


def get_nesta_colours() -> List[str]:
    """Gets the Nesta colours.

    Returns: List of the Nesta colours: blue, yellow, light grey, green, red, purple, orange, dark blue,
    light blue, light purple, light pink, grey, white and black.
    """
    return list(formatting._load_nesta_theme()["config"]["range"]["category"])
