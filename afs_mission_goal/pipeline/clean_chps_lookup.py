import pandas as pd
from nesta_ds_utils.loading_saving.S3 import upload_obj

from afs_mission_goal.utils.preprocessing import (
    preprocess_strings,
    remove_nan_rows_and_columns,
)
from afs_mission_goal.getters.chps.raw.get_chps_lookup import get_chps_lookup

from afs_mission_goal import S3_BUCKET

if __name__ == "__main__":
    chps_lookup = get_chps_lookup()

    # Remove the extra rows at the bottom of the sheet which contain conversions such as "13m" to "13-15 months". Keep only the rows related to the council areas.
    chps_lookup_clean = remove_nan_rows_and_columns(chps_lookup)[:32]

    # No preprocessing of the columns required for this dataset

    upload_obj(
        obj=chps_lookup_clean,
        bucket=S3_BUCKET,
        path_to=f"scotland/data/chps_aggregated/processed/chps_lookup.csv",
        kwargs_writing={"index": False},
    )
