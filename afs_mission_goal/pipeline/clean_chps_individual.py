import pandas as pd
from nesta_ds_utils.loading_saving.S3 import upload_obj

from afs_mission_goal.pipeline.cleaning_functions_chps import clean_chps
from afs_mission_goal.getters.chps.raw.get_chps_individual_breakdowns import (
    get_chps_data_ethnicity,
    get_chps_data_lac,
    get_chps_data_eng,
)

from afs_mission_goal import S3_BUCKET

if __name__ == "__main__":
    ethnicity = get_chps_data_ethnicity()
    lac = get_chps_data_lac()
    eng = get_chps_data_eng()

    ethnicity_clean = clean_chps(ethnicity)
    lac_clean = clean_chps(lac)
    eng_clean = clean_chps(eng)

    upload_obj(
        obj=ethnicity_clean,
        bucket=S3_BUCKET,
        path_to=f"scotland/data/chps_aggregated/processed/ethnicity_developmental_breakdown.csv",
        kwargs_writing={"index": False},
    )

    upload_obj(
        obj=lac_clean,
        bucket=S3_BUCKET,
        path_to=f"scotland/data/chps_aggregated/processed/lac_developmental_breakdown.csv",
        kwargs_writing={"index": False},
    )

    upload_obj(
        obj=eng_clean,
        bucket=S3_BUCKET,
        path_to=f"scotland/data/chps_aggregated/processed/eng_developmental_breakdown.csv",
        kwargs_writing={"index": False},
    )
