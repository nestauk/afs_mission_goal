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

    dictionary_of_dataframes = {"ethnicity": ethnicity, "lac": lac, "eng": eng}

    for df_name, df in dictionary_of_dataframes.items():
        df_clean = clean_chps(df)
        upload_obj(
            obj=df_clean,
            bucket=S3_BUCKET,
            path_to=f"scotland/data/chps_aggregated/processed/{df_name}_developmental_breakdown.csv",
            kwargs_writing={"index": False},
        )
