import pandas as pd
from nesta_ds_utils.loading_saving.S3 import upload_obj

from afs_mission_goal.pipeline.cleaning_functions_chps import clean_chps
from afs_mission_goal.getters.chps.raw.get_chps_counts_of_concerns import (
    get_chps_data_simd_counts_of_concerns,
    get_chps_data_sex_counts_of_concerns,
    get_chps_data_ethnicity_counts_of_concerns,
    get_chps_data_eng_counts_concerns,
)

from afs_mission_goal import S3_BUCKET

if __name__ == "__main__":
    simd = get_chps_data_simd_counts_of_concerns()
    sex = get_chps_data_sex_counts_of_concerns()
    ethnicity = get_chps_data_ethnicity_counts_of_concerns()
    eng = get_chps_data_eng_counts_concerns()

    dictionary_of_dataframes = {
        "simd": simd,
        "sex": sex,
        "ethnicity": ethnicity,
        "eng": eng,
    }

    for df_name, df in dictionary_of_dataframes.items():
        df_clean = clean_chps(df)

        if df_name == "simd":
            # Rename one of the SIMD columns so it is clearer
            df_clean = df_clean.rename(
                columns={"simd_quintile_1_most_deprived": "simd_quintile"}
            )
            # Remove the last two columns of the SIMD dataframe
            df_clean = df_clean.iloc[:, :-2]

        upload_obj(
            obj=df_clean,
            bucket=S3_BUCKET,
            path_to=f"scotland/data/chps_aggregated/processed/{df_name}_counts_of_concerns.csv",
            kwargs_writing={"index": False},
        )
