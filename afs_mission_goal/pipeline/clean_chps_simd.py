import pandas as pd
from nesta_ds_utils.loading_saving.S3 import upload_obj

from afs_mission_goal.pipeline.cleaning_functions_chps import clean_chps
from afs_mission_goal.getters.chps.raw.get_chps_simd_breakdowns import (
    get_chps_data_la_simd,
    get_chps_data_simd_sex,
    get_chps_data_simd_ethnicity,
    get_chps_data_simd_lac,
    get_chps_data_simd_eng,
    get_chps_data_simd_primary_carer_smoking,
    get_chps_data_simd_secondhand_smoke,
    get_chps_data_simd_childcare,
)

from afs_mission_goal import S3_BUCKET

if __name__ == "__main__":
    la_simd = get_chps_data_la_simd()
    simd_sex = get_chps_data_simd_sex()
    simd_ethnicity = get_chps_data_simd_ethnicity()
    simd_lac = get_chps_data_simd_lac()
    simd_eng = get_chps_data_simd_eng()
    simd_childcare = get_chps_data_simd_childcare()
    simd_primary_carer_smoking = get_chps_data_simd_primary_carer_smoking()
    simd_secondhand_smoke = get_chps_data_simd_secondhand_smoke()

    # Rename the SIMD columns so it is clearer
    dictionary_of_dataframes = {
        "simd_la": la_simd,
        "simd_ethnicity": simd_ethnicity,
        "simd_sex": simd_sex,
        "simd_childcare": simd_childcare,
        "simd_lac": simd_lac,
        "simd_eng": simd_eng,
        "simd_primary_carer_smoking": simd_primary_carer_smoking,
        "simd_secondhand_smoke": simd_secondhand_smoke,
    }

    for df_name, df in dictionary_of_dataframes.items():
        df_clean = clean_chps(df, simd=True)

        df_clean = df_clean.rename(
            columns={"simd_quintile_1_most_deprived": "simd_quintile"}
        )
        upload_obj(
            obj=df_clean,
            bucket=S3_BUCKET,
            path_to=f"scotland/data/chps_aggregated/processed/{df_name}_developmental_breakdown.csv",
            kwargs_writing={"index": False},
        )
