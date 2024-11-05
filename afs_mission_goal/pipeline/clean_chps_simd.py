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

    la_simd_clean = clean_chps(la_simd, simd=True)
    simd_sex_clean = clean_chps(simd_sex, simd=True)
    simd_ethnicity_clean = clean_chps(simd_ethnicity, simd=True)
    simd_childcare_clean = clean_chps(simd_childcare, simd=True)
    simd_lac_clean = clean_chps(simd_lac, simd=True)
    simd_eng_clean = clean_chps(simd_eng, simd=True)
    simd_primary_carer_smoking_clean = clean_chps(simd_primary_carer_smoking, simd=True)
    simd_secondhand_smoke_clean = clean_chps(simd_secondhand_smoke, simd=True)

    # Rename the SIMD columns so it is clearer
    la_simd_clean = la_simd_clean.rename(
        columns={"simd_quintile_1_most_deprived": "simd_quintile"}
    )
    simd_sex_clean = simd_sex_clean.rename(
        columns={"simd_quintile_1_most_deprived": "simd_quintile"}
    )
    simd_ethnicity_clean = simd_ethnicity_clean.rename(
        columns={"simd_quintile_1_most_deprived": "simd_quintile"}
    )
    simd_childcare_clean = simd_childcare_clean.rename(
        columns={"simd_quintile_1_most_deprived": "simd_quintile"}
    )
    simd_lac_clean = simd_lac_clean.rename(
        columns={"simd_quintile_1_most_deprived": "simd_quintile"}
    )
    simd_eng_clean = simd_eng_clean.rename(
        columns={"simd_quintile_1_most_deprived": "simd_quintile"}
    )
    simd_primary_carer_smoking_clean = simd_primary_carer_smoking_clean.rename(
        columns={"simd_quintile_1_most_deprived": "simd_quintile"}
    )
    simd_secondhand_smoke_clean = simd_secondhand_smoke_clean.rename(
        columns={"simd_quintile_1_most_deprived": "simd_quintile"}
    )

    upload_obj(
        obj=la_simd_clean,
        bucket=S3_BUCKET,
        path_to=f"scotland/data/chps_aggregated/processed/simd_la_developmental_breakdown.csv",
        kwargs_writing={"index": False},
    )

    upload_obj(
        obj=simd_ethnicity_clean,
        bucket=S3_BUCKET,
        path_to=f"scotland/data/chps_aggregated/processed/simd_ethnicity_developmental_breakdown.csv",
        kwargs_writing={"index": False},
    )

    upload_obj(
        obj=simd_sex_clean,
        bucket=S3_BUCKET,
        path_to=f"scotland/data/chps_aggregated/processed/simd_sex_developmental_breakdown.csv",
        kwargs_writing={"index": False},
    )

    upload_obj(
        obj=simd_childcare_clean,
        bucket=S3_BUCKET,
        path_to=f"scotland/data/chps_aggregated/processed/simd_childcare_developmental_breakdown.csv",
        kwargs_writing={"index": False},
    )

    upload_obj(
        obj=simd_lac_clean,
        bucket=S3_BUCKET,
        path_to=f"scotland/data/chps_aggregated/processed/simd_lac_developmental_breakdown.csv",
        kwargs_writing={"index": False},
    )

    upload_obj(
        obj=simd_eng_clean,
        bucket=S3_BUCKET,
        path_to=f"scotland/data/chps_aggregated/processed/simd_eng_developmental_breakdown.csv",
        kwargs_writing={"index": False},
    )

    upload_obj(
        obj=simd_primary_carer_smoking_clean,
        bucket=S3_BUCKET,
        path_to=f"scotland/data/chps_aggregated/processed/simd_primary_carer_smoking_developmental_breakdown.csv",
        kwargs_writing={"index": False},
    )

    upload_obj(
        obj=simd_secondhand_smoke_clean,
        bucket=S3_BUCKET,
        path_to=f"scotland/data/chps_aggregated/processed/simd_secondhand_smoke_developmental_breakdown.csv",
        kwargs_writing={"index": False},
    )
