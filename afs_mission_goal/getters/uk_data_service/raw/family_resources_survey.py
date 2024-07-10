import pandas as pd
from afs_mission_goal.utils.load_s3 import load_from_s3

BUCKET = "afs-uk-data-service"


def get_accounts_data() -> pd.DataFrame:
    """Function to load the Family Resources Survey accounts data from the UK Data Service.
    Returns:
        pd.DataFrame: Family Resources Survey accounts data.
    """
    path = "raw/family_resources_survey/2022/accounts.dta"
    return load_from_s3(path, bucket=BUCKET)


def get_adult_data() -> pd.DataFrame:
    """Function to load the Family Resources Survey adult data from the UK Data Service.
    Returns:
        pd.DataFrame: Family Resources Survey adult data.
    """
    path = "raw/family_resources_survey/2022/adult.dta"
    return load_from_s3(path, bucket=BUCKET)


def get_assets_data() -> pd.DataFrame:
    """Function to load the Family Resources Survey assets data from the UK Data Service.
    Returns:
        pd.DataFrame: Family Resources Survey assets data.
    """
    path = "raw/family_resources_survey/2022/assets.dta"
    return load_from_s3(path, bucket=BUCKET)


def get_benefits_data() -> pd.DataFrame:
    """Function to load the Family Resources Survey benefits data from the UK Data Service.
    Returns:
        pd.DataFrame: Family Resources Survey benefits data.
    """
    path = "raw/family_resources_survey/2022/benefits.dta"
    return load_from_s3(path, bucket=BUCKET)


def get_benefit_unit_data() -> pd.DataFrame:
    """Function to load the Family Resources Survey benefit unit data from the UK Data Service.
    Returns:
        pd.DataFrame: Family Resources Survey benefit unit  data.
    """
    path = "raw/family_resources_survey/2022/benunit.dta"
    return load_from_s3(path, bucket=BUCKET)


def get_care_data() -> pd.DataFrame:
    """Function to load the Family Resources Survey care data from the UK Data Service.
    Returns:
        pd.DataFrame: Family Resources Survey care data.
    """
    path = "raw/family_resources_survey/2022/care.dta"
    return load_from_s3(path, bucket=BUCKET)


def get_child_data() -> pd.DataFrame:
    """Function to load the Family Resources Survey child data from the UK Data Service.
    Returns:
        pd.DataFrame: Family Resources Survey child data.
    """
    path = "raw/family_resources_survey/2022/child.dta"
    return load_from_s3(path, bucket=BUCKET)


def get_childcare_data() -> pd.DataFrame:
    """Function to load the Family Resources Survey childcare data from the UK Data Service.
    Returns:
        pd.DataFrame: Family Resources Survey childcare data.
    """
    path = "raw/family_resources_survey/2022/chldcare.dta"
    return load_from_s3(path, bucket=BUCKET)


def get_dictionary_data() -> pd.DataFrame:
    """Function to load the Family Resources Survey dictionary from the UK Data Service.
    Returns:
        pd.DataFrame: Family Resources Survey dictionary.
    """
    path = "raw/family_resources_survey/2022/dictnary.dta"
    return load_from_s3(path, bucket=BUCKET)


def get_endowment_data() -> pd.DataFrame:
    """Function to load the Family Resources Survey endowment data from the UK Data Service.
    Returns:
        pd.DataFrame: Family Resources Survey endowment data.
    """
    path = "raw/family_resources_survey/2022/endowmnt.dta"
    return load_from_s3(path, bucket=BUCKET)


def get_ext_child_data() -> pd.DataFrame:
    """Function to load the Family Resources Survey ext_child data from the UK Data Service.
    Returns:
        pd.DataFrame: Family Resources Survey ext_child data.
    """
    path = "raw/family_resources_survey/2022/extchild.dta"
    return load_from_s3(path, bucket=BUCKET)


def get_frs2223_data() -> pd.DataFrame:
    """Function to load the Family Resources Survey frs2223 data from the UK Data Service.
    Returns:
        pd.DataFrame: Family Resources Survey frs2223 data.
    """
    path = "raw/family_resources_survey/2022/frs2223.dta"
    return load_from_s3(path, bucket=BUCKET)


def get_gov_pay_data() -> pd.DataFrame:
    """Function to load the Family Resources Survey gov_pay data from the UK Data Service.
    Returns:
        pd.DataFrame: Family Resources Survey gov_pay data.
    """
    path = "raw/family_resources_survey/2022/govpay.dta"
    return load_from_s3(path, bucket=BUCKET)


def get_household_data() -> pd.DataFrame:
    """Function to load the Family Resources Survey household data from the UK Data Service.
    Returns:
        pd.DataFrame: Family Resources Survey household data.
    """
    path = "raw/family_resources_survey/2022/househol.dta"
    return load_from_s3(path, bucket=BUCKET)


def get_job_data() -> pd.DataFrame:
    """Function to load the Family Resources Survey job data from the UK Data Service.
    Returns:
        pd.DataFrame: Family Resources Survey job data.
    """
    path = "raw/family_resources_survey/2022/job.dta"
    return load_from_s3(path, bucket=BUCKET)


def get_maint_data() -> pd.DataFrame:
    """Function to load the Family Resources Survey maint data from the UK Data Service.
    Returns:
        pd.DataFrame: Family Resources Survey maint data.
    """
    path = "raw/family_resources_survey/2022/maint.dta"
    return load_from_s3(path, bucket=BUCKET)


def get_mort_cont_data() -> pd.DataFrame:
    """Function to load the Family Resources Survey mort_cont data from the UK Data Service.
    Returns:
        pd.DataFrame: Family Resources Survey mort_cont data.
    """
    path = "raw/family_resources_survey/2022/mortcont.dta"
    return load_from_s3(path, bucket=BUCKET)


def get_mortgage_data() -> pd.DataFrame:
    """Function to load the Family Resources Survey mortgage data from the UK Data Service.
    Returns:
        pd.DataFrame: Family Resources Survey mortgage data.
    """
    path = "raw/family_resources_survey/2022/mortgage.dta"
    return load_from_s3(path, bucket=BUCKET)


def get_odd_job_data() -> pd.DataFrame:
    """Function to load the Family Resources Survey odd_job data from the UK Data Service.
    Returns:
        pd.DataFrame: Family Resources Survey odd_job data.
    """
    path = "raw/family_resources_survey/2022/oddjob.dta"
    return load_from_s3(path, bucket=BUCKET)


def get_owner_data() -> pd.DataFrame:
    """Function to load the Family Resources Survey owner data from the UK Data Service.
    Returns:
        pd.DataFrame: Family Resources Survey owner data.
    """
    path = "raw/family_resources_survey/2022/owner.dta"
    return load_from_s3(path, bucket=BUCKET)


def get_pension_provider_data() -> pd.DataFrame:
    """Function to load the Family Resources Survey pension provider data from the UK Data Service.
    Returns:
        pd.DataFrame: Family Resources Survey pension provider data.
    """
    path = "raw/family_resources_survey/2022/penprov.dta"
    return load_from_s3(path, bucket=BUCKET)


def get_pension_data() -> pd.DataFrame:
    """Function to load the Family Resources Survey pension data from the UK Data Service.
    Returns:
        pd.DataFrame: Family Resources Survey pension data.
    """
    path = "raw/family_resources_survey/2022/pension.dta"
    return load_from_s3(path, bucket=BUCKET)


def get_rent_cont_data() -> pd.DataFrame:
    """Function to load the Family Resources Survey rent cont data from the UK Data Service.
    Returns:
        pd.DataFrame: Family Resources Survey rent cont data.
    """
    path = "raw/family_resources_survey/2022/rentcont.dta"
    return load_from_s3(path, bucket=BUCKET)


def get_renter_data() -> pd.DataFrame:
    """Function to load the Family Resources Survey rent data from the UK Data Service.
    Returns:
        pd.DataFrame: Family Resources Survey rent data.
    """
    path = "raw/family_resources_survey/2022/renter.dta"
    return load_from_s3(path, bucket=BUCKET)


def get_tables_data() -> pd.DataFrame:
    """Function to load the Family Resources Survey tables from the UK Data Service.
    Returns:
        pd.DataFrame: Family Resources Survey tables.
    """
    path = "raw/family_resources_survey/2022/tables.dta"
    return load_from_s3(path, bucket=BUCKET)
