import pandas as pd
import logging
import boto3
from botocore.exceptions import ClientError
import tempfile
from fnmatch import fnmatch
import requests
import json
from typing import Any

BUCKET = "afs-mission-indicators"

logger = logging.getLogger(__name__)

s3 = boto3.client("s3")


def s3_exists(path: str, **kwargs) -> bool:
    """Checks whether 'data/{path}' exists in 'BUCKET' in S3
    Args:
        path (str):  Path to file after 'data/' in 'BUCKET'

    Returns:
        bool: True or False if the file exists in S3.
    """
    bucket = kwargs.get("bucket", BUCKET)
    path = "data/" + path
    try:
        s3.head_object(Bucket=bucket, Key=path)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        else:
            print(e.response)
            raise e


def data_from_s3(path: str, filename: str, **kwargs) -> Any:
    """Function to download 'data/{path}' from 'BUCKET' in S3. If you need to download a csv or a json file from S3, please use the nesta_ds_utils function loading_saving.download_obj instead.

    Args:
        path (str): Path to file after 'data/' in 'BUCKET'
        filename (str): Path to file after in local machine

    Returns:
        Any: Returns data from 'data/{path}' from 'BUCKET' in S3.
    """
    bucket = kwargs.get("bucket", BUCKET)
    if s3_exists(path, bucket=bucket) == True:
        object_path = "data/" + path

        return s3.download_file(bucket, object_path, filename)
    else:
        logging.warning(f"s3://{bucket}/data/{path} does not exist.")


def load_from_s3(path: str, **kwargs) -> Any:
    """Loads 'data/{path}' from 'BUCKET' in S3. If you wish to load a json or csv, please use the nesta_ds_utils package (pip install nesta_ds_utils[s3] @ git+https://github.com/nestauk/nesta_ds_utils.git) and use loading_saving.download_obj.

    Args:
        path (str): Path to file after 'data/' in 'BUCKET' in S3
            E.g. `path="aux/Ward_Boundaries.geojson"` will
            fetch the s3 key
            `"s3://{BUCKET}/data/aux/Ward_Boundaries.geojson"`

    Returns:
        Any: Data from 'data/{path}' in S3 'BUCKET'.
    """
    bucket = kwargs.get("bucket", BUCKET)
    header = kwargs.get("header", 0)
    index_col = kwargs.get("index_col", None)
    if fnmatch(path, "*.xlsm") or fnmatch(path, "*.xlsx"):
        temp = tempfile.NamedTemporaryFile()
        data_from_s3(path, temp.name, bucket)
        sheet_name = kwargs.get("sheet_name", "Sheet1")
        usecols = kwargs.get("usecols", None)
        skiprows = kwargs.get("skiprows", None)
        df = pd.read_excel(
            temp.name,
            sheet_name=sheet_name,
            header=header,
            usecols=usecols,
            skiprows=skiprows,
            index_col=index_col,
        )
        temp.close()
        return df
    elif fnmatch(path, "*.dta"):
        temp = tempfile.NamedTemporaryFile()
        data_from_s3(path, temp.name, bucket=bucket)
        df = pd.read_stata(temp.name, convert_categoricals=False)
        temp.close()
        return df
    elif fnmatch(path, "*.geojson"):
        object_path = "data/" + path
        geojson = s3.get_object(Bucket=BUCKET, Key=object_path)
        geojson = geojson["Body"].read().decode("utf-8")
        return json.loads(geojson)
    else:
        logger.exception(
            'Function not supported for file type other than ".xlsx", ".geojson", ".dta" and ".xlsm"'
        )
        raise ValueError(
            'Function not supported for file type other than ".xlsx", ".geojson", ".dta" and ".xlsm"'
        )
