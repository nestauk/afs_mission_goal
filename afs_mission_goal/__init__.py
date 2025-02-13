"""afs_mission_goal."""

import logging
import logging.config
from pathlib import Path
from typing import Optional

import yaml


def get_yaml_config(file_path: Path) -> Optional[dict]:
    """Fetch yaml config and return as dict if it exists."""
    if file_path.exists():
        with open(file_path, "rt") as f:
            return yaml.load(f.read(), Loader=yaml.FullLoader)


# Define project base directory
PROJECT_DIR = Path(__file__).resolve().parents[1]

## Define the two S3 buckets, one for the UK data service data and one for the more general mission goal data.
DS_BUCKET = "afs-uk-data-service"
S3_BUCKET = "afs-mission-goal"

# Define log output locations
info_out = str(PROJECT_DIR / "info.log")
error_out = str(PROJECT_DIR / "errors.log")

# Read log config file
_log_config_path = Path(__file__).parent.resolve() / "config/logging.yaml"
_logging_config = get_yaml_config(_log_config_path)
if _logging_config:
    logging.config.dictConfig(_logging_config)

# Define module logger
logger = logging.getLogger(__name__)

# base/global config
_base_config_path = Path(__file__).parent.resolve() / "config/base.yaml"
config = get_yaml_config(_base_config_path)

# Scottish health review conversion config
_health_review_config_path = (
    Path(__file__).parent.resolve() / "config/chps_review_conversion.yaml"
)
health_review_config = get_yaml_config(_health_review_config_path)

# USoc configs
_usoc_config_01 = Path(__file__).parent.resolve() / "config/usoc/01_spine.yaml"
usoc_config_01 = get_yaml_config(_usoc_config_01)
_usoc_config_03 = Path(__file__).parent.resolve() / "config/usoc/03_family.yaml"
usoc_config_03 = get_yaml_config(_usoc_config_03)
