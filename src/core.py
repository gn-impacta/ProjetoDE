"""Configuration and schema definitions for the ProjetoDE pipeline."""

from __future__ import annotations

from pathlib import Path
from typing import Dict, List

import yaml
from pydantic import BaseModel, Field, ValidationError


PACKAGE_ROOT = Path(__file__).resolve().parents[1]
ASSETS_PATH = PACKAGE_ROOT / "assets"
CONFIG_FILE_PATH = ASSETS_PATH / "config.yml"


class ApiConfig(BaseModel):
    """API parameters used during ingestion."""

    url: str
    results: int = Field(gt=0, description="Number of records requested from the API.")
    timeout: int = Field(gt=0, description="Timeout used in the HTTP request.")


class DataConfig(BaseModel):
    """Parameters related to data transformation and storage."""

    raw_columns: List[str]
    rename_columns: Dict[str, str]
    text_columns: List[str]
    db_file: str
    table_name: str


class Config(BaseModel):
    """Master configuration object for the project."""

    api_config: ApiConfig
    data_config: DataConfig


class PreparedUserSchema(BaseModel):
    """Schema used to validate the prepared dataset before persistence."""

    gender: str
    first_name: str
    last_name: str
    email: str
    age: int = Field(ge=0)
    city: str
    country: str
    nationality: str
    registration_date: str


class MultiplePreparedUsersSchema(BaseModel):
    """Container schema for validating multiple prepared records."""

    records: List[PreparedUserSchema]


def create_and_validate_config(cfg_path: Path = CONFIG_FILE_PATH) -> Config:
    """Load the YAML configuration file and validate it with Pydantic.

    Args:
        cfg_path: Path to the YAML configuration file.

    Returns:
        A validated :class:`Config` object.

    Raises:
        OSError: If the configuration file cannot be found.
        ValidationError: If the configuration content is invalid.
    """
    if not cfg_path.exists():
        raise OSError(f"Configuration file not found at: {cfg_path}")

    with cfg_path.open("r", encoding="utf-8") as conf_file:
        parsed_config = yaml.safe_load(conf_file)

    try:
        return Config(**parsed_config)
    except ValidationError as exc:
        raise ValidationError.from_exception_data(
            title="Invalid project configuration",
            line_errors=exc.errors(),
        ) from exc


configs = create_and_validate_config()


if __name__ == "__main__":
    print(create_and_validate_config())
