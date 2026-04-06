"""Utility functions for ingestion, validation and preparation steps."""

from __future__ import annotations

import logging
import re
import sqlite3
import unicodedata
from typing import Any, Dict, List

import pandas as pd
import requests

from core import ASSETS_PATH, MultiplePreparedUsersSchema


LOGGER = logging.getLogger(__name__)


BASE_FALLBACK_RESULTS: List[Dict[str, Any]] = [
    {
        "gender": "female",
        "name": {"first": "Ana", "last": "Silva"},
        "email": "ana.silva@example.com",
        "dob": {"age": 31},
        "location": {"city": "São Paulo", "country": "Brazil"},
        "nat": "BR",
        "registered": {"date": "2022-05-10T09:14:12.000Z"},
    },
    {
        "gender": "male",
        "name": {"first": "Carlos", "last": "Souza"},
        "email": "carlos.souza@example.com",
        "dob": {"age": 28},
        "location": {"city": "Rio de Janeiro", "country": "Brazil"},
        "nat": "BR",
        "registered": {"date": "2021-11-01T14:45:00.000Z"},
    },
    {
        "gender": "female",
        "name": {"first": "María", "last": "Pérez"},
        "email": "maria.perez@example.com",
        "dob": {"age": 35},
        "location": {"city": "Bogotá", "country": "Colombia"},
        "nat": "CO",
        "registered": {"date": "2020-08-20T18:30:55.000Z"},
    },
    {
        "gender": "male",
        "name": {"first": "João", "last": "Mendes"},
        "email": "joao.mendes@example.com",
        "dob": {"age": 42},
        "location": {"city": "Curitiba", "country": "Brazil"},
        "nat": "BR",
        "registered": {"date": "2019-01-15T07:02:10.000Z"},
    },
]


def _fallback_payload(total_records: int) -> Dict[str, List[Dict[str, Any]]]:
    """Return a local payload used when the external API is unavailable.

    Args:
        total_records: Desired number of records for the fallback payload.

    Returns:
        A dictionary with the same high-level structure returned by randomuser.me.
    """

    results: List[Dict[str, Any]] = []
    for index in range(total_records):
        template = BASE_FALLBACK_RESULTS[index % len(BASE_FALLBACK_RESULTS)].copy()
        template["email"] = template["email"].replace("@", f".{index + 1}@")
        results.append(template)

    return {"results": results}


def _remove_special_characters(value: Any) -> Any:
    """Normalize text values by removing accents and special characters.

    Args:
        value: Any value that may contain textual content.

    Returns:
        The normalized value. Non-string values are returned unchanged.
    """

    if not isinstance(value, str):
        return value

    normalized = unicodedata.normalize("NFKD", value)
    ascii_text = normalized.encode("ascii", "ignore").decode("utf-8")
    clean_text = re.sub(r"[^A-Za-z0-9@._\-\s]", "", ascii_text)
    return re.sub(r"\s+", " ", clean_text).strip()


def ingestion(configs: Any) -> pd.DataFrame:
    """Ingest user records from the Random User API.

    The function attempts to consume data from ``randomuser.me`` using the
    parameters informed in the project configuration. If the HTTP request is not
    available, a local fallback payload is used to keep the pipeline executable.

    Args:
        configs: Validated project configuration object.

    Returns:
        A pandas DataFrame with the raw ingested data.
    """

    params = {"results": configs.api_config.results}

    try:
        response = requests.get(
            configs.api_config.url,
            params=params,
            timeout=configs.api_config.timeout,
        )
        response.raise_for_status()
        payload = response.json()
        LOGGER.info("Dados ingeridos com sucesso da API externa.")
    except requests.RequestException as exc:
        payload = _fallback_payload(configs.api_config.results)
        LOGGER.warning(
            "Falha ao acessar a API externa (%s). Utilizando payload local de fallback.",
            exc,
        )

    df = pd.json_normalize(payload["results"])
    df = df.loc[:, configs.data_config.raw_columns]
    LOGGER.info("Total de registros ingeridos: %s", len(df))
    return df


def validation_inputs(df: pd.DataFrame, configs: Any) -> bool:
    """Validate the prepared dataset before saving it to SQLite.

    Args:
        df: Prepared DataFrame to be validated.
        configs: Validated project configuration object.

    Returns:
        ``True`` when the dataset is valid.

    Raises:
        ValueError: If the DataFrame is empty or required columns are missing.
        ValidationError: If one or more records do not satisfy the schema.
    """

    expected_columns = list(configs.data_config.rename_columns.values())

    if df.empty:
        raise ValueError("O DataFrame está vazio. Não há dados para validar.")

    missing_columns = [column for column in expected_columns if column not in df.columns]
    if missing_columns:
        raise ValueError(f"Colunas obrigatórias ausentes: {missing_columns}")

    records = df[expected_columns].to_dict(orient="records")
    MultiplePreparedUsersSchema(records=records)

    invalid_emails = df[~df["email"].astype(str).str.contains(r"@", regex=True, na=False)]
    if not invalid_emails.empty:
        raise ValueError("Foram encontrados e-mails inválidos no conjunto de dados.")

    LOGGER.info("Dados corretos")
    return True


def preparation(df: pd.DataFrame, configs: Any) -> pd.DataFrame:
    """Prepare the raw dataset and persist it in a SQLite database.

    Steps executed:
        1. Rename columns.
        2. Adjust data types.
        3. Remove accents and special characters from configured text columns.
        4. Validate the final dataset.
        5. Save the data into SQLite inside the ``assets`` directory.

    Args:
        df: Raw DataFrame returned by :func:`ingestion`.
        configs: Validated project configuration object.

    Returns:
        The prepared DataFrame.
    """

    prepared_df = df.copy()
    prepared_df = prepared_df.rename(columns=configs.data_config.rename_columns)

    prepared_df["age"] = prepared_df["age"].astype(int)
    prepared_df["registration_date"] = pd.to_datetime(
        prepared_df["registration_date"], errors="coerce"
    ).dt.strftime("%Y-%m-%d")

    for column in configs.data_config.text_columns:
        prepared_df[column] = prepared_df[column].apply(_remove_special_characters)

    validation_inputs(prepared_df, configs)

    ASSETS_PATH.mkdir(parents=True, exist_ok=True)
    db_path = ASSETS_PATH / configs.data_config.db_file

    with sqlite3.connect(db_path) as connection:
        prepared_df.to_sql(
            configs.data_config.table_name,
            connection,
            if_exists="replace",
            index=False,
        )

    LOGGER.info(
        "Dados preparados e salvos com sucesso em %s (tabela: %s).",
        db_path,
        configs.data_config.table_name,
    )
    return prepared_df
