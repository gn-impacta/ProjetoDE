"""Main entry point for the ProjetoDE pipeline."""

from __future__ import annotations

import logging

import utils
from core import configs


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
LOGGER = logging.getLogger(__name__)


def main() -> None:
    """Execute the ingestion and preparation pipeline."""

    LOGGER.info("Iniciando processo de ingestão")

    try:
        df = utils.ingestion(configs)
    except Exception as exc:
        LOGGER.exception("Erro de ingestão de dados: %s", exc)
        raise

    try:
        prepared_df = utils.preparation(df, configs)
        LOGGER.info("Fim do processo de ingestão")
        LOGGER.info("Prévia dos dados tratados:\n%s", prepared_df.head().to_string(index=False))
    except Exception as exc:
        LOGGER.exception("Erro de preparação: %s", exc)
        raise


if __name__ == "__main__":
    main()
