import logging
from datetime import datetime, timezone

from transformers import pipeline

TRANSLATION_MODEL = "Helsinki-NLP/opus-mt-vi-en"  # Model for Vietnamese to English
BATCH_SIZE = 100  # Process and index in batches

# --- Setup Logging ---
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# --- Initialize Translation Pipeline ---
try:
    translator = pipeline("translation", model=TRANSLATION_MODEL)
    logger.info(f"Loaded translation model: {TRANSLATION_MODEL}")
except Exception as e:
    logger.error(
        f"Failed to load translation model '{TRANSLATION_MODEL}': {e}", exc_info=True
    )
    translator = None  # Allow script to continue but skip translation


def translate_text(text: str | None) -> str | None:
    """Translates text using the loaded pipeline."""
    if not text or not translator:
        return None
    try:
        # Split long text? Some models have limits. Let's assume it handles reasonably sized content.
        translated_list = translator(
            text, max_length=2048
        )  # Adjust max_length if needed
        if (
            translated_list
            and isinstance(translated_list, list)
            and "translation_text" in translated_list[0]
        ):
            return translated_list[0]["translation_text"]
        else:
            logger.warning(
                f"Translation did not return expected format for text starting with: {text[:50]}..."
            )
            return None
    except Exception as e:
        logger.error(
            f"Translation failed for text starting with: {text[:50]}... Error: {e}",
            exc_info=True,
        )
        return None


def parse_date(date_str: str) -> str | None:
    """Converts DD/MM/YYYY to ISO 8601 format (UTC)."""
    if not date_str:
        return None
    try:
        # Parse as naive datetime first
        dt_naive = datetime.strptime(date_str, "%d/%m/%Y")
        # Assume the date is in UTC or your local timezone, then convert to UTC
        # If dates are local, adjust accordingly e.g., using pytz
        dt_utc = dt_naive.replace(tzinfo=timezone.utc)
        return dt_utc.isoformat()
    except ValueError:
        logger.warning(f"Could not parse date: '{date_str}'. Skipping date field.")
        return None
    except Exception as e:
        logger.error(f"Unexpected error parsing date '{date_str}': {e}", exc_info=True)
        return None
