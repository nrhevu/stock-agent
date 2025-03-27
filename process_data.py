import json
import logging
from glob import glob
from typing import Any, Dict, List

from tqdm import tqdm

from nlp.translate import parse_date, translate_text
from utils.es import ElasticsearchUtils

# --- Setup Logging ---
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def translate_and_index_to_elk(index_name: str, json_path: str, company: str) -> None:
    """
    Reads articles from a JSON file, translates Vietnamese text to English,
    processes dates, and indexes documents into Elasticsearch.
    
    Args:
        index_name: Name of the Elasticsearch index to store documents.
    """
    # Load articles from JSON file
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            articles = json.load(f)
        logger.info(f"Loaded {len(articles)} articles from {json_path}")
    except FileNotFoundError:
        logger.error(f"JSON file not found: {json_path}")
        return
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON format: {e}")
        return
    except Exception as e:
        logger.error(f"Error loading articles: {e}")
        return

    translated_docs: List[Dict[str, Any]] = []
    
    for article in tqdm(articles):
        # Extract fields
        title_vi = article.get('title')
        publish_date_str = article.get('publish_date')
        content_vi = article.get('content', '')

        # Validate required fields
        if not title_vi or not publish_date_str:
            logger.warning("Skipping article missing title/publish_date")
            continue

        # Translate title
        title_en = translate_text(title_vi)
        if not title_en:
            logger.warning(f"Translation failed for title: {title_vi[:50]}...")

        # Translate content if present
        content_en = None
        if content_vi.strip():
            content_en = translate_text(content_vi)
            if not content_en:
                logger.warning(f"Content translation failed for: {title_vi[:50]}...")

        # Parse date
        publish_date = parse_date(publish_date_str)
        if not publish_date:
            logger.warning(f"Invalid date: {publish_date_str}")
            continue

        # Build document
        doc = {
            'title_vi': title_vi,
            'title_en': title_en,
            'content_vi': content_vi if content_vi.strip() else None,
            'content_en': content_en,
            'publish_date': publish_date,
            'company': company,
            # Include other fields from original data
        }
        # Remove None values
        doc = {k: v for k, v in doc.items() if v is not None}
        translated_docs.append(doc)

    if not translated_docs:
        logger.warning("No valid documents to index")
        return

    # Initialize Elasticsearch connection
    try:
        es_utils = ElasticsearchUtils()  # Uses env vars for connection
    except Exception as e:
        logger.error(f"Elasticsearch connection failed: {e}")
        return

    # Bulk index documents
    success, failed = es_utils.bulk_index(
        documents=translated_docs,
        index_name=index_name,
        doc_id_field=None  # Or specify a unique ID field if available
    )
    logger.info(f"Indexing complete. Success: {success}, Failed: {failed}")


if __name__ == "__main__":
    for json_path in glob("data/news_data/*.json"):
        # Extract company name from filename
        company = json_path.split("/")[-1].split(".")[0].replace("cleaned_", "")
        logger.info(f"Processing {company} articles from {json_path}")
        
        # Translate and index
        translate_and_index_to_elk("news_data", json_path, company)
        logger.info(f"Finished processing {company} articles.")