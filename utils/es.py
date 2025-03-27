import logging
import os
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv
from elasticsearch import (ConnectionError, Elasticsearch, NotFoundError,
                           RequestError, helpers)

# --- Setup Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ElasticsearchUtils:
    """
    A utility class for interacting with Elasticsearch.

    Handles connection, indexing single/bulk documents, retrieving by ID,
    and searching documents.
    """
    def __init__(
        self,
        # default_index: str,
        hosts: Optional[List[str]] = None,
        api_key: Optional[Tuple[str, str]] = None,
        basic_auth: Optional[Tuple[str, str]] = None,
        cloud_id: Optional[str] = None,
        timeout: int = 30,
        max_retries: int = 3,
        retry_on_timeout: bool = True
    ):
        """
        Initializes the Elasticsearch client.

        Connection parameters are taken from arguments if provided,
        otherwise falls back to environment variables.

        Args:
            default_index: The default Elasticsearch index name to use for operations.
            hosts: List of Elasticsearch node URLs (e.g., ["http://localhost:9200"]).
                   Falls back to ELASTICSEARCH_HOST env var.
            api_key: API key tuple (id, api_key) for authentication.
                     Falls back to ELASTICSEARCH_API_KEY env var.
            basic_auth: Basic auth tuple (username, password).
                        Falls back to ELASTICSEARCH_USER/PASSWORD env vars.
            cloud_id: Cloud ID for Elastic Cloud authentication.
                      Falls back to ELASTICSEARCH_CLOUD_ID env var.
            timeout: Connection timeout in seconds.
            max_retries: Maximum number of retries for failed connections.
            retry_on_timeout: Whether to retry on connection timeouts.

        Raises:
            ConnectionError: If the connection to Elasticsearch cannot be established.
        """
        load_dotenv() # Load .env file if present

        # Determine connection parameters
        _hosts = hosts or [os.getenv("ELASTICSEARCH_HOST", "http://localhost:9200")]
        _api_key = api_key or os.getenv("ELASTICSEARCH_API_KEY")
        _basic_auth = basic_auth or (
            os.getenv("ELASTICSEARCH_USER"),
            os.getenv("ELASTICSEARCH_PASSWORD")
        )
        _cloud_id = cloud_id or os.getenv("ELASTICSEARCH_CLOUD_ID")

        connection_args = {
            "timeout": timeout,
            "max_retries": max_retries,
            "retry_on_timeout": retry_on_timeout
        }

        if _cloud_id:
             connection_args["cloud_id"] = _cloud_id
        else:
             connection_args["hosts"] = _hosts

        if _api_key:
            connection_args["api_key"] = _api_key.split(':') if isinstance(_api_key, str) else _api_key
        elif _basic_auth[0] and _basic_auth[1]:
            connection_args["basic_auth"] = _basic_auth
        # Add other auth methods (bearer, etc.) if needed

        try:
            self.client = Elasticsearch(**connection_args)
            # Test connection
            if not self.client.ping():
                raise ConnectionError("Elasticsearch connection check failed.")
            logger.info(f"Successfully connected to Elasticsearch: {_hosts or _cloud_id}")
        except ConnectionError as ce:
            logger.error(f"Failed to connect to Elasticsearch ({_hosts or _cloud_id}): {ce}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"An unexpected error occurred during Elasticsearch connection: {e}", exc_info=True)
            raise

        # self.default_index = default_index


    def _get_index(self, index_name: Optional[str]) -> str:
        """Returns the index name to use, defaulting to self.default_index."""
        resolved_index = index_name
        if not resolved_index:
             raise ValueError("Index name must be provided either during initialization or method call.")
        return resolved_index

    def index_document(
        self,
        document: Dict[str, Any],
        index_name: Optional[str] = None,
        doc_id: Optional[str] = None,
        refresh: Optional[str] = None # e.g., "wait_for", True, False
    ) -> Optional[Dict[str, Any]]:
        """
        Indexes a single document into Elasticsearch.

        Args:
            document: The dictionary representing the document data.
            index_name: The index to push data to. Defaults to self.default_index.
            doc_id: Optional specific ID for the document. If None, ES generates one.
            refresh: Controls if the index should be refreshed after the operation.

        Returns:
            The response dictionary from Elasticsearch upon success, None on failure.
        """
        target_index = self._get_index(index_name)
        try:
            response = self.client.index(
                index=target_index,
                document=document,
                id=doc_id,
                refresh=refresh
            )
            logger.debug(f"Indexed document {response.get('_id')} into index '{target_index}'")
            return response
        except RequestError as re:
             logger.error(f"Error indexing document into '{target_index}': Invalid request. {re.info}", exc_info=True)
             # Log document details carefully if needed (potential PII)
             # logger.error(f"Document content (partial): {str(document)[:200]}")
        except ConnectionError as ce:
             logger.error(f"Connection error indexing document into '{target_index}': {ce}", exc_info=True)
        except Exception as e:
            logger.error(f"Unexpected error indexing document into '{target_index}': {e}", exc_info=True)
        return None

    def bulk_index(
        self,
        documents: List[Dict[str, Any]],
        index_name: Optional[str] = None,
        doc_id_field: Optional[str] = None # Optional field in doc dict to use as _id
    ) -> Tuple[int, int]:
        """
        Indexes multiple documents in bulk for efficiency.

        Args:
            documents: A list of dictionaries, where each dictionary is a document.
            index_name: The index to push data to. Defaults to self.default_index.
            doc_id_field: If specified, the value of this field in each document
                          will be used as the document's _id.

        Returns:
            A tuple: (number of successfully indexed documents, number of failed documents).
        """
        target_index = self._get_index(index_name)
        actions = []
        for doc in documents:
            action: Dict[str, Any] = {
                "_index": target_index,
                "_source": doc
            }
            if doc_id_field and doc_id_field in doc:
                action["_id"] = doc[doc_id_field]
            actions.append(action)

        if not actions:
            logger.info("Bulk index called with no documents.")
            return 0, 0

        success_count = 0
        failed_count = 0
        errors = []

        try:
            success_count, errors = helpers.bulk(
                self.client,
                actions,
                stats_only=False, # Set to True if you only need counts, False for error details
                raise_on_error=False, # Don't stop on first error
                raise_on_exception=False # Don't stop on connection errors during bulk
            )
            failed_count = len(errors) if isinstance(errors, list) else 0 # errors is int if stats_only=True

            if failed_count > 0:
                logger.error(f"Bulk indexing to '{target_index}' completed with {failed_count} errors.")
                # Log first few errors for inspection
                for idx, error_info in enumerate(errors[:5]):
                     try:
                         # Attempt to extract more specific error details
                         err_type = error_info.get('index', {}).get('error', {}).get('type', 'Unknown Type')
                         err_reason = error_info.get('index', {}).get('error', {}).get('reason', 'Unknown Reason')
                         err_id = error_info.get('index', {}).get('_id', 'N/A')
                         logger.error(f"  Bulk Error {idx+1}: ID={err_id}, Type={err_type}, Reason={err_reason}")
                     except Exception: # Fallback for unexpected error structure
                         logger.error(f"  Bulk Error {idx+1}: {error_info}")
            else:
                logger.info(f"Successfully bulk indexed {success_count} documents into '{target_index}'.")

        except helpers.BulkIndexError as bie:
             # This might be raised if raise_on_error=True, but capture anyway
             logger.error(f"Bulk indexing error occurred for index '{target_index}': {bie}", exc_info=True)
             # Estimate failures - might be inaccurate if some succeeded before error
             failed_count = len(documents) - success_count # Crude estimate
        except ConnectionError as ce:
            logger.error(f"Connection error during bulk indexing to '{target_index}': {ce}", exc_info=True)
            failed_count = len(documents) # Assume all failed on connection error
        except Exception as e:
            logger.error(f"Unexpected error during bulk indexing to '{target_index}': {e}", exc_info=True)
            failed_count = len(documents) - success_count # Crude estimate

        return success_count, failed_count


    def get_document_by_id(
        self,
        doc_id: str,
        index_name: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Retrieves a single document by its _id.

        Args:
            doc_id: The ID of the document to retrieve.
            index_name: The index to retrieve from. Defaults to self.default_index.

        Returns:
            The document's source dictionary (_source) if found, None otherwise.
        """
        target_index = self._get_index(index_name)
        try:
            response = self.client.get(index=target_index, id=doc_id)
            logger.debug(f"Retrieved document {doc_id} from index '{target_index}'")
            return response.get("_source")
        except NotFoundError:
            logger.warning(f"Document with ID '{doc_id}' not found in index '{target_index}'.")
        except ConnectionError as ce:
             logger.error(f"Connection error retrieving document '{doc_id}' from '{target_index}': {ce}", exc_info=True)
        except Exception as e:
            logger.error(f"Unexpected error retrieving document '{doc_id}' from '{target_index}': {e}", exc_info=True)
        return None

    def search(
        self,
        query_body: Dict[str, Any],
        index_name: Optional[str] = None,
        size: int = 10,
        scroll: Optional[str] = None # e.g., '5m' to keep scroll context open for 5 mins
    ) -> List[Dict[str, Any]]:
        """
        Performs a search query against Elasticsearch.

        Args:
            query_body: The Elasticsearch Query DSL body (dictionary).
            index_name: The index or indices (comma-separated) to search. Defaults to self.default_index.
            size: The maximum number of hits to return per page (or total if not scrolling).
            scroll: If set (e.g., '1m'), initiates a scroll search to retrieve large result sets.
                    Caller needs to handle subsequent scroll calls (not implemented in this basic method).

        Returns:
            A list of hit dictionaries (containing _id, _index, _score, _source, etc.),
            or an empty list if no results or an error occurred.
        """
        target_index = self._get_index(index_name)
        results = []
        try:
            response = self.client.search(
                index=target_index,
                body=query_body,
                size=size,
                scroll=scroll
            )
            results = response.get("hits", {}).get("hits", [])
            total_hits = response.get("hits", {}).get("total", {}).get("value", 0)
            relation = response.get("hits", {}).get("total", {}).get("relation", "eq")
            total_display = f"{total_hits}{'+' if relation == 'gte' else ''}"

            logger.info(f"Search on '{target_index}' found {total_display} total hits. Returning {len(results)}.")
            # Add scroll ID info if scrolling was initiated
            if scroll and "_scroll_id" in response:
                logger.info(f"Scroll ID for further results: {response['_scroll_id']}")
                # You might want to return the scroll_id along with the results
                # return results, response['_scroll_id'] # Example modification

        except RequestError as re:
             logger.error(f"Invalid search request for index '{target_index}': {re.info}", exc_info=True)
             logger.error(f"Query Body: {query_body}")
        except ConnectionError as ce:
             logger.error(f"Connection error during search on '{target_index}': {ce}", exc_info=True)
        except Exception as e:
            logger.error(f"Unexpected error during search on '{target_index}': {e}", exc_info=True)

        return results # Returns list of hits (each is a dict)
