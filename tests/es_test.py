import os
import sys

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, project_root)

from utils.es import ElasticsearchUtils


def test_connection():
    es_utils = ElasticsearchUtils()
    print(es_utils.client)
