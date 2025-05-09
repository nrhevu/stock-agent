import os
import sys

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, project_root)

from utils.es import ElasticsearchUtils
from dotenv import load_dotenv

load_dotenv()

def test_connection():
    es_utils = ElasticsearchUtils()
    print(es_utils.client)

test_connection()