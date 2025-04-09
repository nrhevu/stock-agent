import logging

from langchain.schema import StrOutputParser

from core import llm
from core.prompts import company_detection_prompt, analysis_prompt

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

company_detection_chain = (
    company_detection_prompt
    | llm
    | StrOutputParser()
)

analysis_chain = (
    analysis_prompt
    | llm
    | StrOutputParser()
)



