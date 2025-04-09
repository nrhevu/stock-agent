import logging

from langchain import hub
from langchain.agents import AgentExecutor, create_react_agent

from core import llm
from core.parser import CustomOutputParser
from core.prompts import prompt, tools

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# --- Create Agent and Executor ---
agent_executor: AgentExecutor = None
try:
    agent = create_react_agent(llm=llm, tools=tools, prompt=prompt, output_parser=CustomOutputParser())
    agent_executor = AgentExecutor(
        agent=agent,
        tools=tools,
        verbose=True,
        handle_parsing_errors=True, # Keep this for robustness
        max_iterations=7,
        early_stopping_method="generate",
    )
    logger.info("Langchain retrieval agent created successfully.")
except Exception as e:
    logger.critical(f"Failed to create Langchain agent: {e}", exc_info=True)
    agent_executor = None