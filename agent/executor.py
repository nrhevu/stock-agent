import logging

from langchain import hub
from langchain.agents import AgentExecutor, create_react_agent

from agent import llm
from agent.prompts import prompt, tools

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# --- Create Agent and Executor ---
agent_executor: AgentExecutor = None
try:
    agent = create_react_agent(llm=llm, tools=tools, prompt=prompt)
    agent_executor = AgentExecutor(
        agent=agent,
        tools=tools,
        verbose=True,
        handle_parsing_errors=True, # Keep this for robustness
        max_iterations=5 # Reduced iterations for simpler flow
    )
    logger.info("Langchain retrieval agent created successfully.")
except Exception as e:
    logger.critical(f"Failed to create Langchain agent: {e}", exc_info=True)
    agent_executor = None