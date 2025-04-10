import logging
import os
import random
import sys

import streamlit as st

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, project_root)

from core.executor import agent_executor, stock_agent

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# --- Streamlit App UI ---

st.title('🤖 Hỏi đáp thông tin giá cổ phiếu')
st.caption("Powered by Langchain, OpenAI, Elasticsearch, and PostgreSQL")

# Initialize session state for messages
if 'messages' not in st.session_state:
    st.session_state.messages = []

# Display chat history
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# Chat input
if prompt := st.chat_input("Nhập yêu cầu (ví dụ: 'tin tức và giá cổ phiếu google')"):
    # Add user message to state and display
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    # Check if agent is initialized before proceeding
    if agent_executor:
        # Generate and display assistant response
        with st.chat_message("assistant"):
            message_placeholder = st.empty() # Use placeholder for streaming-like effect
            message_placeholder.markdown("Thinking...")
            try:
                # Invoke the agent
                with st.spinner("Agent is working..."): # Show spinner during execution
                    # response = agent_executor.invoke({"input": prompt})
                    # assistant_response = response.get('output', "Sorry, I encountered an issue and couldn't get a response.")
                    response = stock_agent.run(prompt, use_agent=False)
                    assistant_response = response.get('prediction', "Sorry, I encountered an issue and couldn't get a response.")

                # Display the final response
                message_placeholder.markdown(assistant_response)
                # Add assistant response to chat history
                st.session_state.messages.append({"role": "assistant", "content": assistant_response})

            except Exception as e:
                logger.error(f"Error during agent execution: {e}", exc_info=True)
                error_message = f"Sorry, an error occurred while processing your request: {e}"
                message_placeholder.error(error_message) # Display error in the chat
                st.session_state.messages.append({"role": "assistant", "content": error_message})
    else:
        # Handle case where agent failed to initialize
        st.error("The agent could not be initialized. Please check the logs or environment configuration.")
        # Add error message to chat history
        error_msg_init = "Error: Agent initialization failed. Cannot process request."
        st.session_state.messages.append({"role": "assistant", "content": error_msg_init})
        with st.chat_message("assistant"):
             st.error(error_msg_init)


# Optional: Add a button to clear chat history
if st.button("Clear Chat History"):
    st.session_state.messages = []
    st.rerun()