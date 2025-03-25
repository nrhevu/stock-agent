import random

import streamlit as st

# Tiêu đề ứng dụng
st.title('🤖 Hỏi đáp thông tin giá cổ phiếu')

# Khởi tạo session state để lưu tin nhắn
if 'messages' not in st.session_state:
    st.session_state.messages = []

# Hiển thị lịch sử tin nhắn
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# Hàm tạo phản hồi giả lập
def generate_response(user_message):
    # Danh sách phản hồi mẫu
    responses = [
        "Tôi hiểu rồi.",
        "Thật là thú vị!",
        "Bạn có thể nói rõ hơn không?",
        "Điều đó rất thú vị.",
        "Tôi không chắc về điều đó."
    ]
    return random.choice(responses)

# Ô nhập tin nhắn
if prompt := st.chat_input("Nhập tin nhắn của bạn"):
    # Hiển thị tin nhắn của người dùng
    st.chat_message("user").markdown(prompt)
    
    # Lưu tin nhắn của người dùng
    st.session_state.messages.append({
        "role": "user", 
        "content": prompt
    })
    
    # Tạo và hiển thị phản hồi
    response = generate_response(prompt)
    with st.chat_message("assistant"):
        st.markdown(response)
    
    # Lưu tin nhắn phản hồi
    st.session_state.messages.append({
        "role": "assistant", 
        "content": response
    })