import streamlit as st
import requests

API_URL = "http://127.0.0.1:8000/ask"  

st.title("FEC Contributions Q&A")

# Input from user
user_input = st.text_input("Enter your question:")

if st.button("Ask"):
    if user_input.strip() != "":
        with st.spinner("Generating answer..."):
            try:
                response = requests.post(API_URL, json={"query": user_input})
                if response.status_code == 200:
                    answer = response.json().get("answer", "No answer returned.")
                    st.subheader("Answer:")
                    st.write(answer)
                else:
                    st.error(f"Error: {response.status_code}")
            except requests.exceptions.RequestException as e:
                st.error(f"Request failed: {e}")
# streamlit run chatbot/streamlit_app.py 