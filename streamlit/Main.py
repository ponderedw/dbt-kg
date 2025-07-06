import streamlit as st
import os
import requests
import datetime
import dateutil.relativedelta


if "http_session" not in st.session_state:
    st.session_state.http_session = requests.Session()
    # call /chat/new once when the session is first created
    response = st.session_state.http_session.post(
        "http://fastapi:8080/chat/new",
        headers={"x-access-token":
                 os.environ.get('FAST_API_ACCESS_SECRET_TOKEN')}
    )
    response.raise_for_status()
    st.write("Initialized session:", response.cookies)


def get_chat_response(prompt):
    url = "http://fastapi:8080/chat/ask/"
    start_time = datetime.datetime.now()
    with st.session_state.http_session.post(
            url,
            stream=True,
            headers={"x-access-token":
                     os.environ.get('FAST_API_ACCESS_SECRET_TOKEN')},
            json={'message': prompt}
            ) as response:
        for chunk in response.iter_content(chunk_size=1024):
            if chunk:
                yield str(chunk, encoding="utf-8")
    rd = dateutil.relativedelta.relativedelta(datetime.datetime.now(),
                                              start_time)
    yield "\n (%d minutes and %d seconds)" % (rd.minutes, rd.seconds)


st.title("Hipposys Chat")

if "messages" not in st.session_state:
    st.session_state.messages = [{"role": "ai", "content": "Hello 👋"}]

# Display chat messages from history on app rerun
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])


if prompt := st.chat_input("What is up?"):
    # Display user message in chat message container
    with st.chat_message("user"):
        st.markdown(prompt)
    # Add user message to chat history
    st.session_state.messages.append({"role": "user", "content": prompt})

    with st.chat_message("assistant"):
        response = st.write_stream(get_chat_response(prompt))
    # Add assistant response to chat history
    st.session_state.messages.append({"role": "assistant",
                                      "content": response})
