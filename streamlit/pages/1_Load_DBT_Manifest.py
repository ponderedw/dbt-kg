import streamlit as st
import requests

with st.form("user_form"):
    catalog_file = st.file_uploader("Upload Catalog",
                                    type=['json'])
    manifest_file = st.file_uploader("Upload Manifest",
                                     type=["json"])
    submitted = st.form_submit_button("Parse Json Files")

if submitted:
    if (catalog_file and manifest_file):
        url = 'http://fastapi:8080/embeddings/upload_dbt_to_kg/'
        response = requests.post(
                url,
                files={'catalog_file': catalog_file,
                       'manifest_file': manifest_file}
                )
        st.write(response.status_code)
    else:
        st.warning("Please upload files before submitting and add node id.")
