import json
import streamlit as st
from google.cloud import bigquery
import google.generativeai as palm
from google.oauth2 import service_account
import google.generativeai as genai

# Ambil API Key dari secrets
#api_key = st.secrets["vertex_ai"]
#api_key = st.secrets["vertex_ai"]["api_key"]
#palm.configure(api_key=api_key)

# Konfigurasi BigQuery Client
credentials_info = {
    "type": st.secrets["bigquery"]["type"],
    "project_id": st.secrets["bigquery"]["project_id"],
    "private_key_id": st.secrets["bigquery"]["private_key_id"],
    "private_key": st.secrets["bigquery"]["private_key"].replace("\\n", "\n"),  # Perbaiki newline
    "client_email": st.secrets["bigquery"]["client_email"],
    "client_id": st.secrets["bigquery"]["client_id"],
    "auth_uri": st.secrets["bigquery"]["auth_uri"],
    "token_uri": st.secrets["bigquery"]["token_uri"],
    "auth_provider_x509_cert_url": st.secrets["bigquery"]["auth_provider_x509_cert_url"],
    "client_x509_cert_url": st.secrets["bigquery"]["client_x509_cert_url"],
    "universe_domain": st.secrets["bigquery"]["universe_domain"],
}

# Konversi ke objek credentials
credentials = service_account.Credentials.from_service_account_info(credentials_info)

# Inisialisasi BigQuery Client
client = bigquery.Client(credentials=credentials, project=credentials_info["project_id"])

# Kirim prompt ke model
api_key = st.secrets["genai"]["api_key"]
genai.configure(api_key=api_key)

#print(genai.list_models())
#model = genai.GenerativeModel("gemini-1.0-pro")
models = genai.list_models()

# Print semua model yang bisa digunakan
for model in models:
    print(model.name)
    
# UI Streamlit
st.title("Query Data BigQuery dengan NLP")

# Input Natural Language
prompt = st.text_input("Masukkan pertanyaan (misal: 'Top 5 penyedia di Jakarta'):")

if st.button("Jalankan Query"):
    # Kirim ke Gemini untuk diubah ke SQL
    response = model.generate_content(f"Convert this question to SQL query: {prompt}")

    # Ambil SQL yang dihasilkan
    generated_sql = response.text

    st.write("Query SQL yang dihasilkan:")
    st.code(generated_sql)

    # Jalankan SQL ke BigQuery
    query_job = client.query(generated_sql)
    results = query_job.result()

    # Tampilkan hasil dalam tabel
    st.write("Hasil Query:")
    st.dataframe(results.to_dataframe())
