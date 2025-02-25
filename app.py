import streamlit as st
from google.cloud import bigquery
import google.generativeai as palm

# Ambil API Key dari secrets
api_key = st.secrets["vertex_ai"]["api_key"]
palm.configure(api_key=api_key)

# Konfigurasi BigQuery Client
client = bigquery.Client()

# UI Streamlit
st.title("Query Data BigQuery dengan NLP")

# Input Natural Language
prompt = st.text_input("Masukkan pertanyaan (misal: 'Top 5 penyedia di Jakarta'):")

if st.button("Jalankan Query"):
    # Kirim ke Vertex AI untuk diubah ke SQL
    palm_response = palm.generate_text(
        model="models/text-bison-001", 
        prompt=f"Convert this question to SQL query: {prompt}",
        max_output_tokens=100
    )
    
    # Ambil SQL yang dihasilkan
    generated_sql = palm_response.result

    st.write("Query SQL yang dihasilkan:")
    st.code(generated_sql)

    # Jalankan SQL ke BigQuery
    query_job = client.query(generated_sql)
    results = query_job.result()

    # Tampilkan hasil dalam tabel
    st.write("Hasil Query:")
    st.dataframe(results.to_dataframe())
