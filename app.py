import json
import streamlit as st
from google.cloud import bigquery
import google.generativeai as genai
from google.oauth2 import service_account

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

# Gantilah dengan dataset dan tabel yang benar
dataset_id = "transdata-451904.full"
table_id = "transdata-451904.full.transdata"

# Ambil schema tabel dari BigQuery
table_ref = client.get_table(table_id)
schema_info = "\n".join([f"{field.name} ({field.field_type})" for field in table_ref.schema])

# Ambil sample data dari tabel
query = f"SELECT * FROM `{table_id}` LIMIT 5"
query_job = client.query(query)
rows = query_job.result()

# Konversi hasil query ke JSON agar lebih terbaca
sample_data = [dict(row.items()) for row in rows]  # List of dictionaries
sample_data_str = json.dumps(sample_data, indent=2)  # Convert to JSON string

# Konfigurasi Google Gemini API
api_key = st.secrets["genai"]["api_key"]
genai.configure(api_key=api_key)

# Pilih model Gemini
model = genai.GenerativeModel("gemini-1.5-flash-latest")

# UI Streamlit
st.title("Query Data BigQuery dengan NLP")

# Input Natural Language
user_question = st.text_input("Masukkan pertanyaan (misal: 'Top 5 penyedia di Jakarta'):")

if st.button("Jalankan Query"):
    
    # Buat prompt untuk Gemini
    prompt = f"""
    Based on the following BigQuery table schema and sample data, convert the given natural language question into an SQL query.

    Table Schema:
    {schema_info}

    Sample Data:
    {sample_data_str}

    User Question: "{user_question}"
    """

    # Minta Gemini untuk membuat SQL berdasarkan data BigQuery yang tersedia
    response = model.generate_content(prompt)

    # Pastikan Gemini memberikan output sebelum lanjut ke eksekusi
    if response and hasattr(response, "text"):
        generated_sql = response.text
        st.write("Query SQL yang dihasilkan:")
        st.code(generated_sql)

        # Validasi format SQL sebelum dijalankan
        if "SELECT" in generated_sql.upper():
            try:
                # Jalankan SQL ke BigQuery
                query_job = client.query(generated_sql)
                results = query_job.result()

                # Tampilkan hasil dalam tabel jika tidak kosong
                if results.total_rows > 0:
                    st.write("Hasil Query:")
                    st.dataframe(results.to_dataframe())
                else:
                    st.warning("Query berhasil dijalankan, tetapi tidak ada hasil.")
            except Exception as e:
                st.error(f"Terjadi kesalahan saat menjalankan query: {e}")
        else:
            st.error("SQL yang dihasilkan tidak valid.")
    else:
        st.error("Gemini gagal menghasilkan query SQL.")
