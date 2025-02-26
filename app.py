import json
import streamlit as st
from google.cloud import bigquery
import google.generativeai as genai
from google.oauth2 import service_account
from decimal import Decimal
from datetime import datetime

# Fungsi untuk mengubah data ke format JSON yang bisa dibaca
def serialize(obj):
    if isinstance(obj, Decimal):
        return float(obj)  # Ubah Decimal ke float
    elif isinstance(obj, datetime):
        return obj.isoformat()  # Ubah datetime ke format string ISO
    elif isinstance(obj, bigquery.Row):
        return dict(obj.items())  # Ubah BigQuery Row ke dictionary
    else:
        raise TypeError(f"Type {type(obj)} not serializable")

# Konfigurasi BigQuery Client
credentials_info = {
    "type": st.secrets["bigquery"]["type"],
    "project_id": st.secrets["bigquery"]["project_id"],
    "private_key_id": st.secrets["bigquery"]["private_key_id"],
    "private_key": st.secrets["bigquery"]["private_key"].replace("\\n", "\n"),
    "client_email": st.secrets["bigquery"]["client_email"],
    "client_id": st.secrets["bigquery"]["client_id"],
    "auth_uri": st.secrets["bigquery"]["auth_uri"],
    "token_uri": st.secrets["bigquery"]["token_uri"],
    "auth_provider_x509_cert_url": st.secrets["bigquery"]["auth_provider_x509_cert_url"],
    "client_x509_cert_url": st.secrets["bigquery"]["client_x509_cert_url"],
    "universe_domain": st.secrets["bigquery"]["universe_domain"],
}

credentials = service_account.Credentials.from_service_account_info(credentials_info)
client = bigquery.Client(credentials=credentials, project=credentials_info["project_id"])

# Dataset & Tabel
dataset_id = "transdata-451904.full"
table_id = "transdata-451904.full.transdata"

# Ambil schema tabel
table_ref = client.get_table(table_id)
schema_info = "\n".join([f"{field.name} ({field.field_type})" for field in table_ref.schema])

# Ambil sample data dari tabel
query = f"SELECT * FROM `{table_id}` LIMIT 5"
query_job = client.query(query)
rows = query_job.result()

# Konversi hasil query ke JSON yang valid
sample_data = [dict(row.items()) for row in rows]
sample_data_str = json.dumps(sample_data, indent=2, default=serialize)  # Pakai default serializer

# Inisialisasi Google Gemini AI
api_key = st.secrets["genai"]["api_key"]
genai.configure(api_key=api_key)
model = genai.GenerativeModel("gemini-1.5-flash-latest")

# UI Streamlit
st.title("Query Data BigQuery dengan NLP")

# Input Natural Language
user_question = st.text_input("Masukkan pertanyaan (misal: 'Top 5 penyedia di Jakarta'):")

if st.button("Jalankan Query"):
    # Prompt untuk Gemini
    prompt = f"""
    Based on the following BigQuery table schema and sample data, convert the given natural language question into an SQL query.

    ## **Important Instructions**:
    - Use the exact table name: `{table_id}`
    - Use the exact column names and formatting as they appear in BigQuery.
    - Preserve **uppercase and lowercase** letters exactly as in the schema.
    - If a column name has spaces, enclose it in backticks (`).
    - Do not modify or guess any column names.
    - Ensure the query runs correctly in **Google BigQuery**.
    - Output directly with the SQL Query coDE, without need "```sql" or "```"
    - Also pay attention to the appearance of unnecessary backticks, that could makes sql query error
    - Pay lot of your attention on the field name from bigquery, mostly we are using space not underscore, so doublecheck on it more precise
    - Ensure always to use LOWER on all and every field's value and input's value
    - Always remember if the field name has space, enclose it in backticks (`).

    Table Schema:
    {schema_info}

    Sample Data:
    {sample_data_str}

    User Question: "{user_question}"
    """

    # Minta Gemini untuk membuat SQL berdasarkan data BigQuery yang tersedia
    response = model.generate_content(prompt)

    # Pastikan respons ada
    if response and hasattr(response, "text"):
        generated_sql = response.text.strip()
        st.write("Query SQL yang dihasilkan:")
        st.code(generated_sql, language="sql")

        # Jalankan SQL ke BigQuery
        query_job = client.query(generated_sql)
        results = query_job.result()

        # Tampilkan hasil dalam tabel
        st.write("Hasil Query:")
        st.dataframe(results.to_dataframe())
    else:
        st.error("Gagal mendapatkan query SQL dari Gemini. Coba lagi.")
