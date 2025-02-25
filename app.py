import streamlit as st
from google.cloud import bigquery
import google.generativeai as palm
from google.oauth2 import service_account

# 🔹 CEK DAN AMBIL API KEY VERTEX AI
if "vertex_ai" not in st.secrets or "api_key" not in st.secrets["vertex_ai"]:
    st.error("⚠ API Key untuk Vertex AI tidak ditemukan. Pastikan sudah ditambahkan ke Streamlit Secrets.")
    st.stop()

api_key = st.secrets["vertex_ai"]["api_key"]
palm.configure(api_key=api_key)

# 🔹 CEK DAN AMBIL CREDENTIALS BIGQUERY
if "bigquery" not in st.secrets:
    st.error("⚠ BigQuery credentials tidak ditemukan di Streamlit Secrets.")
    st.stop()

try:
    credentials_info = {
        "type": st.secrets["bigquery"]["type"],
        "project_id": st.secrets["bigquery"]["project_id"],
        "private_key_id": st.secrets["bigquery"]["private_key_id"],
        "private_key": st.secrets["bigquery"]["private_key"].replace("\\n", "\n"),  # Fix newline issue
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
except Exception as e:
    st.error(f"⚠ Gagal membaca BigQuery credentials: {e}")
    st.stop()

# 🔹 STREAMLIT UI
st.title("🔍 Query Data BigQuery dengan NLP")

prompt = st.text_input("Masukkan pertanyaan (misal: 'Top 5 penyedia di Jakarta'):")

if st.button("Jalankan Query"):
    if not prompt.strip():
        st.warning("Masukkan pertanyaan terlebih dahulu.")
        st.stop()

    # 🔹 KIRIM KE VERTEX AI UNTUK DIUBAH KE SQL
    try:
        palm_response = palm.generate_text(
            model="models/text-bison-001", 
            prompt=f"Convert this question to SQL query: {prompt}",
            max_output_tokens=200  # Meningkatkan agar query tidak terpotong
        )
    except Exception as e:
        st.error(f"⚠ Error saat memanggil Vertex AI: {e}")
        st.stop()

    # 🔹 VALIDASI RESPON
    if not palm_response or not hasattr(palm_response, "result"):
        st.error("⚠ Tidak ada hasil dari Vertex AI. Coba pertanyaan lain.")
        st.stop()

    generated_sql = palm_response.result.strip()

    # 🔹 CEK APAKAH QUERY VALID
    if not generated_sql.lower().startswith("select"):
        st.error("⚠ Query yang dihasilkan bukan perintah SELECT. Mohon coba pertanyaan lain.")
        st.code(generated_sql, language="sql")
        st.stop()

    st.write("✅ Query SQL yang dihasilkan:")
    st.code(generated_sql, language="sql")

    # 🔹 JALANKAN QUERY KE BIGQUERY
    try:
        query_job = client.query(generated_sql)
        results = query_job.result()

        # Tampilkan hasil dalam tabel
        df = results.to_dataframe()
        st.write("📊 Hasil Query:")
        st.dataframe(df)

    except Exception as e:
        st.error(f"⚠ Terjadi kesalahan saat menjalankan query: {e}")
