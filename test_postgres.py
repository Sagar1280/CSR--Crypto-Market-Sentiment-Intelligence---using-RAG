import psycopg2

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    user="csr_user",
    password="csr_pass",
    dbname="csr_db"
)

print("Connected to Postgres!")
conn.close()
