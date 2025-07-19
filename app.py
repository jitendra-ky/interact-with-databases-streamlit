
import streamlit as st
import pyodbc

# Streamlit input fields for credentials
st.sidebar.header("Database Credentials")
SERVER = st.sidebar.text_input("Server", value="bossdb.database.windows.net")
USERNAME = st.sidebar.text_input("Username", value="boss")
PASSWORD = st.sidebar.text_input("Password", type="password", value="")
DRIVER = st.sidebar.text_input("ODBC Driver", value="{ODBC Driver 18 for SQL Server}")

# Helper to get connection string for a specific database
def get_connection_string(database):
    return f"DRIVER={DRIVER};SERVER={SERVER};DATABASE={database};UID={USERNAME};PWD={PASSWORD};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"

# Get list of databases
def list_databases():
    conn = pyodbc.connect(get_connection_string('master'))
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sys.databases")
    dbs = [row[0] for row in cursor.fetchall()]
    conn.close()
    return dbs

# Get list of tables in a database
def list_tables(database):
    conn = pyodbc.connect(get_connection_string(database))
    cursor = conn.cursor()
    cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")
    tables = [row[0] for row in cursor.fetchall()]
    conn.close()
    return tables

# Get table content
def get_table_content(database, table):
    conn = pyodbc.connect(get_connection_string(database))
    cursor = conn.cursor()
    # Get schema name for the table
    cursor.execute(f"SELECT TABLE_SCHEMA FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table}'")
    schema_row = cursor.fetchone()
    schema = schema_row[0] if schema_row else 'dbo'
    # Get column names and types
    cursor.execute(f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table}' AND TABLE_SCHEMA = '{schema}'")
    col_info = cursor.fetchall()
    select_cols = []
    unsupported_types = ['datetimeoffset', 'sql_variant', 'geometry', 'geography', 'hierarchyid']
    for col_name, data_type in col_info:
        if data_type.lower() in unsupported_types:
            select_cols.append(f"CONVERT(VARCHAR, [{col_name}]) AS [{col_name}]")
        else:
            select_cols.append(f"[{col_name}]")
    select_query = f"SELECT {', '.join(select_cols)} FROM [{schema}].[{table}]"
    cursor.execute(select_query)
    columns = [column[0] for column in cursor.description]
    rows = cursor.fetchall()
    conn.close()
    return columns, rows

# Delete a table from the database
def delete_table(database, table):
    conn = pyodbc.connect(get_connection_string(database))
    cursor = conn.cursor()
    cursor.execute(f"DROP TABLE [{table}]")
    conn.commit()
    conn.close()

# Extract database as zip containing CSVs for each table
import io
import zipfile
import pandas as pd

def extract_database_as_zip(database):
    tables = list_tables(database)
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        for table in tables:
            columns, rows = get_table_content(database, table)
            # Ensure rows are a list of tuples and match columns
            if not rows:
                df = pd.DataFrame(columns=columns)
            else:
                # Fix for rows with wrong shape
                fixed_rows = []
                for row in rows:
                    if len(columns) == 1 and not isinstance(row, tuple):
                        fixed_rows.append((row,))
                    elif len(row) != len(columns):
                        # Pad or trim row to match columns
                        fixed_rows.append(tuple(row)[:len(columns)] + (None,) * (len(columns) - len(row)))
                    else:
                        fixed_rows.append(tuple(row))
                df = pd.DataFrame(fixed_rows, columns=columns)
            csv_bytes = df.to_csv(index=False).encode("utf-8")
            zf.writestr(f"{table}.csv", csv_bytes)
    zip_buffer.seek(0)
    return zip_buffer

st.title("SQL Server Database Explorer")

# Step 1: List databases
databases = list_databases()
selected_db = st.selectbox("Select a database", databases)

if selected_db:
    tables = list_tables(selected_db)
    selected_table = st.selectbox("Select a table", tables)

    if selected_table:
        columns, rows = get_table_content(selected_db, selected_table)
        st.write(f"Showing contents of table: {selected_table}")
        st.dataframe([dict(zip(columns, row)) for row in rows])

        st.markdown("---")
        st.warning(f"Delete table '{selected_table}' from database '{selected_db}'?")
        if st.button(f"Delete Table '{selected_table}'"):
            delete_table(selected_db, selected_table)
            st.success(f"Table '{selected_table}' deleted successfully!")
            st.experimental_rerun()

    st.markdown("---")
    st.info(f"Extract complete database '{selected_db}' as zip containing CSVs for each table.")
    if st.button(f"Download '{selected_db}' as zip"):
        zip_buffer = extract_database_as_zip(selected_db)
        st.download_button(
            label=f"Download {selected_db}.zip",
            data=zip_buffer,
            file_name=f"{selected_db}.zip",
            mime="application/zip"
        )
