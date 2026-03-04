import streamlit as st
import snowflake.connector
import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv()

st.set_page_config(
    page_title="Snowflake Schema Explorer",
    page_icon="❄️",
    layout="wide",
)

st.title("❄️ Snowflake — Explorateur de Schéma")
st.markdown("---")

@st.cache_resource(show_spinner="Connexion à Snowflake…")
def get_connection():
    try:
        conn = snowflake.connector.connect(
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
            role=os.getenv("SNOWFLAKE_ROLE") or None,
        )
        return conn
    except Exception as e:
        st.error(f"❌ Erreur de connexion : {e}")
        st.stop()

def run_query(conn, query):
    with conn.cursor() as cur:
        cur.execute(query)
        cols = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)

with st.sidebar:
    st.header("⚙️ Paramètres")
    conn = get_connection()

    try:
        databases_df = run_query(conn, "SHOW DATABASES")
        db_names = databases_df["name"].tolist()
    except Exception:
        db_names = [os.getenv("SNOWFLAKE_DATABASE", "")]

    default_db = os.getenv("SNOWFLAKE_DATABASE", db_names[0])
    selected_db = st.selectbox(
        "Base de données",
        db_names,
        index=db_names.index(default_db) if default_db in db_names else 0,
    )

    try:
        schemas_df = run_query(conn, f"SHOW SCHEMAS IN DATABASE {selected_db}")
        schema_names = schemas_df["name"].tolist()
    except Exception:
        schema_names = [os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC")]

    default_schema = os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC")
    selected_schema = st.selectbox(
        "Schéma",
        schema_names,
        index=schema_names.index(default_schema) if default_schema in schema_names else 0,
    )

    st.markdown("---")
    st.caption(f"Connecté : **{os.getenv('SNOWFLAKE_USER')}**")
    st.caption(f"Warehouse : **{os.getenv('SNOWFLAKE_WAREHOUSE')}**")

col1, col2, col3 = st.columns(3)
col1.metric("Base de données", selected_db)
col2.metric("Schéma", selected_schema)

st.subheader(f"📋 Tables dans `{selected_db}`.`{selected_schema}`")

try:
    tables_df = run_query(
        conn,
        f"""
        SELECT TABLE_NAME, TABLE_TYPE, ROW_COUNT, BYTES, CREATED, LAST_ALTERED, COMMENT
        FROM {selected_db}.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = '{selected_schema}'
        ORDER BY TABLE_NAME
        """,
    )

    col3.metric("Nombre de tables", len(tables_df))

    if tables_df.empty:
        st.info("Aucune table trouvée dans ce schéma.")
    else:
        def format_bytes(b):
            if pd.isna(b) or b == 0:
                return "—"
            for unit in ["B", "KB", "MB", "GB", "TB"]:
                if b < 1024:
                    return f"{b:.1f} {unit}"
                b /= 1024
            return f"{b:.1f} PB"

        tables_df["SIZE"] = tables_df["BYTES"].apply(format_bytes)
        tables_df["ROW_COUNT"] = tables_df["ROW_COUNT"].apply(
            lambda x: f"{int(x):,}" if pd.notna(x) else "—"
        )

        display_df = tables_df[
            ["TABLE_NAME", "TABLE_TYPE", "ROW_COUNT", "SIZE", "CREATED", "LAST_ALTERED", "COMMENT"]
        ].rename(columns={
            "TABLE_NAME": "Nom", "TABLE_TYPE": "Type", "ROW_COUNT": "Nb lignes",
            "SIZE": "Taille", "CREATED": "Créée le", "LAST_ALTERED": "Modifiée le", "COMMENT": "Commentaire",
        })

        search = st.text_input("🔍 Filtrer les tables", placeholder="Rechercher…")
        if search:
            mask = display_df["Nom"].str.contains(search, case=False, na=False)
            display_df = display_df[mask]

        st.dataframe(display_df, use_container_width=True, hide_index=True)

        st.markdown("---")
        st.subheader("🔎 Aperçu d'une table")
        table_names = tables_df["TABLE_NAME"].tolist()
        selected_table = st.selectbox("Choisir une table", ["— Aucune —"] + table_names)

        if selected_table != "— Aucune —":
            limit = st.slider("Nb de lignes", 5, 500, 50, step=5)
            with st.spinner(f"Chargement de {selected_table}…"):
                try:
                    preview_df = run_query(
                        conn,
                        f'SELECT * FROM "{selected_db}"."{selected_schema}"."{selected_table}" LIMIT {limit}',
                    )
                    st.dataframe(preview_df, use_container_width=True, hide_index=True)
                    with st.expander("📐 Structure des colonnes"):
                        cols_df = run_query(
                            conn,
                            f"""
                            SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT, COMMENT
                            FROM {selected_db}.INFORMATION_SCHEMA.COLUMNS
                            WHERE TABLE_SCHEMA = '{selected_schema}' AND TABLE_NAME = '{selected_table}'
                            ORDER BY ORDINAL_POSITION
                            """,
                        )
                        st.dataframe(cols_df, use_container_width=True, hide_index=True)
                except Exception as e:
                    st.error(f"Erreur : {e}")

except Exception as e:
    st.error(f"Erreur : {e}")
