import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd

# ── Session Snowflake (injectée automatiquement par Snowflake) ────────────────
session = get_active_session()

# ── Configuration page ────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Snowflake Schema Explorer",
    page_icon="❄️",
    layout="wide",
)

st.title("❄️ Snowflake — Explorateur de Schéma")
st.markdown("---")


# ── Helper ────────────────────────────────────────────────────────────────────
def run_sql(query: str) -> pd.DataFrame:
    """Exécute une requête SQL et retourne un DataFrame pandas."""
    return session.sql(query).to_pandas()


# ── Sidebar : sélection dynamique DB / Schema ─────────────────────────────────
with st.sidebar:
    st.header("⚙️ Paramètres")

    # Liste des bases de données
    try:
        dbs_df = run_sql("SHOW DATABASES")
        db_names = dbs_df["name"].tolist()
    except Exception as e:
        st.error(f"Impossible de lister les bases : {e}")
        db_names = []

    selected_db = st.selectbox("🗄️ Base de données", db_names) if db_names else None

    # Liste des schémas
    schema_names = []
    if selected_db:
        try:
            schemas_df = run_sql(f"SHOW SCHEMAS IN DATABASE {selected_db}")
            schema_names = schemas_df["name"].tolist()
        except Exception as e:
            st.error(f"Impossible de lister les schémas : {e}")

    selected_schema = st.selectbox("📂 Schéma", schema_names) if schema_names else None

    st.markdown("---")
    if selected_db and selected_schema:
        st.success(f"**{selected_db}** › **{selected_schema}**")


# ── Corps principal ───────────────────────────────────────────────────────────
if not selected_db or not selected_schema:
    st.info("👈 Sélectionnez une base de données et un schéma dans la barre latérale.")
    st.stop()

# Métriques en haut
col1, col2, col3 = st.columns(3)
col1.metric("Base de données", selected_db)
col2.metric("Schéma", selected_schema)

# ── Liste des tables ──────────────────────────────────────────────────────────
st.subheader(f"📋 Tables dans `{selected_db}`.`{selected_schema}`")

try:
    tables_df = run_sql(f"""
        SELECT
            TABLE_NAME,
            TABLE_TYPE,
            ROW_COUNT,
            BYTES,
            CREATED,
            LAST_ALTERED,
            COMMENT
        FROM {selected_db}.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = '{selected_schema}'
        ORDER BY TABLE_NAME
    """)

    col3.metric("Nb de tables", len(tables_df))

    if tables_df.empty:
        st.info("Aucune table trouvée dans ce schéma.")
        st.stop()

    # Formatage taille
    def format_bytes(b):
        if pd.isna(b) or b == 0:
            return "—"
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if b < 1024:
                return f"{b:.1f} {unit}"
            b /= 1024
        return f"{b:.1f} PB"

    tables_df["TAILLE"] = tables_df["BYTES"].apply(format_bytes)
    tables_df["ROW_COUNT"] = tables_df["ROW_COUNT"].apply(
        lambda x: f"{int(x):,}" if pd.notna(x) else "—"
    )

    display_df = tables_df[[
        "TABLE_NAME", "TABLE_TYPE", "ROW_COUNT", "TAILLE", "CREATED", "LAST_ALTERED", "COMMENT"
    ]].rename(columns={
        "TABLE_NAME": "Nom",
        "TABLE_TYPE": "Type",
        "ROW_COUNT": "Nb lignes",
        "TAILLE": "Taille",
        "CREATED": "Créée le",
        "LAST_ALTERED": "Modifiée le",
        "COMMENT": "Commentaire",
    })

    # Recherche
    search = st.text_input("🔍 Filtrer les tables", placeholder="Tapez un nom de table…")
    if search:
        mask = display_df["Nom"].str.contains(search, case=False, na=False)
        display_df = display_df[mask]
        st.caption(f"{len(display_df)} résultat(s)")

    st.dataframe(display_df, use_container_width=True, hide_index=True)

    # ── Aperçu d'une table ────────────────────────────────────────────────────
    st.markdown("---")
    st.subheader("🔎 Aperçu d'une table")

    table_list = tables_df["TABLE_NAME"].tolist()
    selected_table = st.selectbox("Choisir une table", ["— Aucune —"] + table_list)

    if selected_table != "— Aucune —":
        limit = st.slider("Nombre de lignes à afficher", 5, 1000, 100, step=5)

        with st.spinner(f"Chargement de {selected_table}…"):
            try:
                preview_df = run_sql(
                    f'SELECT * FROM "{selected_db}"."{selected_schema}"."{selected_table}" LIMIT {limit}'
                )
                st.caption(f"**{selected_table}** — {len(preview_df)} ligne(s) affichée(s)")
                st.dataframe(preview_df, use_container_width=True, hide_index=True)

                with st.expander("📐 Structure des colonnes"):
                    cols_df = run_sql(f"""
                        SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT, COMMENT
                        FROM {selected_db}.INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_SCHEMA = '{selected_schema}'
                          AND TABLE_NAME   = '{selected_table}'
                        ORDER BY ORDINAL_POSITION
                    """)
                    st.dataframe(cols_df, use_container_width=True, hide_index=True)

            except Exception as e:
                st.error(f"Impossible de charger la table : {e}")

except Exception as e:
    st.error(f"Erreur lors de la récupération des tables : {e}")
