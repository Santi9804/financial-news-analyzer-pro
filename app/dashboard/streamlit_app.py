import os
import sys
import streamlit as st
import pandas as pd
import json
from datetime import datetime
from app.collectors.valora_collector import get_valora_news
from app.services.news_utils import save_news_to_db

# Agrega la raíz del proyecto al path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from app.db.connection import get_connection, create_table


def configure_page():
    st.set_page_config(
        page_title="Financial News Analyzer Pro",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="expanded"
    )


def load_custom_css():
    st.markdown("""
    <style>
        .app-title {
            font-size: 2.3rem;
            font-weight: 700;
            margin-bottom: 0.2rem;
        }

        .app-subtitle {
            color: #9aa4b2;
            font-size: 1rem;
            margin-bottom: 1.2rem;
        }

        .news-card {
            background-color: #111827;
            border: 1px solid #243041;
            border-radius: 14px;
            padding: 1rem;
            margin-bottom: 1rem;
        }

        .news-title {
            font-size: 1.1rem;
            font-weight: 600;
            color: white;
            margin-bottom: 0.5rem;
        }

        .news-meta {
            font-size: 0.85rem;
            color: #9aa4b2;
            margin-bottom: 0.5rem;
        }

        .source-badge {
            display: inline-block;
            padding: 0.22rem 0.55rem;
            border-radius: 999px;
            font-size: 0.75rem;
            font-weight: 600;
            margin-right: 0.5rem;
            margin-bottom: 0.7rem;
            background-color: rgba(59, 130, 246, 0.15);
            color: #93c5fd;
            border: 1px solid rgba(59, 130, 246, 0.35);
        }

        .impact-badge {
            display: inline-block;
            padding: 0.25rem 0.6rem;
            border-radius: 999px;
            font-size: 0.78rem;
            font-weight: 600;
            margin-bottom: 0.7rem;
        }

        .impact-positive {
            background-color: rgba(34, 197, 94, 0.15);
            color: #22c55e;
            border: 1px solid rgba(34, 197, 94, 0.35);
        }

        .impact-negative {
            background-color: rgba(239, 68, 68, 0.15);
            color: #ef4444;
            border: 1px solid rgba(239, 68, 68, 0.35);
        }

        .impact-neutral {
            background-color: rgba(156, 163, 175, 0.15);
            color: #d1d5db;
            border: 1px solid rgba(156, 163, 175, 0.35);
        }

        .entity-tag {
            display: inline-block;
            background-color: #1f2937;
            color: #cbd5e1;
            border-radius: 999px;
            padding: 0.2rem 0.55rem;
            margin-right: 0.3rem;
            margin-bottom: 0.3rem;
            font-size: 0.75rem;
        }
    </style>
    """, unsafe_allow_html=True)


@st.cache_data
def load_data():
    create_table()

    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            SELECT
                title,
                link,
                content,
                source,
                published_at,
                companies,
                commodities,
                raw_materials,
                indices,
                currencies,
                sectors,
                impact_general,
                impact_score_general,
                impact_by_company,
                impact_by_commodity,
                impact_by_raw_material,
                impact_by_index,
                impact_by_currency,
                impact_by_sector
            FROM news
            ORDER BY id DESC
        """)
        rows = cursor.fetchall()
    except:
        rows = []

    conn.close()

    df = pd.DataFrame(rows, columns=[
        "title","link","content","source","published_at",
        "companies","commodities","raw_materials","indices",
        "currencies","sectors","impact_general",
        "impact_score_general","impact_by_company",
        "impact_by_commodity","impact_by_raw_material",
        "impact_by_index","impact_by_currency",
        "impact_by_sector"
    ])

    if df.empty:
        return df

    df = df.fillna("")
    df["published_at"] = pd.to_datetime(df["published_at"], errors="coerce", utc=True)
    df["impact_score_general"] = pd.to_numeric(df["impact_score_general"], errors="coerce").fillna(0)

    return df.sort_values(by="published_at", ascending=False)
    for col in df.columns:
        df[col] = df[col].fillna("")

    df["published_at_raw"] = df["published_at"]
    df["published_at"] = pd.to_datetime(df["published_at"], errors="coerce", utc=True)
    df["impact_score_general"] = pd.to_numeric(df["impact_score_general"], errors="coerce").fillna(0)
    df["impact_score"] = df["impact_score_general"]

    df = df.sort_values(by="published_at", ascending=False, na_position="last")

    return df


def render_header():
    st.markdown('<div class="app-title">📊 Financial News Analyzer Pro</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="app-subtitle">Monitorea noticias financieras, detecta entidades clave y evalúa impacto por categoría e índice.</div>',
        unsafe_allow_html=True
    )
    st.caption(f"Última actualización visual: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


def parse_json_field(text):
    try:
        if not text:
            return {}
        return json.loads(text)
    except Exception:
        return {}


def split_entities(text):
    if not text:
        return []
    return [item.strip() for item in str(text).split(",") if item.strip()]


def collect_all(df, column_name):
    values = set()
    for item in df[column_name]:
        for entity in split_entities(item):
            values.add(entity)
    return sorted(values)


def collect_all_sources(df):
    values = set()
    for source in df["source"]:
        if str(source).strip():
            values.add(source.strip())
    return sorted(values)


def format_published_at(value):
    if pd.isna(value):
        return "Fecha no disponible"

    try:
        return value.strftime("%Y-%m-%d %H:%M")
    except Exception:
        return "Fecha no disponible"


def apply_filters(df):
    st.sidebar.header("Filtros avanzados")

    all_sources = collect_all_sources(df)
    all_companies = collect_all(df, "companies")
    all_commodities = collect_all(df, "commodities")
    all_raw_materials = collect_all(df, "raw_materials")
    all_indices = collect_all(df, "indices")
    all_currencies = collect_all(df, "currencies")
    all_sectors = collect_all(df, "sectors")

    selected_source = st.sidebar.selectbox("Fuente", ["Todas"] + all_sources)
    selected_company = st.sidebar.selectbox("Empresa", ["Todas"] + all_companies)
    selected_commodity = st.sidebar.selectbox("Commodity", ["Todas"] + all_commodities)
    selected_raw_material = st.sidebar.selectbox("Materia prima", ["Todas"] + all_raw_materials)
    selected_index = st.sidebar.selectbox("Índice", ["Todas"] + all_indices)
    selected_currency = st.sidebar.selectbox("Divisa", ["Todas"] + all_currencies)
    selected_sector = st.sidebar.selectbox("Sector", ["Todos"] + all_sectors)
    selected_impact = st.sidebar.selectbox("Impacto general", ["Todos", "Positivo", "Negativo", "Neutro"])
    date_filter = st.sidebar.selectbox(
        "Periodo",
        ["Todo", "Últimas 24h", "Últimos 7 días", "Últimos 30 días"]
    )
    search_text = st.sidebar.text_input("Buscar palabra clave")

    if len(df) <= 5:
        max_news = len(df)
        st.sidebar.info("Hay pocas noticias disponibles")
    else:
        max_news = st.sidebar.slider(
            "Cantidad de noticias",
            min_value=5,
            max_value=min(100, len(df)),
            value=min(20, len(df))
        )

    filtered_df = df.copy()

    if selected_source != "Todas":
        filtered_df = filtered_df[
            filtered_df["source"].str.lower() == selected_source.lower()
        ]

    if selected_company != "Todas":
        filtered_df = filtered_df[
            filtered_df["companies"].str.contains(selected_company, case=False, na=False)
        ]

    if selected_commodity != "Todas":
        filtered_df = filtered_df[
            filtered_df["commodities"].str.contains(selected_commodity, case=False, na=False)
        ]

    if selected_raw_material != "Todas":
        filtered_df = filtered_df[
            filtered_df["raw_materials"].str.contains(selected_raw_material, case=False, na=False)
        ]

    if selected_index != "Todas":
        filtered_df = filtered_df[
            filtered_df["indices"].str.contains(selected_index, case=False, na=False)
        ]

    if selected_currency != "Todas":
        filtered_df = filtered_df[
            filtered_df["currencies"].str.contains(selected_currency, case=False, na=False)
        ]

    if selected_sector != "Todos":
        filtered_df = filtered_df[
            filtered_df["sectors"].str.contains(selected_sector, case=False, na=False)
        ]

    if selected_impact != "Todos":
        filtered_df = filtered_df[
            filtered_df["impact_general"].str.lower() == selected_impact.lower()
        ]

    if date_filter != "Todo":
        now = pd.Timestamp.now(tz="UTC")

        if date_filter == "Últimas 24h":
            filtered_df = filtered_df[
                filtered_df["published_at"] >= now - pd.Timedelta(days=1)
            ]
        elif date_filter == "Últimos 7 días":
            filtered_df = filtered_df[
                filtered_df["published_at"] >= now - pd.Timedelta(days=7)
            ]
        elif date_filter == "Últimos 30 días":
            filtered_df = filtered_df[
                filtered_df["published_at"] >= now - pd.Timedelta(days=30)
            ]

    if search_text:
        filtered_df = filtered_df[
            filtered_df["title"].str.contains(search_text, case=False, na=False) |
            filtered_df["content"].str.contains(search_text, case=False, na=False) |
            filtered_df["source"].str.contains(search_text, case=False, na=False)
        ]

    return filtered_df.head(max_news)


def render_kpis(df):
    total_news = len(df)
    total_sources = df["source"].nunique()

    all_entities = set()
    for col in ["companies", "commodities", "raw_materials", "indices", "currencies", "sectors"]:
        for value in df[col]:
            for entity in split_entities(value):
                all_entities.add(entity)

    positive_count = (df["impact_general"].str.lower() == "positivo").sum()
    negative_count = (df["impact_general"].str.lower() == "negativo").sum()
    neutral_count = (df["impact_general"].str.lower() == "neutro").sum()

    avg_score = round(df["impact_score_general"].mean(), 2) if not df.empty else 0

    latest_date = "No disponible"
    if not df.empty and df["published_at"].notna().any():
        latest_date = format_published_at(df["published_at"].max())

    col1, col2, col3, col4, col5, col6 = st.columns(6)
    col1.metric("Noticias", total_news)
    col2.metric("Fuentes", total_sources)
    col3.metric("Entidades", len(all_entities))
    col4.metric("Positivas", positive_count)
    col5.metric("Negativas", negative_count)
    col6.metric("Score promedio", avg_score)

    st.caption(f"Neutras: {neutral_count} | Última fecha detectada: {latest_date}")


def build_entity_ranking(df, column_name, label_name):
    stats = {}

    for _, row in df.iterrows():
        entities = split_entities(row[column_name])
        score = row.get("impact_score_general", 0)

        for entity in entities:
            if entity not in stats:
                stats[entity] = {
                    label_name: entity,
                    "Menciones": 0,
                    "Score acumulado": 0.0
                }

            stats[entity]["Menciones"] += 1
            stats[entity]["Score acumulado"] += score

    ranking_df = pd.DataFrame(list(stats.values()))

    if not ranking_df.empty:
        ranking_df["Score promedio"] = (
            ranking_df["Score acumulado"] / ranking_df["Menciones"]
        ).round(2)

    return ranking_df


def build_index_impact_summary(df):
    ranking_df = build_entity_ranking(df, "indices", "Índice")

    if ranking_df.empty:
        return ranking_df

    ranking_df = ranking_df.sort_values(by="Score acumulado", ascending=False)
    return ranking_df


def render_summary(df):
    st.subheader("📊 Panel de análisis")

    col1, col2 = st.columns(2)

    with col1:
        st.write("### Distribución de impacto")
        impact_counts = df["impact_general"].value_counts()
        if impact_counts.empty:
            st.info("No hay datos de impacto.")
        else:
            st.bar_chart(impact_counts)

    with col2:
        st.write("### Noticias por fuente")
        source_counts = df["source"].value_counts()
        if source_counts.empty:
            st.info("No hay fuentes disponibles.")
        else:
            st.bar_chart(source_counts)

    st.divider()

    c1, c2, c3 = st.columns(3)

    with c1:
        st.write("### Ranking de empresas")
        ranking_df = build_entity_ranking(df, "companies", "Empresa")
        if ranking_df.empty:
            st.info("No hay empresas detectadas.")
        else:
            st.dataframe(ranking_df.head(10), use_container_width=True, hide_index=True)

    with c2:
        st.write("### Ranking de índices")
        ranking_df = build_entity_ranking(df, "indices", "Índice")
        if ranking_df.empty:
            st.info("No hay índices detectados.")
        else:
            st.dataframe(ranking_df.head(10), use_container_width=True, hide_index=True)

    with c3:
        st.write("### Ranking de sectores")
        ranking_df = build_entity_ranking(df, "sectors", "Sector")
        if ranking_df.empty:
            st.info("No hay sectores detectados.")
        else:
            st.dataframe(ranking_df.head(10), use_container_width=True, hide_index=True)

    st.divider()

    st.write("### Impacto agregado por índice")
    index_summary_df = build_index_impact_summary(df)

    if index_summary_df.empty:
        st.info("No hay suficiente información por índice.")
    else:
        st.dataframe(index_summary_df.head(15), use_container_width=True, hide_index=True)


def render_top_movers(df):
    st.subheader("🚀 Top movers")

    categories = {
        "Empresas": "companies",
        "Commodities": "commodities",
        "Materias primas": "raw_materials",
        "Índices": "indices",
        "Divisas": "currencies",
        "Sectores": "sectors"
    }

    selected_category = st.selectbox("Categoría a analizar", list(categories.keys()))
    sort_option = st.selectbox(
        "Ordenar por",
        ["Score acumulado", "Score promedio", "Menciones"]
    )

    column_name = categories[selected_category]

    ranking_df = build_entity_ranking(
        df,
        column_name,
        selected_category[:-1] if selected_category.endswith("s") else selected_category
    )

    if ranking_df.empty:
        st.info("No hay datos para esta categoría.")
    else:
        ranking_df = ranking_df.sort_values(by=sort_option, ascending=False)
        st.dataframe(ranking_df.head(15), use_container_width=True, hide_index=True)


def render_index_focus(df):
    st.subheader("📈 Vista por índice")

    ranking_df = build_entity_ranking(df, "indices", "Índice")

    if ranking_df.empty:
        st.info("No hay datos de índices para analizar.")
        return

    available_indices = sorted(ranking_df["Índice"].tolist())
    selected_index = st.selectbox("Selecciona un índice para profundizar", available_indices)

    index_df = df[
        df["indices"].str.contains(selected_index, case=False, na=False)
    ].copy()

    st.write(f"### Noticias asociadas a {selected_index}")
    st.caption(f"Total detectadas: {len(index_df)}")

    companies_ranking = build_entity_ranking(index_df, "companies", "Empresa")

    col1, col2 = st.columns(2)

    with col1:
        st.write("#### Empresas más afectadas en este índice")
        if companies_ranking.empty:
            st.info("No hay empresas detectadas para este índice.")
        else:
            st.dataframe(
                companies_ranking.sort_values(by="Score acumulado", ascending=False).head(10),
                use_container_width=True,
                hide_index=True
            )

    with col2:
        st.write("#### Resumen del impacto del índice")
        positive_count = (index_df["impact_general"].str.lower() == "positivo").sum()
        negative_count = (index_df["impact_general"].str.lower() == "negativo").sum()
        neutral_count = (index_df["impact_general"].str.lower() == "neutro").sum()
        avg_score = round(index_df["impact_score_general"].mean(), 2) if not index_df.empty else 0

        st.metric("Noticias del índice", len(index_df))
        st.metric("Score promedio", avg_score)
        st.caption(f"Positivas: {positive_count} | Negativas: {negative_count} | Neutras: {neutral_count}")


def render_insights(df):
    st.subheader("🧠 Insights del mercado")

    if df.empty:
        st.warning("No hay datos suficientes.")
        return

    col1, col2 = st.columns(2)

    with col1:
        st.write("### ⚠️ Noticias negativas destacadas")
        negative_df = df[df["impact_general"].str.lower() == "negativo"]
        if negative_df.empty:
            st.success("No hay noticias negativas relevantes")
        else:
            for _, row in negative_df.head(5).iterrows():
                st.write(f"- {row['title']}")

    with col2:
        st.write("### 🚀 Noticias positivas destacadas")
        positive_df = df[df["impact_general"].str.lower() == "positivo"]
        if positive_df.empty:
            st.info("No hay noticias positivas relevantes")
        else:
            for _, row in positive_df.head(5).iterrows():
                st.write(f"- {row['title']}")

    st.divider()
    render_top_movers(df)
    st.divider()
    render_index_focus(df)


def get_impact_class(impact):
    impact = str(impact).strip().lower()

    if impact == "positivo":
        return "impact-positive"
    elif impact == "negativo":
        return "impact-negative"
    else:
        return "impact-neutral"


def render_entity_tags(title, entities):
    if entities:
        st.markdown(f"**{title}:**")
        html = ""
        for entity in entities:
            html += f'<span class="entity-tag">{entity}</span>'
        st.markdown(html, unsafe_allow_html=True)


def render_impact_dict(title, impact_dict):
    st.write(f"**{title}:**")

    if not impact_dict:
        st.write("No aplica")
        return

    for entity, impact in impact_dict.items():
        st.write(f"- {entity}: {impact}")


def render_company_index_map(company_to_indices):
    st.write("**Relación empresa → índices:**")

    if not company_to_indices:
        st.write("No aplica")
        return

    for company, indices in company_to_indices.items():
        if isinstance(indices, list) and indices:
            st.write(f"- {company}: {', '.join(indices)}")
        else:
            st.write(f"- {company}: Sin índices asociados")


def render_news_cards(df):
    st.subheader("📰 Noticias")

    if df.empty:
        st.warning("No hay noticias para mostrar con los filtros actuales.")
        return

    for _, row in df.iterrows():
        title = row["title"]
        link = row["link"]
        content = row["content"]
        source = row["source"]
        impact_general = row["impact_general"]
        impact_score = row["impact_score_general"]
        published_at_text = format_published_at(row["published_at"])

        companies = split_entities(row["companies"])
        commodities = split_entities(row["commodities"])
        raw_materials = split_entities(row["raw_materials"])
        indices = split_entities(row["indices"])
        currencies = split_entities(row["currencies"])
        sectors = split_entities(row["sectors"])

        impact_by_company = parse_json_field(row["impact_by_company"])
        impact_by_commodity = parse_json_field(row["impact_by_commodity"])
        impact_by_raw_material = parse_json_field(row["impact_by_raw_material"])
        impact_by_index = parse_json_field(row["impact_by_index"])
        impact_by_currency = parse_json_field(row["impact_by_currency"])
        impact_by_sector = parse_json_field(row["impact_by_sector"])
        company_to_indices = parse_json_field(row["company_to_indices"])

        badge_class = get_impact_class(impact_general)
        preview = content[:250] + "..." if len(content) > 250 else content

        st.markdown(f"""
        <div class="news-card">
            <div class="news-title">{title}</div>
            <span class="source-badge">{source if source else "Sin fuente"}</span>
            <span class="impact-badge {badge_class}">{impact_general}</span>
            <div class="news-meta">Fecha: {published_at_text}</div>
            <div class="news-meta">{preview}</div>
            <div class="news-meta">Score de impacto: {impact_score}</div>
        </div>
        """, unsafe_allow_html=True)

        render_entity_tags("Empresas", companies)
        render_entity_tags("Commodities", commodities)
        render_entity_tags("Materias primas", raw_materials)
        render_entity_tags("Índices", indices)
        render_entity_tags("Divisas", currencies)
        render_entity_tags("Sectores", sectors)

        if link:
            st.markdown(f"[Ver noticia original]({link})")

        with st.expander("Ver detalle completo"):
            st.write("**Fuente:**", source if source else "Sin fuente")
            st.write("**Fecha:**", published_at_text)
            st.write("**Contenido completo:**")
            st.write(content if content else "No hay contenido disponible.")

            st.write("### Impacto específico")
            render_impact_dict("Impacto por empresa", impact_by_company)
            render_impact_dict("Impacto por commodity", impact_by_commodity)
            render_impact_dict("Impacto por materia prima", impact_by_raw_material)
            render_impact_dict("Impacto por índice", impact_by_index)
            render_impact_dict("Impacto por divisa", impact_by_currency)
            render_impact_dict("Impacto por sector", impact_by_sector)

            st.write("### Índices asociados a empresas detectadas")
            render_company_index_map(company_to_indices)

        st.divider()


def main():
    configure_page()
    load_custom_css()
    render_header()

    df = load_data()

    if df.empty:
        st.warning("No hay noticias guardadas en la base de datos.")
        return

    filtered_df = apply_filters(df)

    tabs = st.tabs(["📰 Noticias", "📊 Análisis", "🧠 Insights"])

    with tabs[0]:
        render_kpis(filtered_df)
        st.divider()
        render_news_cards(filtered_df)

    with tabs[1]:
        render_summary(filtered_df)

    with tabs[2]:
        render_insights(filtered_df)


if __name__ == "__main__":
    main()
