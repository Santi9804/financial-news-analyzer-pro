import re
import json
import requests
from bs4 import BeautifulSoup
from app.db.connection import get_connection
from app.nlp.index_company_aliases import COMPANY_ALIASES, COMPANY_TO_INDICES

# =========================
# CONFIGURACIÓN GENERAL
# =========================

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

# =========================
# LISTAS DE ENTIDADES
# =========================

COMMODITIES = [
    "petróleo", "crudo", "oro", "plata", "cobre", "gas natural",
    "carbón", "café", "azúcar", "trigo", "maíz", "soya", "litio",
    "níquel", "aluminio", "hierro",
    "oil", "crude", "gold", "silver", "copper", "natural gas",
    "coal", "coffee", "sugar", "wheat", "corn", "soy", "soybean",
    "lithium", "nickel", "aluminum", "iron"
]

RAW_MATERIALS = [
    "acero", "cemento", "madera", "algodón", "celulosa",
    "resina", "plástico", "silicio", "fertilizantes", "insumos",
    "steel", "cement", "wood", "cotton", "cellulose",
    "resin", "plastic", "silicon", "fertilizers", "inputs"
]

INDICES = [
    "S&P 500", "SP500", "Dow Jones", "Nasdaq", "Colcap", "MSCI",
    "Russell 2000", "IBEX 35", "DAX", "Nikkei", "FTSE 100",
    "Stoxx 600", "CAC 40", "Hang Seng"
]

CURRENCIES = [
    "dólar", "usd", "euro", "eur", "peso colombiano", "cop",
    "yen", "jpy", "yuan", "cny", "libra esterlina", "gbp",
    "franco suizo", "chf", "real brasileño", "brl",
    "dollar", "colombian peso", "pound sterling", "swiss franc", "brazilian real"
]

SECTORS = [
    "sector financiero", "sector bancario", "sector tecnológico",
    "sector energético", "sector minero", "sector consumo",
    "sector salud", "sector industrial", "sector inmobiliario",
    "sector automotriz", "sector agroindustrial", "sector transporte",
    "sector retail", "sector construcción", "sector asegurador",
    "financial sector", "banking sector", "technology sector",
    "energy sector", "mining sector", "consumer sector",
    "healthcare sector", "industrial sector", "real estate sector",
    "automotive sector", "transport sector", "retail sector",
    "construction sector", "insurance sector"
]

# =========================
# PALABRAS DE IMPACTO V2
# =========================

POSITIVE_PHRASES_STRONG = [
    "beat expectations", "beats expectations", "beats estimates",
    "tops estimates", "exceeds expectations", "strong earnings",
    "record revenue", "dividend increase", "bullish outlook",
    "guidance raised", "margin expansion", "price surge",
    "shares rally", "stock rally", "demand rises",
    "superó expectativas", "mejores resultados", "ingresos récord",
    "aumento de dividendos", "mejora de márgenes", "utilidad neta creció",
    "fuerte demanda", "guía al alza"
]

POSITIVE_PHRASES_MEDIUM = [
    "higher revenue", "revenue rose", "profit increased",
    "earnings growth", "upgraded", "strong demand",
    "rate cut bets", "cuts cholesterol", "costs fell",
    "ingresos crecieron", "utilidad aumentó", "costos bajaron",
    "mejora en ingresos", "crecimiento de utilidades"
]

NEGATIVE_PHRASES_STRONG = [
    "miss expectations", "missed expectations", "missed estimates",
    "weak guidance", "profit warning", "credit downgrade",
    "bearish outlook", "margin pressure", "supply crisis",
    "supply shock", "shares plunge", "stock slump",
    "cuts forecast", "warning on profits",
    "no alcanzó expectativas", "advertencia de ganancias",
    "rebaja de calificación", "presión en márgenes",
    "crisis de suministro", "recorte de proyecciones"
]

NEGATIVE_PHRASES_MEDIUM = [
    "rising costs", "lower demand", "revenue fell", "profit dropped",
    "cost pressures", "shares fall", "shares drop",
    "tariff risk", "demand weakens",
    "aumento de costos", "débil demanda", "ingresos cayeron",
    "utilidad cayó", "caída de la demanda"
]

POSITIVE_WORDS_STRONG = [
    "rally", "surge", "jump", "jumps", "jumped", "record",
    "outperform", "bullish", "beat", "beats", "growth",
    "profitability", "recovery", "solvency", "superávit",
    "récord", "rentabilidad", "recuperación", "solvencia",
    "revalorización", "plusvalía", "bonanza"
]

POSITIVE_WORDS_MEDIUM = [
    "profit", "profits", "gain", "gains", "rise", "rises", "rose",
    "strong", "strength", "improvement", "increase", "increased",
    "expansion", "benefit", "success", "dividend", "dividends",
    "cash flow", "innovation", "solid", "upside",
    "ganancia", "ganancias", "sube", "alza", "mejora",
    "avance", "beneficio", "utilidad", "éxito", "incremento",
    "expansión", "flujo de caja", "innovación", "solidez"
]

NEGATIVE_WORDS_STRONG = [
    "plunge", "slump", "bankruptcy", "default", "recession",
    "downgrade", "bearish", "crisis", "war", "fraud",
    "quiebra", "default", "recesión", "crisis", "fraude",
    "desplome", "impago", "insolvencia"
]

NEGATIVE_WORDS_MEDIUM = [
    "fall", "falls", "loss", "losses", "drop", "decline",
    "risk", "problem", "volatility", "uncertainty", "inflation",
    "layoffs", "deficit", "contraction", "deterioration",
    "debt", "illiquidity", "attack", "conflict",
    "caída", "pérdida", "pérdidas", "riesgo", "problema",
    "volatilidad", "incertidumbre", "inflación", "despidos",
    "déficit", "contracción", "deterioro", "endeudamiento",
    "ilíquidez", "ataque", "conflicto"
]

NEUTRAL_WORDS = [
    "announced", "reported", "presented", "strategy", "market",
    "sector", "industry", "report", "trend", "projection",
    "regulation", "budget", "agreement", "negotiation",
    "anunció", "reportó", "presentó", "estrategia", "mercado",
    "sector", "industria", "informe", "tendencia", "proyección",
    "regulación", "presupuesto", "acuerdo", "negociación"
]

INDICATORS = [
    "roe", "roa", "ebitda", "wacc", "liquidez", "solvencia",
    "margen bruto", "flujo de caja", "patrimonio", "capital", "roi",
    "gross margin", "cash flow", "equity", "margin"
]

POSITIVE_CONTEXT = [
    "aumenta", "crece", "mejora", "sube", "incrementa",
    "fuerte", "alto", "positivo", "sólido",
    "increases", "grows", "improves", "rises", "higher",
    "strong", "positive", "solid", "improving"
]

NEGATIVE_CONTEXT = [
    "disminuye", "cae", "reduce", "débil", "bajo", "negativo",
    "decreases", "falls", "drops", "reduces", "weak",
    "lower", "negative", "declines"
]

# =========================
# FUNCIONES BÁSICAS
# =========================

def clean_text(text):
    text = re.sub(r"\s+", " ", text or "")
    return text.strip()


def normalize_text(text):
    return clean_text(text).lower()


def unique_preserve_order(items):
    seen = set()
    result = []

    for item in items:
        key = item.strip().lower()
        if key and key not in seen:
            seen.add(key)
            result.append(item)

    return result

# =========================
# DETECCIÓN DE ENTIDADES
# =========================

def contains_term(text, term):
    pattern = r"\b" + re.escape(term.lower()) + r"\b"
    return re.search(pattern, text.lower()) is not None


def detect_entities(text, entity_list):
    found = []
    text_lower = normalize_text(text)

    for entity in entity_list:
        if contains_term(text_lower, entity):
            found.append(entity)

    return unique_preserve_order(found)


def detect_companies(text):
    found = []
    text_lower = normalize_text(text)

    for canonical_name, aliases in COMPANY_ALIASES.items():
        for alias in aliases:
            if contains_term(text_lower, alias):
                found.append(canonical_name)
                break

    return unique_preserve_order(found)


def detect_indices_from_companies(companies):
    found_indices = []

    for company in companies:
        company_indices = COMPANY_TO_INDICES.get(company, [])
        for index_name in company_indices:
            found_indices.append(index_name)

    return unique_preserve_order(found_indices)


def detect_all_indices(text, companies):
    direct_indices = detect_entities(text, INDICES)
    inferred_indices = detect_indices_from_companies(companies)

    combined = direct_indices + inferred_indices
    return unique_preserve_order(combined)


def build_company_to_indices_map(companies):
    result = {}

    for company in companies:
        result[company] = COMPANY_TO_INDICES.get(company, [])

    return result

# =========================
# CLASIFICACIÓN DE IMPACTO V2
# =========================

def calculate_impact_score(text):
    text_lower = normalize_text(text)
    score = 0

    for phrase in POSITIVE_PHRASES_STRONG:
        if phrase in text_lower:
            score += 3

    for phrase in POSITIVE_PHRASES_MEDIUM:
        if phrase in text_lower:
            score += 2

    for phrase in NEGATIVE_PHRASES_STRONG:
        if phrase in text_lower:
            score -= 3

    for phrase in NEGATIVE_PHRASES_MEDIUM:
        if phrase in text_lower:
            score -= 2

    for word in POSITIVE_WORDS_STRONG:
        if contains_term(text_lower, word):
            score += 2

    for word in POSITIVE_WORDS_MEDIUM:
        if contains_term(text_lower, word):
            score += 1

    for word in NEGATIVE_WORDS_STRONG:
        if contains_term(text_lower, word):
            score -= 2

    for word in NEGATIVE_WORDS_MEDIUM:
        if contains_term(text_lower, word):
            score -= 1

    for indicator in INDICATORS:
        if indicator in text_lower:
            for word in POSITIVE_CONTEXT:
                if contains_term(text_lower, word):
                    score += 1
            for word in NEGATIVE_CONTEXT:
                if contains_term(text_lower, word):
                    score -= 1

    return score


def classify_impact(text):
    score = calculate_impact_score(text)
    text_lower = normalize_text(text)

    neutral_hits = sum(word in text_lower for word in NEUTRAL_WORDS)

    if score >= 2:
        return "Positivo"
    elif score <= -2:
        return "Negativo"
    elif neutral_hits >= 2:
        return "Neutro"
    else:
        return "Neutro"


def classify_impact_for_entities(text, entities):
    results = {}

    for entity in entities:
        results[entity] = classify_impact(text)

    return results

# =========================
# EXTRACCIÓN DE TEXTO
# =========================

def extract_article_text_generic(url):
    try:
        response = requests.get(url, headers=HEADERS, timeout=20)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")

        possible_classes = [
            "article-body",
            "caas-body",
            "body-content",
            "entry-content",
            "article-content",
            "main-content",
            "post-content"
        ]

        for class_name in possible_classes:
            block = soup.find("div", class_=class_name)
            if block:
                paragraphs = block.find_all("p")
                text = " ".join(p.get_text(" ", strip=True) for p in paragraphs)
                text = clean_text(text)

                if len(text) > 120:
                    return text[:2000]

        paragraphs = soup.find_all("p")
        text = " ".join(p.get_text(" ", strip=True) for p in paragraphs)
        text = clean_text(text)

        if len(text) > 120:
            return text[:2000]

        return "Contenido no encontrado"

    except Exception:
        return "Error al obtener contenido"

# =========================
# CONSTRUCCIÓN DE NOTICIA
# =========================

def build_news_item(title, link, content, source, published_at=""):
    full_text = f"{title} {content}"

    companies = detect_companies(full_text)
    commodities = detect_entities(full_text, COMMODITIES)
    raw_materials = detect_entities(full_text, RAW_MATERIALS)
    indices = detect_all_indices(full_text, companies)
    currencies = detect_entities(full_text, CURRENCIES)
    sectors = detect_entities(full_text, SECTORS)
    company_to_indices = build_company_to_indices_map(companies)

    impact_text = full_text
    impact_score = calculate_impact_score(impact_text)
    impact_general = classify_impact(impact_text)

    return {
        "title": title,
        "link": link,
        "content": content,
        "source": source,
        "published_at": published_at,

        "companies": companies,
        "commodities": commodities,
        "raw_materials": raw_materials,
        "indices": indices,
        "currencies": currencies,
        "sectors": sectors,
        "company_to_indices": company_to_indices,

        "impact_general": impact_general,
        "impact_score_general": impact_score,

        "impact_by_company": classify_impact_for_entities(impact_text, companies),
        "impact_by_commodity": classify_impact_for_entities(impact_text, commodities),
        "impact_by_raw_material": classify_impact_for_entities(impact_text, raw_materials),
        "impact_by_index": classify_impact_for_entities(impact_text, indices),
        "impact_by_currency": classify_impact_for_entities(impact_text, currencies),
        "impact_by_sector": classify_impact_for_entities(impact_text, sectors),
    }

# =========================
# GUARDAR EN BD
# =========================

def save_news_to_db(news_list):
    conn = get_connection()
    cursor = conn.cursor()

    for item in news_list:
        try:
            cursor.execute("""
                INSERT OR IGNORE INTO news (
                    title, link, content, source, published_at,
                    companies, commodities, raw_materials, indices, currencies, sectors,
                    impact_general, impact_score_general,
                    impact_by_company, impact_by_commodity,
                    impact_by_raw_material, impact_by_index,
                    impact_by_currency, impact_by_sector,
                    company_to_indices
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                item["title"],
                item["link"],
                item["content"],
                item["source"],
                item.get("published_at", ""),

                ", ".join(item["companies"]),
                ", ".join(item["commodities"]),
                ", ".join(item["raw_materials"]),
                ", ".join(item["indices"]),
                ", ".join(item["currencies"]),
                ", ".join(item["sectors"]),

                item["impact_general"],
                item.get("impact_score_general", 0),

                json.dumps(item["impact_by_company"], ensure_ascii=False),
                json.dumps(item["impact_by_commodity"], ensure_ascii=False),
                json.dumps(item["impact_by_raw_material"], ensure_ascii=False),
                json.dumps(item["impact_by_index"], ensure_ascii=False),
                json.dumps(item["impact_by_currency"], ensure_ascii=False),
                json.dumps(item["impact_by_sector"], ensure_ascii=False),
                json.dumps(item.get("company_to_indices", {}), ensure_ascii=False),
            ))
        except Exception as e:
            print(f"Error guardando noticia: {e}")

    conn.commit()
    conn.close()