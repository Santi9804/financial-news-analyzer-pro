import sys
import os
import requests
import xml.etree.ElementTree as ET
import re
from datetime import datetime

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from app.services.news_utils import (
    HEADERS,
    clean_text,
    build_news_item,
    save_news_to_db
)
from app.db.connection import create_table

URL = "https://www.bloomberg.com/sitemaps/news/latest.xml"
MAX_NEWS = 20


def extract_date_from_url(url):
    match = re.search(r"/(\d{4}-\d{2}-\d{2})/", url)
    if match:
        return match.group(1)
    return ""


def normalize_date(date_text):
    if not date_text:
        return ""

    date_text = clean_text(date_text)

    try:
        dt = datetime.fromisoformat(date_text.replace("Z", "+00:00"))
        return dt.isoformat()
    except Exception:
        pass

    return date_text


def build_fallback_content(title, link, published_at):
    date_text = published_at if published_at else extract_date_from_url(link)

    content = (
        f"Bloomberg reportó la siguiente noticia el {date_text}: {title}. "
        f"Esta noticia fue identificada desde el sitemap de Bloomberg. "
        f"El artículo completo no pudo ser extraído automáticamente debido a restricciones de acceso del portal, "
        f"pero el titular sugiere un evento relevante para análisis financiero y de mercado."
    )

    return content


def get_bloomberg_news():
    response = requests.get(URL, headers=HEADERS, timeout=20)
    print("Status code sitemap:", response.status_code)
    response.raise_for_status()

    root = ET.fromstring(response.text)

    ns = {
        "sm": "http://www.sitemaps.org/schemas/sitemap/0.9",
        "news": "http://www.google.com/schemas/sitemap-news/0.9"
    }

    news_list = []
    seen = set()

    for url_node in root.findall("sm:url", ns):
        loc = url_node.find("sm:loc", ns)
        title_node = url_node.find("news:news/news:title", ns)
        publication_date_node = url_node.find("news:news/news:publication_date", ns)

        if loc is None or title_node is None:
            continue

        link = clean_text(loc.text)
        title = clean_text(title_node.text)

        if not link or not title:
            continue

        if "/news/articles/" not in link:
            continue

        if link in seen:
            continue
        seen.add(link)

        published_at = ""
        if publication_date_node is not None and publication_date_node.text:
            published_at = normalize_date(publication_date_node.text)

        if not published_at:
            published_at = extract_date_from_url(link)

        print("\nTítulo:", title)
        print("Link:", link)
        print("Fecha detectada:", published_at)

        content = build_fallback_content(title, link, published_at)
        print("Contenido generado:", content[:120])

        news_item = build_news_item(
            title=title,
            link=link,
            content=content,
            source="Bloomberg",
            published_at=published_at
        )

        news_list.append(news_item)
        print("-> noticia agregada")

        if len(news_list) >= MAX_NEWS:
            break

    print("\nTotal noticias útiles:", len(news_list))
    return news_list


if __name__ == "__main__":
    create_table()

    news = get_bloomberg_news()
    save_news_to_db(news)

    for i, item in enumerate(news, 1):
        print(f"\nNoticia {i}")
        print("Título:", item["title"])
        print("Fuente:", item["source"])
        print("Fecha:", item.get("published_at", ""))
        print("Impacto:", item["impact_general"])
        print("Link:", item["link"])
        print("Resumen:", item["content"][:300])
        print("-" * 100)