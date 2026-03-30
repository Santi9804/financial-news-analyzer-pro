import requests
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from bs4 import BeautifulSoup
from app.db.connection import create_table
from app.services.news_utils import (
    HEADERS,
    clean_text,
    build_news_item,
    save_news_to_db
)

URL = "https://www.valoraanalitik.com/ultimas-noticias/"
MAX_NEWS = 12


def extract_published_at(soup):
    try:
        time_tag = soup.find("time")

        if time_tag:
            datetime_value = time_tag.get("datetime")
            if datetime_value:
                return datetime_value.strip()

            text_value = clean_text(time_tag.get_text(" ", strip=True))
            if text_value:
                return text_value

        meta_candidates = [
            {"property": "article:published_time"},
            {"name": "article:published_time"},
            {"property": "og:updated_time"},
            {"name": "publish_date"},
        ]

        for attrs in meta_candidates:
            meta_tag = soup.find("meta", attrs=attrs)
            if meta_tag and meta_tag.get("content"):
                return meta_tag.get("content").strip()

    except Exception:
        pass

    return ""


def is_valid_news_link(title, link):
    if not title or not link:
        return False

    if not link.startswith("http"):
        return False

    if "valoraanalitik.com" not in link:
        return False

    banned_terms = [
        "suscrib",
        "newsletter",
        "contact",
        "planes",
        "precio",
        "login",
        "wp-content",
        "tag/",
        "category/",
        "#",
    ]

    link_lower = link.lower()
    title_lower = title.lower()

    for term in banned_terms:
        if term in link_lower or term in title_lower:
            return False

    if len(title) < 30:
        return False

    return True


def is_valid_article_content(title, content):
    if not content:
        return False

    content_lower = content.lower()

    banned_content_terms = [
        "solicite precios",
        "inicio – contacto",
        "email: [email protected]",
        "te enviaremos correos noticiosos",
        "suscríbete",
        "todos los derechos reservados"
    ]

    for term in banned_content_terms:
        if term in content_lower:
            return False

    if len(content) < 250:
        return False

    return True


def get_article_content_and_date(url):
    try:
        response = requests.get(url, headers=HEADERS, timeout=15)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")
        published_at = extract_published_at(soup)

        possible_classes = [
            "td-post-content",
            "entry-content",
            "post-content",
            "article-content"
        ]

        for class_name in possible_classes:
            article = soup.find("div", class_=class_name)
            if article:
                paragraphs = article.find_all("p")
                text = " ".join([p.get_text(" ", strip=True) for p in paragraphs])
                text = clean_text(text)

                if len(text) > 100:
                    return text[:2000], published_at

        paragraphs = soup.find_all("p")
        text = " ".join([p.get_text(" ", strip=True) for p in paragraphs])
        text = clean_text(text)

        if len(text) > 100:
            return text[:2000], published_at

        return "Contenido no encontrado", published_at

    except Exception:
        return "Error al obtener contenido", ""


def get_valora_news():
    response = requests.get(URL, headers=HEADERS, timeout=15)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")

    news_list = []
    seen_links = set()
    articles = soup.find_all("a", href=True)

    for tag in articles:
        title = clean_text(tag.get_text(" ", strip=True))
        link = tag.get("href", "").strip()

        if not is_valid_news_link(title, link):
            continue

        if link in seen_links:
            continue

        seen_links.add(link)

        content, published_at = get_article_content_and_date(link)

        if content in ["Contenido no encontrado", "Error al obtener contenido"]:
            continue

        if not is_valid_article_content(title, content):
            continue

        news_item = build_news_item(
            title=title,
            link=link,
            content=content,
            source="Valora Analitik",
            published_at=published_at
        )

        news_list.append(news_item)

        if len(news_list) >= MAX_NEWS:
            break

    return news_list


if __name__ == "__main__":
    create_table()
    news = get_valora_news()
    save_news_to_db(news)

    print(f"\nTotal noticias válidas encontradas: {len(news)}")

    for i, item in enumerate(news, start=1):
        print(f"\nNoticia {i}")
        print(f"Título: {item['title']}")
        print(f"Fuente: {item['source']}")
        print(f"Fecha: {item.get('published_at', '')}")
        print(f"Impacto general: {item['impact_general']}")
        print(f"Empresas: {item['companies']}")
        print(f"Commodities: {item['commodities']}")
        print(f"Materias primas: {item['raw_materials']}")
        print(f"Índices: {item['indices']}")
        print(f"Divisas: {item['currencies']}")
        print(f"Sectores: {item['sectors']}")
        print(f"Resumen: {item['content'][:500]}")
        print("-" * 100)