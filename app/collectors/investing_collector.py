import sys
import os
import json
import re
import cloudscraper
from datetime import datetime

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from bs4 import BeautifulSoup
from app.services.news_utils import (
    clean_text,
    build_news_item,
    save_news_to_db
)
from app.db.connection import create_table

URLS = [
    "https://www.investing.com/news/latest-news",
    "https://www.investing.com/news/economy-news",
    "https://www.investing.com/news/stock-market-news",
    "https://www.investing.com/news/forex-news",
    "https://www.investing.com/news/commodities-news"
]

MAX_NEWS = 25

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,es;q=0.8",
    "Referer": "https://www.google.com/"
}


def extract_published_at(soup):
    try:
        # 1) <time>
        time_tag = soup.find("time")
        if time_tag:
            datetime_value = time_tag.get("datetime")
            if datetime_value:
                return datetime_value.strip()

            text_value = clean_text(time_tag.get_text(" ", strip=True))
            if text_value and any(char.isdigit() for char in text_value):
                return text_value

        # 2) Meta tags
        meta_candidates = [
            {"property": "article:published_time"},
            {"name": "article:published_time"},
            {"property": "og:updated_time"},
            {"name": "publish_date"},
            {"itemprop": "datePublished"},
            {"property": "og:published_time"},
        ]

        for attrs in meta_candidates:
            meta_tag = soup.find("meta", attrs=attrs)
            if meta_tag and meta_tag.get("content"):
                return meta_tag.get("content").strip()

        # 3) JSON-LD
        scripts = soup.find_all("script", type="application/ld+json")
        for script in scripts:
            raw = script.string or script.get_text(strip=True)
            if not raw:
                continue

            try:
                data = json.loads(raw)
                candidates = data if isinstance(data, list) else [data]

                for item in candidates:
                    if isinstance(item, dict):
                        if item.get("datePublished"):
                            return str(item["datePublished"]).strip()
                        if item.get("dateModified"):
                            return str(item["dateModified"]).strip()
            except Exception:
                continue

        # 4) Regex sobre todo el HTML
        html = str(soup)

        patterns = [
            r'"datePublished"\s*:\s*"([^"]+)"',
            r'"dateModified"\s*:\s*"([^"]+)"',
            r'"publish_date"\s*:\s*"([^"]+)"',
            r'"publishedAt"\s*:\s*"([^"]+)"',
        ]

        for pattern in patterns:
            match = re.search(pattern, html)
            if match:
                return match.group(1).strip()

    except Exception:
        pass

    return ""


def is_valid_investing_link(title, link):
    if not title or not link:
        return False

    link_lower = link.lower()
    title_lower = title.lower()

    banned_terms = [
        "/analysis/",
        "/brokers-news/",
        "/opinion/",
        "/technical/",
        "most-popular-news",
        "headlines",
        "newsletter",
        "login",
        "sign-up",
        "#"
    ]

    for term in banned_terms:
        if term in link_lower:
            return False

    if len(title.split()) < 4:
        return False

    if len(title) < 25:
        return False

    if title_lower in ["read more", "more", "news"]:
        return False

    if "investing.com/news/" not in link_lower:
        return False

    return True


def is_valid_article_content(content):
    if not content:
        return False

    content_lower = content.lower()

    banned_content_terms = [
        "risk disclosure",
        "all rights reserved",
        "sign up",
        "advertisement"
    ]

    for term in banned_content_terms:
        if term in content_lower:
            return False

    if len(content) < 250:
        return False

    return True


def extract_investing_article_text_and_date(url, scraper):
    try:
        response = scraper.get(url, headers=HEADERS, timeout=20)
        print("Status artículo:", response.status_code, "|", url)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")
        published_at = extract_published_at(soup)

        possible_classes = [
            "article_WYSIWYG__O0uhw",
            "WYSIWYG articlePage",
            "articlePage",
            "article-body",
            "article-content",
            "main-content",
            "textDiv",
            "mb-4",
            "entry-content",
            "post-content"
        ]

        for class_name in possible_classes:
            if " " in class_name:
                block = soup.find("div", class_=lambda c: c and class_name in c)
            else:
                block = soup.find("div", class_=class_name)

            if block:
                paragraphs = block.find_all("p")
                text = " ".join(p.get_text(" ", strip=True) for p in paragraphs)
                text = clean_text(text)

                if len(text) > 120:
                    return text[:2000], published_at

        paragraphs = soup.find_all("p")
        text = " ".join(p.get_text(" ", strip=True) for p in paragraphs)
        text = clean_text(text)

        if len(text) > 120:
            return text[:2000], published_at

        return "Contenido no encontrado", published_at

    except Exception as e:
        print("Error extrayendo artículo:", e)
        return "Error al obtener contenido", ""


def get_links_from_section(section_url, scraper):
    print(f"\nRevisando sección: {section_url}")

    response = scraper.get(section_url, headers=HEADERS, timeout=20)
    print("Status sección:", response.status_code)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    links = []
    seen_in_section = set()

    articles = soup.find_all("a", href=True)
    print("Cantidad de links encontrados en sección:", len(articles))

    for tag in articles:
        title = clean_text(tag.get_text(" ", strip=True))
        href = tag.get("href", "").strip()

        if not href:
            continue

        if href.startswith("/"):
            link = f"https://www.investing.com{href}"
        else:
            link = href

        if not is_valid_investing_link(title, link):
            continue

        if link in seen_in_section:
            continue

        seen_in_section.add(link)
        links.append((title, link))

    print("Links válidos encontrados en sección:", len(links))
    return links


def get_investing_news():
    scraper = cloudscraper.create_scraper(
        browser={
            "browser": "chrome",
            "platform": "windows",
            "mobile": False
        }
    )

    all_links = []
    global_seen_links = set()

    for section_url in URLS:
        try:
            section_links = get_links_from_section(section_url, scraper)
            for title, link in section_links:
                if link not in global_seen_links:
                    global_seen_links.add(link)
                    all_links.append((title, link))
        except Exception as e:
            print(f"Error revisando sección {section_url}: {e}")

    print(f"\nTotal links únicos recolectados de todas las secciones: {len(all_links)}")

    news_list = []
    revisados = 0

    for title, link in all_links:
        revisados += 1
        print(f"\nLINK CANDIDATO {revisados}")
        print("Título:", title)
        print("Link:", link)

        content, published_at = extract_investing_article_text_and_date(link, scraper)

        if not published_at:
            published_at = datetime.now().isoformat()

        print("Fecha detectada:", published_at)
        print("Primeros 120 caracteres del contenido:", content[:120])

        if content in ["Contenido no encontrado", "Error al obtener contenido"]:
            print("-> descartado por contenido vacío/error")
            continue

        if not is_valid_article_content(content):
            print("-> descartado por contenido inválido")
            continue

        news_item = build_news_item(
            title=title,
            link=link,
            content=content,
            source="Investing",
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

    news = get_investing_news()
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