import requests
from bs4 import BeautifulSoup
from app.services.news_utils import (
    HEADERS,
    clean_text,
    build_news_item,
    extract_article_text_generic
)

URL = "https://finance.yahoo.com/news/"
MAX_NEWS = 10

def is_valid_yahoo_news_url(url):
    if not url:
        return False

    url = url.strip()

    if url.startswith("/news/"):
        return True

    if url.startswith("https://finance.yahoo.com/news/"):
        return True

    return False

def normalize_yahoo_url(url):
    if url.startswith("/news/"):
        return f"https://finance.yahoo.com{url}"
    return url

def get_yahoo_news():
    response = requests.get(URL, headers=HEADERS, timeout=20)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    links = soup.find_all("a", href=True)

    news_list = []
    seen = set()

    for tag in links:
        title = clean_text(tag.get_text(" ", strip=True))
        href = tag.get("href", "").strip()

        if not title or len(title.split()) < 4:
            continue

        if title.lower() in ["news", "latest news", "newsletter", "newsletters"]:
            continue

        if not is_valid_yahoo_news_url(href):
            continue

        link = normalize_yahoo_url(href)

        if "sports.yahoo.com" in link or "health.yahoo.com" in link:
            continue

        if link.rstrip("/") == "https://finance.yahoo.com/news":
            continue

        if link in seen:
            continue
        seen.add(link)

        content = extract_article_text_generic(link)
        if content in ["Contenido no encontrado", "Error al obtener contenido"]:
            continue

        news_list.append(build_news_item(title, link, content, "Yahoo Finance"))

        if len(news_list) >= MAX_NEWS:
            break

    return news_list


if __name__ == "__main__":
    news = get_yahoo_news()
    for i, item in enumerate(news, 1):
        print(f"\nNoticia {i}")
        print("Título:", item["title"])
        print("Fuente:", item["source"])
        print("Impacto:", item["impact_general"])
        print("Link:", item["link"])
        print("Resumen:", item["content"][:300])
        print("-" * 100)