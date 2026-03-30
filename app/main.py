from app.db.connection import create_table
from app.services.news_utils import save_news_to_db

from app.collectors.valora_collector import get_valora_news
from app.collectors.investing_collector import get_investing_news
from app.collectors.bloomberg_collector import get_bloomberg_news


def safe_run(name, func):
    try:
        print(f"\n===== Ejecutando {name} =====")
        news = func()
        print(f"[OK] {name}: {len(news)} noticias obtenidas")
        return news
    except Exception as e:
        print(f"[ERROR] {name}: {e}")
        return []


if __name__ == "__main__":
    create_table()

    all_news = []

    all_news.extend(safe_run("Valora Analitik", get_valora_news))
    all_news.extend(safe_run("Investing", get_investing_news))
    all_news.extend(safe_run("Bloomberg", get_bloomberg_news))

    save_news_to_db(all_news)

    print("\n" + "=" * 60)
    print(f"Total noticias guardadas: {len(all_news)}")
    print("=" * 60)

    for i, item in enumerate(all_news, 1):
        print(f"\nNoticia {i}")
        print("Título:", item["title"])
        print("Fuente:", item["source"])
        print("Impacto:", item["impact_general"])
        print("Link:", item["link"])
        print("Resumen:", item["content"][:250])
        print("-" * 100)