import sqlite3


DB_NAME = "news.db"


def get_connection():
    conn = sqlite3.connect(DB_NAME)
    return conn


def create_table():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS news (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT,
            link TEXT UNIQUE,
            content TEXT,
            source TEXT,
            published_at TEXT,

            companies TEXT,
            commodities TEXT,
            raw_materials TEXT,
            indices TEXT,
            currencies TEXT,
            sectors TEXT,

            impact_general TEXT,
            impact_score_general REAL,

            impact_by_company TEXT,
            impact_by_commodity TEXT,
            impact_by_raw_material TEXT,
            impact_by_index TEXT,
            impact_by_currency TEXT,
            impact_by_sector TEXT,

            company_to_indices TEXT
        )
    """)

    ensure_column_exists(cursor, "news", "published_at", "TEXT")
    ensure_column_exists(cursor, "news", "impact_score_general", "REAL")
    ensure_column_exists(cursor, "news", "company_to_indices", "TEXT")

    conn.commit()
    conn.close()


def ensure_column_exists(cursor, table_name, column_name, column_type):
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = [row[1] for row in cursor.fetchall()]

    if column_name not in columns:
        cursor.execute(
            f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}"
        )