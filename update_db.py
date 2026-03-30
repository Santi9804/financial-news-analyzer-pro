import sqlite3

DB_PATH = "news.db"

def add_published_at_column():
    print("🚀 Iniciando actualización de base de datos...")

    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        print("📌 Revisando columnas existentes...")

        cursor.execute("PRAGMA table_info(news)")
        columns = cursor.fetchall()

        column_names = [col[1] for col in columns]

        print("Columnas actuales:", column_names)

        if "published_at" not in column_names:
            cursor.execute("ALTER TABLE news ADD COLUMN published_at TEXT")
            print("✅ Columna 'published_at' agregada correctamente.")
        else:
            print("ℹ️ La columna 'published_at' ya existe.")

        conn.commit()
        conn.close()

        print("🎉 Proceso terminado.")

    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    add_published_at_column()