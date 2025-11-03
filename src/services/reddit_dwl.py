from src.clients import RedditClient
from src.extract import RedditData
from src.settings import settings
from src.storage import ParquetStorage  # si tienes DuckDBIndex, puedes importarlo también

import pandas as pd

# ---------- 1) Inicialización clientes ----------
rc = RedditClient(
    client_id=settings.reddit_client_id,
    client_secret=settings.reddit_client_secret,
    username=settings.reddit_username,
)
rd = RedditData(rc)

# ---------- 2) Descargas (DataFrames limpios) ----------
#df_new = rd.subreddit_new_df("sneakers", limit=100, max_items=300)
#df_top = rd.subreddit_top_df("sneakers", t="week", max_items=200)
df_q   = rd.search_df("sneakers nike air max", sort="new", max_items=400)

print("Shapes:", df_new.shape, df_top.shape, df_q.shape)
print(df_new.dtypes)

# ---------- 4) Almacenamiento Parquet (ZSTD) ----------
# Estructura de carpetas:
# data/curated/reddit/posts/posts_YYYYMMDD_HHMMSS.parquet
posts_storage = ParquetStorage(base_dir=settings.data_storage_path + "reddit", dataset="posts")

def save_nonempty(df: pd.DataFrame, storage: ParquetStorage, suffix: str) -> str | None:
    if df is None or df.empty:
        print(f"⚠️  {suffix}: DataFrame vacío; no se guarda.")
        return None
    path = storage.save_df(df, suffix=suffix)  # ZSTD, diccionario, etc. según ParquetStorage
    print(f"✅ Guardado {suffix}: {path}  ({len(df)} filas)")
    return path
x=1
p1 = save_nonempty(df_new, posts_storage, suffix="subreddit_new_sneakers")
p2 = save_nonempty(df_top, posts_storage, suffix="subreddit_top_sneakers")
p3 = save_nonempty(df_q,   posts_storage, suffix="sneakers nike air max")

# ---------- 5) (Opcional) Crear vistas DuckDB que lean todos los Parquet sin importar ----------
# from src.storage import DuckDBIndex
# index = DuckDBIndex(db_path="data/reddit.duckdb", base_dir="data/curated/reddit")
# index.create_view_for_dataset("posts")
# q = index.query("""
#   SELECT subreddit, COUNT(*) AS n
#   FROM vw_posts
#   WHERE created_utc >= (extract(epoch from now()) - 30*24*3600)
#   GROUP BY subreddit ORDER BY n DESC
# """)
# print(q.head())
