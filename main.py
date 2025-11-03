from src.settings import settings
from src.clients import RedditClient


# Instancia
rc = RedditClient(
    client_id=settings.reddit_client_id,
    client_secret=settings.reddit_client_secret,
    username="efesan615",
)

# Últimos posts de r/datascience
rows = []
for d in rc.subreddit_new("datascience", limit=100, max_items=300):
    rows.append({
        "id": d.get("id"),
        "title": d.get("title"),
        "created_utc": d.get("created_utc"),
        "score": d.get("score"),
        "num_comments": d.get("num_comments"),
        "permalink": "https://reddit.com" + d.get("permalink", "")
    })

print(f"Recogidos: {len(rows)}")

# Búsqueda global
for d in rc.search("ecommerce marketing", sort="new", limit=100, max_items=200):
    print(d.get("title"))
