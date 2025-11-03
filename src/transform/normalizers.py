# src/transform/normalizers.py
import time, html, re
from typing import Dict, Iterable, List, Optional
import pandas as pd
import numpy as np

USEFUL_COLS = [
    "id","subreddit","author","title","selftext","created_utc",
    "num_comments","score","upvote_ratio","url","permalink",
    "over_18","is_self","domain","link_flair_text","subreddit_subscribers"
]

def _clean_text(s: Optional[str]) -> Optional[str]:
    if s is None: return None
    s = html.unescape(s).replace("\r", " ").replace("\n", " ").strip()
    s = re.sub(r"\s+", " ", s)
    return s or None

def _to_int(x) -> Optional[int]:
    try:
        if x is None or (isinstance(x, float) and np.isnan(x)): return None
        return int(x)
    except Exception:
        return None

def _to_float(x) -> Optional[float]:
    try:
        if x is None or (isinstance(x, float) and np.isnan(x)): return None
        return float(x)
    except Exception:
        return None

def _bool(x) -> Optional[bool]:
    if isinstance(x, bool): return x
    if x in (0, 1): return bool(x)
    return None

def normalize_post_dict(d: Dict) -> Dict:
    rec = d.get("data", d)  # por si viene envuelto
    pid   = rec.get("id")
    sub   = rec.get("subreddit")
    auth  = rec.get("author")
    title = _clean_text(rec.get("title"))
    body  = _clean_text(rec.get("selftext"))

    created = _to_int(rec.get("created_utc"))
    ncom    = _to_int(rec.get("num_comments"))
    score   = _to_int(rec.get("score"))
    ratio   = _to_float(rec.get("upvote_ratio"))

    url = rec.get("url")
    pl  = rec.get("permalink") or ""
    if pl and not pl.startswith("http"):
        pl = "https://reddit.com" + pl

    row = {
        "id": pid if isinstance(pid, str) and pid else None,
        "subreddit": sub if isinstance(sub, str) and sub else None,
        "author": auth if isinstance(auth, str) and auth else None,
        "title": title,
        "selftext": body,
        "created_utc": created,
        "num_comments": ncom,
        "score": score,
        "upvote_ratio": ratio,
        "url": url,
        "permalink": pl or None,
        "over_18": _bool(rec.get("over_18")),
        "is_self": _bool(rec.get("is_self")),
        "domain": rec.get("domain"),
        "link_flair_text": rec.get("link_flair_text"),
        "subreddit_subscribers": _to_int(rec.get("subreddit_subscribers")),
        "retrieved_at": int(time.time()),
        "source": "reddit",
    }
    return row

def normalize_posts(items: Iterable[Dict]) -> pd.DataFrame:
    rows: List[Dict] = [normalize_post_dict(d) for d in items]
    df = pd.DataFrame(rows)

    if "id" in df.columns:
        df = df[df["id"].notna()].drop_duplicates(subset=["id"])

    base_cols = USEFUL_COLS + ["retrieved_at","source"]
    cols = [c for c in base_cols if c in df.columns]
    df = df.reindex(columns=cols)

    for c in ["created_utc","num_comments","score","subreddit_subscribers","retrieved_at"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce", downcast="integer")

    if "upvote_ratio" in df.columns:
        df["upvote_ratio"] = pd.to_numeric(df["upvote_ratio"], errors="coerce", downcast="float")

    for c in ["over_18","is_self"]:
        if c in df.columns:
            df[c] = df[c].astype("boolean")

    for c in ["subreddit","author","domain","link_flair_text","source"]:
        if c in df.columns:
            df[c] = df[c].astype("category")

    for c in ["title","selftext","url","permalink"]:
        if c in df.columns:
            df[c] = df[c].apply(lambda x: _clean_text(x) if isinstance(x, str) else x)

    return df
