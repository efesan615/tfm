# src/extract/reddit_wrappers.py
from typing import Optional, Dict, Iterable, List
import pandas as pd

from src.clients import RedditClient
from src.transform.normalizers import normalize_posts

class RedditData:
    """
    Capa de conveniencia sobre RedditClient que ya devuelve DataFrames limpios.
    """
    def __init__(self, client: RedditClient):
        self.client = client

    def _collect(self, it: Iterable[Dict]) -> pd.DataFrame:
        # it = generador de dicts (children[].data) del cliente
        # los normalizamos y devolvemos DF optimizado
        return normalize_posts(list(it))

    def subreddit_new_df(self, subreddit: str, **kwargs) -> pd.DataFrame:
        return self._collect(self.client.subreddit_new(subreddit, **kwargs))

    def subreddit_top_df(self, subreddit: str, t: str = "day", **kwargs) -> pd.DataFrame:
        return self._collect(self.client.subreddit_top(subreddit, t=t, **kwargs))

    def search_df(self, query: str, sort: str = "new",
                  restrict_sr: bool = False, subreddit: Optional[str] = None, **kwargs) -> pd.DataFrame:
        return self._collect(
            self.client.search(query=query, sort=sort, restrict_sr=restrict_sr, subreddit=subreddit, **kwargs)
        )
