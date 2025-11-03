# src/extract/reddit_wrappers.py
from typing import Optional, Dict, Iterable
import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd

from src.transform.normalizers import normalize_posts


class RedditClient:
    """
    Cliente simple para la API OAuth de Reddit (app-only) con dos capas de uso:
      1) Métodos 'crudos' que devuelven un iterable de dicts (posts).
      2) Métodos *_df que devuelven DataFrames normalizados y listos para análisis.

    - Gestiona sesión, token y reintentos.
    - Helpers para listings con paginación (after).
    """

    AUTH_URL = "https://www.reddit.com/api/v1/access_token"
    API_BASE = "https://oauth.reddit.com"

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        username: str,
        user_agent_prefix: str = "TFM-analytics/1.0",
        timeout: int = 20,
        max_retries: int = 3,
    ):
        self.client_id = client_id
        self.client_secret = client_secret
        self.user_agent = f"{user_agent_prefix} by u/{username}"
        self.timeout = timeout

        # Session + retries (5xx/429 con backoff)
        self.s = requests.Session()
        self.s.headers.update({"User-Agent": self.user_agent})
        retry = Retry(
            total=max_retries,
            backoff_factor=0.8,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(["GET", "POST"]),
        )
        self.s.mount("https://", HTTPAdapter(max_retries=retry))

        self._token: Optional[str] = None
        self._token_expiry_ts: float = 0.0
        self._authenticate()

    # ---------------------- Auth ----------------------

    def _authenticate(self) -> None:
        data = {"grant_type": "client_credentials"}
        auth = requests.auth.HTTPBasicAuth(self.client_id, self.client_secret)
        r = self.s.post(self.AUTH_URL, data=data, auth=auth, timeout=self.timeout)
        r.raise_for_status()
        payload = r.json()

        self._token = payload["access_token"]
        expires_in = int(payload.get("expires_in", 3600))
        # Renueva algo antes de la expiración real
        self._token_expiry_ts = time.time() + expires_in * 0.9

        # Añade header Authorization a la sesión
        self.s.headers.update({"Authorization": f"bearer {self._token}"})

    def _ensure_token(self) -> None:
        if not self._token or time.time() >= self._token_expiry_ts:
            self._authenticate()

    # ---------------------- Core request ----------------------

    def _request(self, method: str, path: str, params: Optional[Dict] = None) -> Dict:
        """
        method: 'GET' | 'POST'
        path: '/r/{sub}/new' o 'search' (se resuelve contra API_BASE)
        """
        self._ensure_token()

        url = path if path.startswith("http") else f"{self.API_BASE}/{path.lstrip('/')}"
        r = self.s.request(method, url, params=params, timeout=self.timeout)

        # Gestiona 429 con espera según cabeceras si no lo absorbió Retry
        if r.status_code == 429:
            reset = r.headers.get("x-ratelimit-reset")
            if reset:
                wait = max(1, int(float(reset)))
                time.sleep(wait)
                r = self.s.request(method, url, params=params, timeout=self.timeout)

        r.raise_for_status()
        try:
            return r.json()
        except ValueError as e:
            raise RuntimeError(f"Respuesta no JSON desde {url}") from e

    # ---------------------- Listings helpers ----------------------

    def listing(
        self,
        path: str,
        limit: int = 100,
        max_items: int = 1000,
        extra_params: Optional[Dict] = None,
    ) -> Iterable[Dict]:
        """
        Itera sobre un listing (children) usando paginación via 'after'.
        """
        params = {"limit": min(limit, 100)}
        if extra_params:
            params.update(extra_params)

        fetched = 0
        after = None

        while True:
            if after:
                params["after"] = after

            payload = self._request("GET", path, params=params)
            data = payload.get("data", {})
            children = data.get("children", [])
            for ch in children:
                yield ch.get("data", {})
                fetched += 1
                if fetched >= max_items:
                    return

            after = data.get("after")
            if not after or not children:
                return

            # respetar rate limiting suave
            self._sleep_respecting_limits(payload)

    def _sleep_respecting_limits(self, _response_json: Dict) -> None:
        # Si la API devolviera headers de x-ratelimit en JSON (no siempre),
        # usa un sleep fijo pequeño para ser “nice”.
        time.sleep(0.6)

    # ---------------------- Métodos crudos ----------------------

    def subreddit_new(self, subreddit: str, **kwargs) -> Iterable[Dict]:
        return self.listing(f"/r/{subreddit}/new", **kwargs)

    def subreddit_top(self, subreddit: str, t: str = "day", **kwargs) -> Iterable[Dict]:
        params = {"t": t}
        return self.listing(f"/r/{subreddit}/top", extra_params=params, **kwargs)

    def search(
        self,
        query: str,
        sort: str = "relevance",
        t: str = "all",
        restrict_sr: bool = False,
        subreddit: Optional[str] = None,
        include_over_18: Optional[bool] = None,
        **kwargs,
    ) -> Iterable[Dict]:
        """
        Retorna un iterable de dicts (posts). Para DataFrame usar search_df().
        """
        params = {
            "q": query,
            "sort": sort,              # 'relevance' | 'new' | 'top' | 'comments'
            "t": t,                    # 'hour'|'day'|'week'|'month'|'year'|'all'
            "type": "link",            # Solo posts (no comentarios)
            "raw_json": 1,
            "limit": min(kwargs.get("limit", 100), 100),
        }

        if include_over_18 is not None:
            params["include_over_18"] = "on" if include_over_18 else "off"

        path = "/search"
        if restrict_sr and subreddit:
            params["restrict_sr"] = 1
            path = f"/r/{subreddit}/search"

        # Filtra kwargs de listing para evitar pasar 'limit' duplicado
        listing_kwargs = {k: v for k, v in kwargs.items() if k not in ("limit",)}
        return self.listing(path, extra_params=params, **listing_kwargs)

    # ---------------------- Capa DataFrame ----------------------

    def _collect(self, it: Iterable[Dict]) -> pd.DataFrame:
        """
        Normaliza una lista de posts (dicts) a DataFrame optimizado.
        """
        return normalize_posts(list(it))

    def subreddit_new_df(self, subreddit: str, **kwargs) -> pd.DataFrame:
        return self._collect(self.subreddit_new(subreddit, **kwargs))

    def subreddit_top_df(self, subreddit: str, t: str = "day", **kwargs) -> pd.DataFrame:
        return self._collect(self.subreddit_top(subreddit, t=t, **kwargs))

    def search_df(
        self,
        query: str,
        sort: str = "new",
        restrict_sr: bool = False,
        subreddit: Optional[str] = None,
        **kwargs,
    ) -> pd.DataFrame:
        """
        Capa conveniente que retorna un DataFrame ya normalizado.
        """
        return self._collect(
            self.search(
                query=query,
                sort=sort,
                restrict_sr=restrict_sr,
                subreddit=subreddit,
                **kwargs,
            )
        )
