import time
from typing import Dict, Iterable, List, Optional
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

class RedditClient:
    """
    Cliente simple para la API OAuth de Reddit (app-only).
    - Gestiona sesión, token y reintentos.
    - Incluye helpers para listings con paginación (after).
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
            allowed_methods=frozenset(["GET", "POST"])
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
        self._token_expiry_ts = time.time() + expires_in * 0.9  # renueva un poco antes

        # añade header Authorization a la sesión
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
        self, path: str, limit: int = 100, max_items: int = 1000, extra_params: Optional[Dict] = None
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

    def _sleep_respecting_limits(self, response_json: Dict) -> None:
        # Si la API devolviera headers de x-ratelimit en JSON (no siempre),
        # o usa un sleep fijo pequeño para ser “nice”.
        time.sleep(0.6)

    # ---------------------- Convenience methods ----------------------

    def subreddit_new(self, subreddit: str, **kwargs) -> Iterable[Dict]:
        return self.listing(f"/r/{subreddit}/new", **kwargs)

    def subreddit_top(self, subreddit: str, t: str = "day", **kwargs) -> Iterable[Dict]:
        params = {"t": t}
        return self.listing(f"/r/{subreddit}/top", extra_params=params, **kwargs)

    def search(self, query: str, sort: str = "new", restrict_sr: bool = False,
               subreddit: Optional[str] = None, **kwargs) -> Iterable[Dict]:
        params = {"q": query, "sort": sort}
        path = "/search"
        if restrict_sr and subreddit:
            params["restrict_sr"] = 1
            path = f"/r/{subreddit}/search"
        return self.listing(path, extra_params=params, **kwargs)
