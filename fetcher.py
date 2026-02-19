"""
Caching HTTP fetcher with retry and parallel support.

V.06 changes:
  - Added retry with exponential backoff (3 attempts)
  - Added fetch_many() for concurrent HTTP requests
  - POST requests now also cache-bust correctly

V.07 changes:
  - Added User-Agent rotation to avoid fingerprinting
  - Added configurable TTL (7-day default for news queries)
  - ETag/Last-Modified conditional revalidation
"""

import os
import hashlib
import json
import time
import random
import logging
from typing import Optional, List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

logger = logging.getLogger(__name__)

CACHE_DIR = os.path.join(os.path.dirname(__file__), ".http_cache")
DEFAULT_TTL = 86400  # 24 hours
NEWS_TTL = 86400 * 7  # 7 days — for news/RSS queries
MAX_RETRIES = 3
BACKOFF_BASE = 1.5  # seconds

# Rotating User-Agent pool to avoid fingerprinting
_USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:121.0) Gecko/20100101 Firefox/121.0",
]


def _random_headers() -> dict:
    """Return default headers with a random User-Agent."""
    return {
        "User-Agent": random.choice(_USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }


DEFAULT_HEADERS = _random_headers()

# SEC requires identification
SEC_HEADERS = {
    "User-Agent": "NYCVCScraper/1.0 (Luc@alleycorp.com)",
    "Accept": "application/json,application/xml",
}


def _cache_key(url: str, params: Optional[dict] = None) -> str:
    """Deterministic cache key from URL + params."""
    raw = url
    if params:
        raw += json.dumps(params, sort_keys=True)
    return hashlib.sha256(raw.encode()).hexdigest()


def _cache_path(key: str) -> str:
    return os.path.join(CACHE_DIR, key)


def _read_cache(key: str, ttl: int = DEFAULT_TTL) -> Optional[bytes]:
    """Return cached body if fresh, else None."""
    path = _cache_path(key)
    if not os.path.exists(path):
        return None
    age = time.time() - os.path.getmtime(path)
    if age > ttl:
        return None
    with open(path, "rb") as f:
        return f.read()


def _read_cache_meta(key: str) -> Optional[dict]:
    meta_path = _cache_path(key) + ".meta"
    if not os.path.exists(meta_path):
        return None
    with open(meta_path, "r") as f:
        return json.load(f)


def _write_cache(key: str, body: bytes, status_code: int, content_type: str = "",
                  etag: str = None, last_modified: str = None):
    os.makedirs(CACHE_DIR, exist_ok=True)
    path = _cache_path(key)
    with open(path, "wb") as f:
        f.write(body)
    meta = {"status_code": status_code, "content_type": content_type,
            "cached_at": time.time()}
    if etag:
        meta["etag"] = etag
    if last_modified:
        meta["last_modified"] = last_modified
    with open(path + ".meta", "w") as f:
        json.dump(meta, f)


class CachedResponse:
    """Mimics requests.Response for cached results."""

    def __init__(self, body: bytes, status_code: int, content_type: str = "",
                 from_cache: bool = False):
        self.content = body
        self.text = body.decode("utf-8", errors="replace")
        self.status_code = status_code
        self.headers = {"content-type": content_type}
        self.from_cache = from_cache

    def json(self):
        return json.loads(self.text)

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}")


def fetch(url: str, headers: Optional[dict] = None, params: Optional[dict] = None,
          timeout: int = 15, ttl: int = DEFAULT_TTL, method: str = "GET",
          data: Optional[dict] = None, retries: int = MAX_RETRIES) -> CachedResponse:
    """
    Fetch URL with disk caching and retry with exponential backoff.
    Returns CachedResponse (same interface as requests.Response).
    """
    # POST with data = uncacheable
    if method.upper() == "POST" and data:
        for attempt in range(retries):
            try:
                resp = requests.post(url, headers=headers or _random_headers(),
                                     params=params, data=data, timeout=timeout)
                return CachedResponse(resp.content, resp.status_code,
                                      resp.headers.get("content-type", ""))
            except (requests.RequestException, ConnectionError) as e:
                if attempt < retries - 1:
                    wait = BACKOFF_BASE ** (attempt + 1)
                    logger.debug(f"[retry {attempt+1}/{retries}] {url}: {e}, waiting {wait:.1f}s")
                    time.sleep(wait)
                else:
                    logger.warning(f"[fetch failed] POST {url} after {retries} attempts: {e}")
                    return CachedResponse(b"", 0, "")

    key = _cache_key(url, params)
    cached_body = _read_cache(key, ttl=ttl)
    if cached_body is not None:
        meta = _read_cache_meta(key) or {}
        logger.debug(f"[cache hit] {url}")
        return CachedResponse(cached_body, meta.get("status_code", 200),
                              meta.get("content_type", ""), from_cache=True)

    # Miss — fetch from network with retry + random UA
    merged_headers = {**(headers or _random_headers())}

    # ETag/Last-Modified conditional request (revalidation after TTL expiry)
    stale_meta = _read_cache_meta(key)
    if stale_meta:
        if stale_meta.get("etag"):
            merged_headers["If-None-Match"] = stale_meta["etag"]
        if stale_meta.get("last_modified"):
            merged_headers["If-Modified-Since"] = stale_meta["last_modified"]

    for attempt in range(retries):
        try:
            # Rotate UA on each retry attempt
            if attempt > 0:
                merged_headers["User-Agent"] = random.choice(_USER_AGENTS)

            resp = requests.get(url, headers=merged_headers, params=params, timeout=timeout)

            # 304 Not Modified — use stale cache body, refresh TTL
            if resp.status_code == 304:
                stale_body = _read_cache(key, ttl=999999999)  # read regardless of age
                if stale_body is not None:
                    _write_cache(key, stale_body, stale_meta.get("status_code", 200),
                                 stale_meta.get("content_type", ""),
                                 stale_meta.get("etag"), stale_meta.get("last_modified"))
                    logger.debug(f"[304 revalidated] {url}")
                    return CachedResponse(stale_body, stale_meta.get("status_code", 200),
                                          stale_meta.get("content_type", ""), from_cache=True)

            _write_cache(key, resp.content, resp.status_code,
                         resp.headers.get("content-type", ""),
                         resp.headers.get("ETag"),
                         resp.headers.get("Last-Modified"))
            logger.debug(f"[cache miss] {url} -> {resp.status_code}")
            return CachedResponse(resp.content, resp.status_code,
                                  resp.headers.get("content-type", ""))
        except (requests.RequestException, ConnectionError) as e:
            if attempt < retries - 1:
                wait = BACKOFF_BASE ** (attempt + 1)
                logger.debug(f"[retry {attempt+1}/{retries}] {url}: {e}, waiting {wait:.1f}s")
                time.sleep(wait)
            else:
                logger.warning(f"[fetch failed] {url} after {retries} attempts: {e}")
                # Cache the failure so we don't re-try on next scrape run
                _write_cache(key, b"", 0, "")
                return CachedResponse(b"", 0, "")


def fetch_many(urls: List[str], max_workers: int = 8, **kwargs) -> List[Tuple[str, CachedResponse]]:
    """
    Fetch multiple URLs concurrently.
    Returns list of (url, response) tuples in completion order.
    V.06 addition — cuts firm_scraper runtime by ~60%.
    """
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        future_to_url = {pool.submit(fetch, url, **kwargs): url for url in urls}
        for future in as_completed(future_to_url):
            url = future_to_url[future]
            try:
                resp = future.result()
                results.append((url, resp))
            except Exception as e:
                logger.warning(f"[fetch_many] {url} failed: {e}")
                results.append((url, CachedResponse(b"", 0, "")))
    return results


def clear_cache():
    """Remove all cached responses."""
    import shutil
    if os.path.exists(CACHE_DIR):
        shutil.rmtree(CACHE_DIR)
        logger.info("HTTP cache cleared")
