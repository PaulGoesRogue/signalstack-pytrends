import os
import time
import random
from typing import List, Optional, Dict, Any

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from pytrends.request import TrendReq


app = FastAPI()

# CORS (safe for internal microservice usage; lock down if you want)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---- Config via env ----
HL = os.getenv("PYTRENDS_HL", "en-US")
TZ = int(os.getenv("PYTRENDS_TZ", "0"))

CACHE_TTL_SECONDS = int(os.getenv("PYTRENDS_CACHE_TTL", "900"))  # 15 min default
MAX_RETRIES = int(os.getenv("PYTRENDS_MAX_RETRIES", "3"))
RETRY_BASE_DELAY_SECONDS = float(os.getenv("PYTRENDS_RETRY_BASE_DELAY", "1.2"))
RETRY_JITTER_SECONDS = float(os.getenv("PYTRENDS_RETRY_JITTER", "0.6"))

# Optional proxy (only if you set it)
# Example: http://user:pass@proxyhost:port
PROXY = os.getenv("PYTRENDS_PROXY", "").strip() or None


# ---- Tiny in-memory cache ----
_CACHE: Dict[str, Dict[str, Any]] = {}


def _cache_get(key: str):
    entry = _CACHE.get(key)
    if not entry:
        return None
    if time.time() > entry["expires_at"]:
        _CACHE.pop(key, None)
        return None
    return entry["value"]


def _cache_set(key: str, value):
    _CACHE[key] = {"value": value, "expires_at": time.time() + CACHE_TTL_SECONDS}


def _sleep_backoff(attempt: int):
    # exponential-ish backoff with jitter
    delay = (RETRY_BASE_DELAY_SECONDS * (2 ** attempt)) + random.random() * RETRY_JITTER_SECONDS
    time.sleep(delay)


def _mk_pytrends() -> TrendReq:
    """
    Create a fresh TrendReq instance.
    Creating fresh instances reduces the chance of stale cookies/sessions causing failures.
    """
    # A slightly more "real browser-like" UA reduces some blocks.
    # pytrends doesn't always need it, but it helps in the wild.
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        )
    }

    if PROXY:
        # pytrends expects a list for proxies
        return TrendReq(hl=HL, tz=TZ, proxies=[PROXY], retries=0, backoff_factor=0, requests_args={"headers": headers})

    return TrendReq(hl=HL, tz=TZ, retries=0, backoff_factor=0, requests_args={"headers": headers})


def _run_with_retries(op_name: str, fn):
    """
    Wrap pytrends calls so a single failure doesn't 500 the whole service without context.
    """
    last_err = None
    for attempt in range(MAX_RETRIES):
        try:
            return fn()
        except Exception as e:
            last_err = e
            # Avoid hammering Google
            if attempt < MAX_RETRIES - 1:
                _sleep_backoff(attempt)
            continue
    # If we get here, all retries failed
    raise RuntimeError(f"{op_name} failed after {MAX_RETRIES} retries: {repr(last_err)}")


@app.get("/health")
def health():
    return {
        "ok": True,
        "service": "pytrends",
        "hl": HL,
        "tz": TZ,
        "cache_ttl_seconds": CACHE_TTL_SECONDS,
        "max_retries": MAX_RETRIES,
        "proxy_enabled": bool(PROXY),
    }


@app.get("/trends/trending")
def trending(
    pn: str = Query("united_states", description="pytrends trending_searches pn value"),
):
    cache_key = f"trending:{pn}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return {**cached, "cached": True}

    def _op():
        py = _mk_pytrends()
        df = py.trending_searches(pn=pn)
        topics: List[str] = []
        if df is not None and len(df.columns) > 0:
            col = df.columns[0]
            topics = [str(x) for x in df[col].tolist()][:50]
        return {"pn": pn, "topics": topics}

    try:
        out = _run_with_retries("trending_searches", _op)
        _cache_set(cache_key, out)
        return {**out, "cached": False}
    except Exception as e:
        # Don't crash callers with HTML stack traces; return structured error.
        return {
            "pn": pn,
            "topics": [],
            "error": "PYTRENDS_FAILED",
            "message": str(e),
        }


@app.get("/trends/interest")
def interest(
    keywords: str = Query(..., description="Comma-separated keywords (max 5)"),
    geo: str = Query("US"),
    timeframe: str = Query("now 7-d"),
):
    # Normalize
    kw_list = [k.strip() for k in keywords.split(",") if k.strip()][:5]
    cache_key = f"interest:{','.join(kw_list)}:{geo}:{timeframe}"

    cached = _cache_get(cache_key)
    if cached is not None:
        return {**cached, "cached": True}

    if not kw_list:
        return {"keywords": [], "geo": geo, "timeframe": timeframe, "data": [], "cached": False}

    def _op():
        py = _mk_pytrends()
        py.build_payload(kw_list, cat=0, timeframe=timeframe, geo=geo, gprop="")
        df = py.interest_over_time()

        series = []
        if df is not None and len(df.index) > 0:
            primary = kw_list[0]
            for idx, row in df.iterrows():
                try:
                    ts = idx.isoformat()
                except Exception:
                    ts = str(idx)

                val = None
                try:
                    # pandas row access
                    val = int(row.get(primary))
                except Exception:
                    try:
                        val = int(row[primary])
                    except Exception:
                        val = None

                series.append({"time": ts, "value": val})

        return {"keywords": kw_list, "geo": geo, "timeframe": timeframe, "data": series}

    try:
        out = _run_with_retries("interest_over_time", _op)
        _cache_set(cache_key, out)
        return {**out, "cached": False}
    except Exception as e:
        return {
            "keywords": kw_list,
            "geo": geo,
            "timeframe": timeframe,
            "data": [],
            "error": "PYTRENDS_FAILED",
            "message": str(e),
            "cached": False,
        }


@app.get("/trends/related")
def related(
    keyword: str = Query(...),
    geo: str = Query("US"),
    timeframe: str = Query("now 7-d"),
):
    keyword = (keyword or "").strip()
    cache_key = f"related:{keyword}:{geo}:{timeframe}"

    cached = _cache_get(cache_key)
    if cached is not None:
        return {**cached, "cached": True}

    if not keyword:
        return {"keyword": "", "geo": geo, "timeframe": timeframe, "top": [], "rising": [], "cached": False}

    def _op():
        py = _mk_pytrends()
        py.build_payload([keyword], cat=0, timeframe=timeframe, geo=geo, gprop="")
        data = py.related_queries() or {}

        top_rows = []
        rising_rows = []

        try:
            top_df = data.get(keyword, {}).get("top")
            if top_df is not None:
                top_rows = top_df[["query", "value"]].head(25).to_dict(orient="records")
        except Exception:
            top_rows = []

        try:
            rising_df = data.get(keyword, {}).get("rising")
            if rising_df is not None:
                rising_rows = rising_df[["query", "value"]].head(25).to_dict(orient="records")
        except Exception:
            rising_rows = []

        return {"keyword": keyword, "geo": geo, "timeframe": timeframe, "top": top_rows, "rising": rising_rows}

    try:
        out = _run_with_retries("related_queries", _op)
        _cache_set(cache_key, out)
        return {**out, "cached": False}
    except Exception as e:
        return {
            "keyword": keyword,
            "geo": geo,
            "timeframe": timeframe,
            "top": [],
            "rising": [],
            "error": "PYTRENDS_FAILED",
            "message": str(e),
            "cached": False,
        }
