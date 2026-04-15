import os
import time
from typing import Optional

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from pytrends.request import TrendReq

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

HL = os.getenv("PYTRENDS_HL", "en-US")
TZ = int(os.getenv("PYTRENDS_TZ", "0"))

_CACHE = {}
_TTL_SECONDS = int(os.getenv("PYTRENDS_CACHE_TTL", "900"))

def _cache_get(key: str):
    entry = _CACHE.get(key)
    if not entry:
        return None
    if time.time() > entry["expires_at"]:
        _CACHE.pop(key, None)
        return None
    return entry["value"]

def _cache_set(key: str, value):
    _CACHE[key] = {
        "value": value,
        "expires_at": time.time() + _TTL_SECONDS,
    }

def _trends():
    return TrendReq(hl=HL, tz=TZ)

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/trends/trending")
def trending(pn: str = Query("united_states")):
    cache_key = f"trending:{pn}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    py = _trends()
    df = py.trending_searches(pn=pn)
    topics = []
    if df is not None and len(df.columns) > 0:
        col = df.columns[0]
        topics = [str(x) for x in df[col].tolist()][:50]

    out = {"pn": pn, "topics": topics}
    _cache_set(cache_key, out)
    return out

@app.get("/trends/interest")
def interest(
    keywords: str = Query(..., description="Comma-separated keywords"),
    geo: str = Query("US"),
    timeframe: str = Query("now 7-d"),
):
    cache_key = f"interest:{keywords}:{geo}:{timeframe}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    kw_list = [k.strip() for k in keywords.split(",") if k.strip()][:5]
    if not kw_list:
        return {"keywords": [], "geo": geo, "timeframe": timeframe, "data": []}

    py = _trends()
    py.build_payload(kw_list, cat=0, timeframe=timeframe, geo=geo, gprop="")
    df = py.interest_over_time()

    series = []
    if df is not None and len(df.index) > 0:
        first = kw_list[0]
        for idx, row in df.iterrows():
            try:
                ts = idx.isoformat()
            except Exception:
                ts = str(idx)
            val = None
            try:
                val = int(row.get(first))
            except Exception:
                pass
            series.append({"time": ts, "value": val})

    out = {
        "keywords": kw_list,
        "geo": geo,
        "timeframe": timeframe,
        "data": series,
    }
    _cache_set(cache_key, out)
    return out

@app.get("/trends/related")
def related(
    keyword: str = Query(...),
    geo: str = Query("US"),
    timeframe: str = Query("now 7-d"),
):
    cache_key = f"related:{keyword}:{geo}:{timeframe}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    py = _trends()
    py.build_payload([keyword], cat=0, timeframe=timeframe, geo=geo, gprop="")
    data = py.related_queries() or {}

    top_rows = []
    rising_rows = []

    try:
        top_df = data.get(keyword, {}).get("top")
        if top_df is not None:
            top_rows = (
                top_df[["query", "value"]]
                .head(25)
                .to_dict(orient="records")
            )
    except Exception:
        top_rows = []

    try:
        rising_df = data.get(keyword, {}).get("rising")
        if rising_df is not None:
            rising_rows = (
                rising_df[["query", "value"]]
                .head(25)
                .to_dict(orient="records")
            )
    except Exception:
        rising_rows = []

    out = {"keyword": keyword, "geo": geo, "timeframe": timeframe, "top": top_rows, "rising": rising_rows}
    _cache_set(cache_key, out)
    return out
