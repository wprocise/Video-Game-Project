import os, time, json
from pathlib import Path
from datetime import datetime
import requests
import pandas as pd

TOKEN_URL = "https://id.twitch.tv/oauth2/token"
IGDB_BASE = "https://api.igdb.com/v4"

CLIENT_ID = os.getenv("TWITCH_CLIENT_ID")
CLIENT_SECRET = os.getenv("TWITCH_CLIENT_SECRET")
if not CLIENT_ID or not CLIENT_SECRET:
    raise SystemExit("Set TWITCH_CLIENT_ID and TWITCH_CLIENT_SECRET env vars first.")

def get_app_access_token():
    """Client Credentials flow to get an app access token."""
    resp = requests.post(
        TOKEN_URL,
        params={
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "grant_type": "client_credentials",
        },
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    return data["access_token"]  # expires in ~60 days per docs

def igdb_query_all(endpoint, fields, where=None, sort="id asc",
                   out_csv="out.csv", max_rows=None, sleep_between=0.35):
    """
    Pulls all rows from an IGDB endpoint with pagination and writes to CSV.
    Arrays are flattened to pipe-separated strings.
    """
    token = get_app_access_token()
    headers = {
        "Client-ID": CLIENT_ID,
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }

    limit = 500   # IGDB max per request
    offset = 0
    total = 0
    out_path = Path(out_csv)
    if out_path.exists():
        out_path.unlink()

    while True:
        body = f"fields {fields};"
        if where:
            body += f" where {where};"
        body += f" sort {sort}; limit {limit}; offset {offset};"

        r = requests.post(f"{IGDB_BASE}/{endpoint}",
                          headers=headers, data=body.encode("utf-8"), timeout=90)

        if r.status_code == 429:           # rate limit – back off and retry
            time.sleep(1.0)
            continue
        r.raise_for_status()

        batch = r.json()
        if not batch:
            break

        # Flatten lists and nested dicts for CSV-friendliness
        for row in batch:
            for k, v in list(row.items()):
                if isinstance(v, list):
                    row[k] = "|".join(str(x) for x in v)
                elif isinstance(v, dict):
                    row[k] = json.dumps(v, separators=(",", ":"))

        df = pd.DataFrame(batch)
        df.to_csv(out_path, index=False, mode="a", header=not out_path.exists())

        got = len(batch)
        total += got
        offset += limit

        if max_rows and total >= max_rows:
            break

        # Stay under 4 req/sec
        time.sleep(sleep_between)

    print(f"[{endpoint}] wrote {total} rows → {out_path}")

def main():
    ts = datetime.now().strftime("%Y%m%d_%H%M")
    outdir = Path("igdb_csv")
    outdir.mkdir(exist_ok=True)

    # Choose concise, analysis-friendly fields for CSVs.
    # You can broaden these (e.g., "fields *;") but CSVs will get very large/noisy.
    tables = {
        "games": {
            "fields": (
                "id,name,slug,first_release_date,updated_at,"
                "total_rating,total_rating_count,aggregated_rating,aggregated_rating_count,"
                "follows,hypes,platforms,genres,status"
            ),
            "filename": outdir / f"games_{ts}.csv",
            # games is huge; start with a cap to sanity-check. Set to None to pull *everything*.
            "max_rows": 10000
        },
        "game_time_to_beats": {
            "fields": "id,game_id,normally,hastily,completely,count,created_at,updated_at",
            "filename": outdir / f"game_time_to_beats_{ts}.csv",
            "max_rows": None
        },
        "popularity_primitives": {
            # Use external_popularity_source (popularity_source is deprecated)
            "fields": "id,game_id,external_popularity_source,popularity_type,value,calculated_at,updated_at",
            "filename": outdir / f"popularity_primitives_{ts}.csv",
            "max_rows": None
        },
        "popularity_types": {
            "fields": "id,name,external_popularity_source,created_at,updated_at",
            "filename": outdir / f"popularity_types_{ts}.csv",
            "max_rows": None
        },
    }

    # Fetch each table
    igdb_query_all("games",
                   fields=tables["games"]["fields"],
                   out_csv=str(tables["games"]["filename"]),
                   max_rows=tables["games"]["max_rows"])

    igdb_query_all("game_time_to_beats",
                   fields=tables["game_time_to_beats"]["fields"],
                   out_csv=str(tables["game_time_to_beats"]["filename"]),
                   max_rows=tables["game_time_to_beats"]["max_rows"])

    igdb_query_all("popularity_primitives",
                   fields=tables["popularity_primitives"]["fields"],
                   out_csv=str(tables["popularity_primitives"]["filename"]),
                   max_rows=tables["popularity_primitives"]["max_rows"])

    igdb_query_all("popularity_types",
                   fields=tables["popularity_types"]["fields"],
                   out_csv=str(tables["popularity_types"]["filename"]),
                   max_rows=tables["popularity_types"]["max_rows"])

if __name__ == "__main__":
    main()
