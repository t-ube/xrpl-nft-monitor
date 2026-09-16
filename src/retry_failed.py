# src/retry_failed.py
import os
import requests
from supabase import create_client
from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta

load_dotenv()

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
CACHE_API_URL = os.environ.get("CACHE_API_URL")

RETRY_INTERVAL = timedelta(minutes=2)
MAX_RETRY = 3
BATCH_SIZE = 10

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

now_dt = datetime.now(timezone.utc)
now = now_dt.isoformat()

rows = supabase.rpc('pick_retry_targets', {
    'p_now': now,
    'p_limit': BATCH_SIZE,
}).execute().data

if not rows:
    print("No failed URIs ready for retry")
    exit()

hex_uris = [item['uri'] for item in rows]

for item in rows:
    supabase.table('uri_cache') \
        .update({
            'retry_count': item['retry_count'] + 1,
            'last_retry_at': now,
        }) \
        .eq('uri', item['uri']) \
        .execute()

# Workerにバッチリクエスト → Queueに再投入される
r = requests.post(f"{CACHE_API_URL}/api/cache/batch", json={"hex_uris": hex_uris})
print(f"Re-queued {len(hex_uris)} items: {r.status_code}")