# src/retry_failed.py
import os
import requests
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
CACHE_API_URL = os.environ.get("CACHE_API_URL")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# 失敗したURIを取得（古いものから）
res = supabase.table('uri_cache') \
    .select('uri, retry_count') \
    .eq('status', 'failed') \
    .lt('retry_count', 3) \
    .limit(50) \
    .execute()

if not res.data:
    print("No failed URIs")
    exit()

hex_uris = [item['uri'] for item in res.data]

# カウント増加
for item in res.data:
    supabase.table('uri_cache') \
        .update({'retry_count': item['retry_count'] + 1}) \
        .eq('uri', item['uri']) \
        .execute()

# Workerにバッチリクエスト → Queueに再投入される
r = requests.post(f"{CACHE_API_URL}/api/cache/batch", json={"hex_uris": hex_uris})
print(f"Re-queued {len(hex_uris)} items: {r.status_code}")