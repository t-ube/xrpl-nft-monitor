# src/convert_gifs.py
import urllib.request
import subprocess
import os
import boto3
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

def ipfs_to_http(url):
    if url.startswith('ipfs://'):
        return 'https://cloudflare-ipfs.com/ipfs/' + url[7:]
    if url.startswith('ar://'):
        return 'https://arweave.net/' + url[5:]
    return url

# 環境変数
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
R2_ENDPOINT = os.environ.get("R2_ENDPOINT")
R2_ACCESS_KEY = os.environ.get("R2_ACCESS_KEY")
R2_SECRET_KEY = os.environ.get("R2_SECRET_KEY")
R2_BUCKET = os.environ.get("R2_BUCKET")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

s3 = boto3.client('s3',
    endpoint_url=R2_ENDPOINT,
    aws_access_key_id=R2_ACCESS_KEY,
    aws_secret_access_key=R2_SECRET_KEY,
)

res = supabase.table('uri_cache') \
    .select('uri, image_url, thumbnail_r2_key') \
    .like('image_url', '%.gif') \
    .not_.is_('thumbnail_r2_key', 'null') \
    .is_('video_r2_key', 'null') \
    .limit(20) \
    .execute()

for item in res.data or []:
    input_path = '/tmp/input.gif'
    output_path = '/tmp/output.mp4'

    try:
        # ダウンロード
        download_url = ipfs_to_http(item['image_url'])
        urllib.request.urlretrieve(download_url, input_path)

        # 変換
        subprocess.run([
            'ffmpeg', '-y', '-i', input_path,
            '-movflags', 'faststart',
            '-pix_fmt', 'yuv420p',
            '-vf', 'scale=trunc(iw/2)*2:trunc(ih/2)*2',
            '-c:v', 'libx264', '-crf', '23',
            output_path
        ], check=True)

        # R2アップロード
        mp4_key = item['uri'] + '.mp4'
        s3.upload_file(output_path, R2_BUCKET, mp4_key,
                       ExtraArgs={'ContentType': 'video/mp4'})

        # DB更新
        supabase.table('uri_cache') \
            .update({'video_r2_key': mp4_key}) \
            .eq('uri', item['uri']) \
            .execute()

        print(f"Converted: {item['uri']}")
    except Exception as e:
        print(f"Failed: {item['uri']} - {e}")
    finally:
        for p in [input_path, output_path]:
            if os.path.exists(p):
                os.remove(p)