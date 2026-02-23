# src/convert_gifs.py
import urllib.request
import subprocess
import os
import boto3
from supabase import create_client

def ipfs_to_http(url):
    if url.startswith('ipfs://'):
        return 'https://cloudflare-ipfs.com/ipfs/' + url[7:]
    if url.startswith('ar://'):
        return 'https://arweave.net/' + url[5:]
    return url

supabase = create_client(os.environ['SUPABASE_URL'], os.environ['SUPABASE_KEY'])

s3 = boto3.client('s3',
    endpoint_url=os.environ['R2_ENDPOINT'],
    aws_access_key_id=os.environ['R2_ACCESS_KEY'],
    aws_secret_access_key=os.environ['R2_SECRET_KEY'],
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
        s3.upload_file(output_path, os.environ['R2_BUCKET'], mp4_key,
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