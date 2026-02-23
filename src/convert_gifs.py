# src/convert_gifs.py
import urllib.request
import subprocess
import os
import boto3
from supabase import create_client
from dotenv import load_dotenv
import time

load_dotenv()

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

paginator = s3.get_paginator('list_objects_v2')
pages = paginator.paginate(Bucket=R2_BUCKET, Prefix='convert/')
input_path = '/tmp/input'
output_path = '/tmp/output.mp4'

for page in pages:
    for obj in page.get('Contents', []):
        src_key = obj['Key']
        filename = src_key.split('/', 1)[1]
        
        # ファイル名がない or 拡張子がない場合はスキップ
        if not filename or '.' not in filename:
            continue
        
        hex_uri = filename.rsplit('.', 1)[0]
        ext = filename.rsplit('.', 1)[1].lower()

        try:
            s3.download_file(R2_BUCKET, src_key, input_path)

            if ext == 'gif':
                # GIF → MP4
                subprocess.run([
                    'ffmpeg', '-y', '-i', input_path,
                    '-movflags', 'faststart',
                    '-pix_fmt', 'yuv420p',
                    '-vf', 'scale=trunc(iw/2)*2:trunc(ih/2)*2',
                    '-c:v', 'libx264', '-crf', '23',
                    output_path
                ], check=True)
            else:
                # MP4/WebM等 → 軽量MP4にトランスコード
                subprocess.run([
                    'ffmpeg', '-y', '-i', input_path,
                    '-movflags', 'faststart',
                    '-pix_fmt', 'yuv420p',
                    '-vf', "scale='trunc(min(iw,512)/2)*2:trunc(min(ih,512)/2)*2'",
                    '-c:v', 'libx264', '-crf', '28',
                    '-preset', 'medium',
                    '-an',               # 音声除去（サムネ用途なら不要）
                    '-t', '30',          # 最大30秒に制限
                    output_path
                ], check=True)

            mp4_key = f'{hex_uri}.mp4'
            s3.upload_file(output_path, R2_BUCKET, mp4_key,
                           ExtraArgs={'ContentType': 'video/mp4'})

            supabase.table('uri_cache') \
                .update({'video_r2_key': mp4_key}) \
                .eq('uri', hex_uri) \
                .execute()

            # 変換済み原本を削除
            s3.delete_object(Bucket=R2_BUCKET, Key=src_key)

            print(f"Converted: {hex_uri} ({ext} → mp4)")
        except Exception as e:
            print(f"Failed: {hex_uri} - {e}")
        finally:
            for p in [input_path, output_path]:
                if os.path.exists(p):
                    os.remove(p)