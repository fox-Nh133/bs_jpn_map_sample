import pandas as pd
import googlemaps
import json
import time
import os
from dotenv import load_dotenv

# 1. 環境変数の読み込み
load_dotenv() 

# 環境変数からAPIキーを取得
API_KEY = os.getenv('GOOGLE_MAPS_API_KEY')
if not API_KEY:
    raise ValueError("APIキーが見つかりません。.envファイルを確認してください。")

# 2. データの読み込み
df = pd.read_csv('scout_all_prefectures_web_data.csv')

# 3. クライアントの準備 (Google Maps)
gmaps = googlemaps.Client(key=API_KEY)

def clean_address(address):
    """
    住所文字列を整形する関数 (アップデート版)
    
    課題: "千葉県〒264-0007 千葉県千葉市..." のように途中にスペースがあると
         最初のパーツだけ取ってしまい住所が不完全になる。
         
    対策: スペースで分割した後、「郵便番号」や「注釈」を除外して、
         最も住所らしい部分を抽出する。
    """
    if pd.isna(address):
        return ""
    
    # 文字列化して空白(全角・半角)で分割
    # 例: ['千葉県〒264-0007', '千葉県千葉市若葉区...', '（駐車場有）']
    parts = str(address).split()
    
    valid_parts = []
    for part in parts:
        # 1. "〒" を含むパーツは、住所本体ではなく郵便番号情報とみなしてスキップ
        if '〒' in part:
            continue
            
        # 2. カッコで始まるパーツは、建物名や注釈とみなしてスキップ
        #    (住所の先頭がカッコで始まることはまずないため)
        if part.startswith('(') or part.startswith('（'):
            continue
        
        # 3. それ以外を候補として残す
        valid_parts.append(part)
    
    # 候補が残っていれば、その先頭を採用
    # (例: '千葉県千葉市若葉区...' が残る)
    if valid_parts:
        return valid_parts[0]
    
    # もしフィルタリングですべて消えてしまった場合（安全策）、元の最初のパーツを返す
    return parts[0] if parts else ""

def get_lat_lon_google(address):
    """
    Google Maps APIを使って緯度経度を取得する
    """
    try:
        if not address:
            return None, None

        geocode_result = gmaps.geocode(address)

        if geocode_result:
            location = geocode_result[0]['geometry']['location']
            return location['lat'], location['lng']
        else:
            return None, None

    except Exception as e:
        print(f"Error ({address}): {e}")
        return None, None

# 4. 変換実行
print("緯度経度への変換を開始します(Google Maps API)...")

# df_subset = df.head(10).copy() 
df_subset = df.copy()

latitudes = []
longitudes = []

total = len(df_subset)

for index, row in df_subset.iterrows():
    original_address = row['Address']
    
    # 【前処理】アップデートされたロジックで整形
    target_address = clean_address(original_address)
    
    print(f"Processing {index+1}/{total}: {original_address} -> {target_address}")
    
    lat, lng = get_lat_lon_google(target_address)
    
    latitudes.append(lat)
    longitudes.append(lng)
    
    time.sleep(0.1)

df_subset['lat'] = latitudes
df_subset['lng'] = longitudes

# 緯度経度が取れなかったデータを除外
df_clean = df_subset.dropna(subset=['lat', 'lng'])

print(f"変換成功: {len(df_clean)} / {total} 件")

# 5. JSON形式で保存
json_output = df_clean.to_json(orient='records', force_ascii=False)

with open('scout_data_geocoded.json', 'w', encoding='utf-8') as f:
    f.write(json_output)

print("完了しました。scout_data_geocoded.json が作成されました。")
