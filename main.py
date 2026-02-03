import functions_framework
import pandas as pd
import requests
from bs4 import BeautifulSoup
from google.cloud import bigquery
from google.cloud import storage
import io
import re
from urllib.parse import urljoin  # URL結合用に追記

# --- SETTINGS ---

PROJECT_ID = "main-project-477501"
BUCKET_NAME = "customs-exchange-rate"
DATASET_ID = "etc"
TABLE_ID = "weekly-custom-exchange-rates"

@functions_framework.http
def fetch_customs_rate(request):
    """
    税関のホームページから最新の公示為替レートCSVを取得し、
    人民元のデータを抽出してBigQueryに保存します。
    """
    base_url = "https://www.customs.go.jp/tetsuzuki/kawase/index.htm"
    
    # User-Agentを設定（アクセス拒否対策）
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    try:
        # 1. 最新CSVのURLを特定
        response = requests.get(base_url, headers=headers)
        response.raise_for_status()
        
        # 文字化け防止のためapparent_encodingを使用するか、HTML解析時はバイト列を渡す
        soup = BeautifulSoup(response.content, "html.parser")
        
        # リンクから /csv/ を含み .csv で終わるものを抽出
        # 税関のページ構造に合わせて柔軟に検索
        csv_link_tag = soup.find("a", href=lambda h: h and ".csv" in h and "csv" in h)
        
        if not csv_link_tag:
            print("Error: CSV link not found in the HTML.")
            return "CSV link not found", 404
            
        # 相対パスを絶対URLに安全に変換 (urljoinを使用)
        relative_path = csv_link_tag["href"]
        csv_url = urljoin(base_url, relative_path)
        
        file_name = csv_url.split("/")[-1]
        print(f"Target CSV URL: {csv_url}") # ログ確認用

        # 2. CSVデータのダウンロード
        csv_res = requests.get(csv_url, headers=headers)
        csv_res.raise_for_status()
        
        # 重要: テキストではなくバイナリ(content)として取得
        # 税関データはShift-JISが多いため、UTF-8でtextアクセスすると壊れる可能性がある
        csv_content_bytes = csv_res.content

        # 3. Cloud Storage への保存 (バイナリとして保存)
        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(f"raw_customs_csv/{file_name}")
        
        # バイナリデータをアップロード
        blob.upload_from_string(csv_content_bytes, content_type="text/csv")
        print(f"Saved to GCS: gs://{BUCKET_NAME}/raw_customs_csv/{file_name}")

        # 4. Pandas によるデータ抽出
        # バイナリIOとして読み込み、エンコーディングにcp932(Shift-JIS拡張)を指定
        # ヘッダー行(通常7行目あたり)までスキップ。CSVの形式が変わっていないか注意が必要
        try:
            df = pd.read_csv(io.BytesIO(csv_content_bytes), skiprows=6, encoding="cp932")
        except UnicodeDecodeError:
            # cp932で失敗した場合はutf-8を試行
            df = pd.read_csv(io.BytesIO(csv_content_bytes), skiprows=6, encoding="utf-8")
        
        # カラム名のクリーニング（空白除去など）
        df.columns = [str(c).strip() for c in df.columns]

        # ISOコード 'CNY' の行を抽出
        # カラム名が不明確な場合があるため、'ISO'カラムが存在するか確認
        if "ISO" not in df.columns:
             # カラムが見つからない場合、列の位置で特定を試みる（例：2列目がISO）
             # 今回は安全のため、カラム名が取れていない場合はエラーとする
             print(f"Columns found: {df.columns}")
             return "ISO column not found in CSV", 500

        cny_row = df[df["ISO"] == "CNY"].copy()
        if cny_row.empty:
            return "CNY data not found", 404

        # ファイル名から適用日付を抽出 (例: 240107240113.csv -> 2024-01-07)
        # 正規表現で数字の羅列を探す
        date_matches = re.findall(r"\d{6}", file_name)
        if len(date_matches) >= 2:
            start_date = pd.to_datetime(f"20{date_matches[0]}", format="%Y%m%d").date()
            end_date = pd.to_datetime(f"20{date_matches[1]}", format="%Y%m%d").date()
        else:
            # 日付が取れない場合のフォールバック（本日日付など）
            print("Could not extract date from filename. Using current date.")
            start_date = pd.Timestamp.now().date()
            end_date = start_date

        # レートの取得（1単位または100単位の列を確認）
        # iloc[0, 4] がレートカラムと仮定（CSV構造依存）
        rate_1 = cny_row.iloc[0, 4]
        rate_100 = cny_row.iloc[0, 5] # 隣の列も念のため確認
        
        final_rate = 0.0
        
        # 数値型に変換できるか確認しながら取得
        def to_float(val):
            try:
                return float(str(val).replace(",", ""))
            except:
                return None

        v1 = to_float(rate_1)
        v100 = to_float(rate_100)

        if v1 is not None and v1 > 0:
            final_rate = v1
        elif v100 is not None and v100 > 0:
            final_rate = v100 / 100
        else:
            return "Valid rate not found", 500

        # BigQuery用DataFrame作成
        load_df = pd.DataFrame([{
            "start_date": start_date,
            "end_date": end_date,
            "iso_code": "CNY",
            "currency_name": "人民元",
            "rate": final_rate,
            "source_file": file_name
        }])
        
        # 日付型をBQ用に変換
        load_df["start_date"] = pd.to_datetime(load_df["start_date"])
        load_df["end_date"] = pd.to_datetime(load_df["end_date"])

        # 5. BigQuery への書き込み
        bq_client = bigquery.Client(project=PROJECT_ID)
        table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        
        bq_client.load_table_from_dataframe(load_df, table_ref, job_config=job_config).result()

        return f"Success: Loaded CNY rate {final_rate} for {start_date} from {file_name}", 200

    except Exception as e:
        # ログにスタックトレースを残す
        import traceback
        traceback.print_exc()
        print(f"Error: {str(e)}")
        return f"Error: {str(e)}", 500