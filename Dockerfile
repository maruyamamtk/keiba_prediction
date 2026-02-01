# Dockerfile
FROM python:3.9-slim

# 作業ディレクトリを設定
WORKDIR /app

# システム依存パッケージのインストール
# - curl: ヘルスチェック用およびJRDBダウンロード用
# - lhasa: lzhファイルの展開用（lhaコマンドを提供）
# - p7zip-full: lzh展開のフォールバック用
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    lhasa \
    p7zip-full \
    && rm -rf /var/lib/apt/lists/*

# 依存パッケージをインストール
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# アプリケーションコードをコピー
COPY src/ ./src/
COPY scripts/ ./scripts/
COPY downloader/ ./downloader/
COPY config/ ./config/
COPY main.py .

# 環境変数のデフォルト値
ENV PORT=8080
ENV PYTHONUNBUFFERED=1

# ポートを公開
EXPOSE 8080

# アプリケーションを起動
CMD ["python", "main.py"]
