# Dockerfile
FROM python:3.9-slim

# 作業ディレクトリを設定
WORKDIR /app

# システム依存パッケージのインストール
# - curl: ヘルスチェック用およびJRDBダウンロード用
# - lhasa: lzhファイルの展開用（lhaコマンドを提供）
# - p7zip-full: lzh展開のフォールバック用
# - libgomp1: LightGBM の OpenMP 並列処理に必要
# - Playwright/Chromium依存パッケージ（netkeibaスクレイピング用）
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    lhasa \
    p7zip-full \
    libgomp1 \
    libnss3 \
    libnspr4 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libxkbcommon0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libasound2 \
    libgtk-3-0 \
    libx11-xcb1 \
    libxcb-dri3-0 \
    && rm -rf /var/lib/apt/lists/*

# 依存パッケージをインストール
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Playwright Chromiumブラウザのインストール（netkeibaスクレイピング用）
RUN playwright install chromium

# アプリケーションコードをコピー
COPY src/ ./src/
COPY scripts/ ./scripts/
COPY config/ ./config/

# 環境変数のデフォルト値
ENV PORT=8080
ENV PYTHONUNBUFFERED=1

# ポートを公開
EXPOSE 8080

# FastAPIアプリケーションを起動
CMD ["uvicorn", "src.automation.api.app:app", "--host", "0.0.0.0", "--port", "8080"]
