#!/bin/sh

# Load environment variables
DIR=$(dirname "$0")
if [ -f "${DIR}/.env" ]; then
  set -a
  . "${DIR}/.env"
  set +a
fi

# Create schema directory if it doesn't exist
mkdir -p "${DOWNLOAD_FILE_OUTPUT_DIRECTORY}schema"

# Base URL for JRDB program resources
JRDB_PROGRAM_BASE_URL="https://jrdb.com/program/"
JRDB_MEMBER_BASE_URL="https://jrdb.com/member/"

# Download all TXT files from data.html
echo "Downloading JRDB specification documents and code tables..."
echo "=============================================================="
echo "Total files to download: Extracting from data.html..."
echo ""

# list_txt_files.py から全URLを取得してダウンロード
python3 "${DIR}/list_txt_files.py" --urls-only | while read -r url; do
  if [ -n "$url" ]; then
    # URLからファイル名を抽出
    filename=$(echo "$url" | sed 's#.*/##')
    # フォルダ構造をスキーマフォルダに保存（サブフォルダも作成）
    subfolder=$(echo "$url" | sed "s#.*program/##" | sed 's#/[^/]*$##')
    
    # サブフォルダがある場合は作成
    if [ "$subfolder" != "$filename" ]; then
      mkdir -p "${DOWNLOAD_FILE_OUTPUT_DIRECTORY}schema/${subfolder}"
      output_path="${DOWNLOAD_FILE_OUTPUT_DIRECTORY}schema/${subfolder}/${filename}"
    else
      output_path="${DOWNLOAD_FILE_OUTPUT_DIRECTORY}schema/${filename}"
    fi
    
    echo "  ✓ Downloading: $filename"
    curl -L -o "$output_path" "$url"
    
    # CP932エンコーディングをUTF-8に変換（テキストファイルの場合）
    if file "$output_path" | grep -q "text"; then
      iconv -f CP932 -t UTF-8 "$output_path" > "${output_path}.tmp" 2>/dev/null && \
      mv "${output_path}.tmp" "$output_path" || true
    fi
  fi
done

echo ""
echo "=============================================================="

# Download jrdb_data_doc.txt specifically (not included in data.html)
echo "Downloading jrdb_data_doc.txt..."
curl -L -o "${DOWNLOAD_FILE_OUTPUT_DIRECTORY}schema/jrdb_data_doc.txt" "${JRDB_PROGRAM_BASE_URL}jrdb_data_doc.txt"
if [ -f "${DOWNLOAD_FILE_OUTPUT_DIRECTORY}schema/jrdb_data_doc.txt" ]; then
  iconv -f CP932 -t UTF-8//IGNORE "${DOWNLOAD_FILE_OUTPUT_DIRECTORY}schema/jrdb_data_doc.txt" > "${DOWNLOAD_FILE_OUTPUT_DIRECTORY}schema/jrdb_data_doc.txt.tmp" 2>/dev/null && \
  mv "${DOWNLOAD_FILE_OUTPUT_DIRECTORY}schema/jrdb_data_doc.txt.tmp" "${DOWNLOAD_FILE_OUTPUT_DIRECTORY}schema/jrdb_data_doc.txt"
  echo "✓ Downloaded and converted: jrdb_data_doc.txt"
fi

echo ""
echo "=============================================================="

# Download member-only data index page
echo "Downloading JRDB member data index..."
curl -u ${JRDB_USER}:${JRDB_PASSWORD} -L -o "${DOWNLOAD_FILE_OUTPUT_DIRECTORY}schema/dataindex.html" "${JRDB_MEMBER_BASE_URL}dataindex.html"

# Convert dataindex.html encoding from CP932 to UTF-8
if [ -f "${DOWNLOAD_FILE_OUTPUT_DIRECTORY}schema/dataindex.html" ]; then
  iconv -f CP932 -t UTF-8 "${DOWNLOAD_FILE_OUTPUT_DIRECTORY}schema/dataindex.html" > "${DOWNLOAD_FILE_OUTPUT_DIRECTORY}schema/dataindex.html.tmp" && \
  mv "${DOWNLOAD_FILE_OUTPUT_DIRECTORY}schema/dataindex.html.tmp" "${DOWNLOAD_FILE_OUTPUT_DIRECTORY}schema/dataindex.html"
  echo "✓ Downloaded and converted: dataindex.html"
fi

echo "=============================================================="
echo "Schema and documentation download completed!"
echo "Files saved to: ${DOWNLOAD_FILE_OUTPUT_DIRECTORY}schema/"

