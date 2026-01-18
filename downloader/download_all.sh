#!/bin/sh

# Load environment variables
DIR=$(dirname "$0")
if [ -f "${DIR}/.env" ]; then
  set -a
  . "${DIR}/.env"
  set +a
fi

# dataindex.htmlから最新のデータタイプを取得
get_latest_datatypes() {
  python3 "${DIR}/extract_datatypes.py"
}

# 利用可能なデータタイプを取得
FILETYPES=$(get_latest_datatypes)

if [ -z "$FILETYPES" ]; then
  echo "Error: Failed to extract data types from dataindex.html"
  exit 1
fi

cat <<EOS

Filedate? (ex. 220319)
yymmdd の形式で入力してください。
EOS

read FILEDATE

echo "Starting download of all data types for $FILEDATE..."
echo "========================================================================"
echo "Data types to download: $FILETYPES"
echo "========================================================================"

for FILETYPE in $FILETYPES
do
  sh $(dirname $0)/downloader.sh << EOS
${FILETYPE}
${FILEDATE}
EOS
done

echo "========================================================================"
echo "All downloads completed!"
