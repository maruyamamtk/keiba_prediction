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

利用可能なデータタイプ:
======================================================================
$FILETYPES
======================================================================

Start date? (ex. 220101)
yymmdd の形式で入力してください。
この日付以降のすべてのファイルがダウンロードされます。
EOS

read START_DATE

# 入力チェック
if [ -z "$START_DATE" ]; then
  echo "Error: Start date is required"
  exit 1
fi

# 日付の形式チェック（6桁の数字）
if ! echo "$START_DATE" | grep -qE '^[0-9]{6}$'; then
  echo "Error: Invalid date format. Please use yymmdd format (e.g., 220101)"
  exit 1
fi

echo ""
echo "========================================================================"
echo "Starting download of all data types from $START_DATE..."
echo "========================================================================"
echo "Data types to download: $FILETYPES"
echo "========================================================================"
echo ""

# 各データタイプのダウンロード結果を記録
SUCCESS_TYPES=""
FAIL_TYPES=""
TOTAL_SUCCESS=0
TOTAL_FAIL=0

for FILETYPE in $FILETYPES
do
  echo ""
  echo "###################################################################"
  echo "# Processing data type: $FILETYPE"
  echo "###################################################################"
  echo ""

  # download_from_date.shを呼び出す（標準入力で自動応答）
  sh $(dirname $0)/download_from_date.sh << EOS
${FILETYPE}
${START_DATE}
y
EOS

  # 終了コードをチェック
  if [ $? -eq 0 ]; then
    SUCCESS_TYPES="$SUCCESS_TYPES $FILETYPE"
    TOTAL_SUCCESS=$((TOTAL_SUCCESS + 1))
  else
    FAIL_TYPES="$FAIL_TYPES $FILETYPE"
    TOTAL_FAIL=$((TOTAL_FAIL + 1))
  fi
done

# 全体のサマリー
echo ""
echo "========================================================================"
echo "All downloads completed!"
echo "========================================================================"
echo ""
echo "Summary:"
echo "--------"
echo "Total data types processed: $(echo $FILETYPES | wc -w | tr -d ' ')"
echo "Successful: $TOTAL_SUCCESS"
echo "Failed: $TOTAL_FAIL"
echo ""

if [ -n "$SUCCESS_TYPES" ]; then
  echo "Successful data types:"
  for type in $SUCCESS_TYPES; do
    echo "  ✓ $type"
  done
  echo ""
fi

if [ -n "$FAIL_TYPES" ]; then
  echo "Failed data types:"
  for type in $FAIL_TYPES; do
    echo "  ✗ $type"
  done
  echo ""
fi

echo "========================================================================"
