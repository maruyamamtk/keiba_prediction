#!/bin/sh

# Load environment variables
DIR=$(dirname "$0")
if [ -f "${DIR}/.env" ]; then
  set -a
  . "${DIR}/.env"
  set +a
fi

JRDB_BASE_URL=http://www.jrdb.com/member/data/

# CSA/KSAは.csvファイルとして直接ダウンロード可能なデータタイプ
CSV_DATATYPES="CSA KSA"

# 拡張子を取得（CSA/KSAは.csv、それ以外は.lzh）
get_extension() {
  case " $CSV_DATATYPES " in
    *" $1 "*) echo ".csv" ;;
    *) echo ".lzh" ;;
  esac
}

# データタイプからファイル名プレフィックスを取得
# 最新データタイプ（大文字）をフォルダ名に変換
# 例：KAA → Kaa, BAA → Baa, KYF → Kyf
# 例外：CSA → Cs, KSA → Ks（2文字のみ）
datatype_to_path() {
  # CSA/KSAは特殊パス（2文字のみ）
  case "$1" in
    CSA) echo "Cs" ;;
    KSA) echo "Ks" ;;
    *)
      local first_char=$(echo $1 | cut -c1)
      local rest=$(echo $1 | cut -c2- | tr '[A-Z]' '[a-z]')
      echo "${first_char}${rest}"
      ;;
  esac
}

filepath() {
  FILETYPE_LOWER=$(datatype_to_path $FILETYPE)
  EXTENTION=$(get_extension $FILETYPE)
  echo ${FILETYPE_LOWER}/${FILETYPE}${FILEDATE}${EXTENTION}
}

file_exists() {
  curl -u ${JRDB_USER}:${JRDB_PASSWORD} ${JRDB_BASE_URL}$(filepath) -o /dev/null -w '%{http_code}\n' -s
}

download() {
  curl -u ${JRDB_USER}:${JRDB_PASSWORD} ${JRDB_BASE_URL}$(filepath) -L -o ${DOWNLOAD_FILE_OUTPUT_DIRECTORY}$(filepath)
}

decompression() {
  FILETYPE_LOWER=$(datatype_to_path $FILETYPE)
  lha -xw=${DOWNLOAD_FILE_OUTPUT_DIRECTORY}${FILETYPE_LOWER} ${DOWNLOAD_FILE_OUTPUT_DIRECTORY}$(filepath)
}

convert_encoding() {
  # 解凍されたテキストファイルを CP932 から UTF-8 に変換
  FILETYPE_LOWER=$(datatype_to_path $FILETYPE)
  for file in ${DOWNLOAD_FILE_OUTPUT_DIRECTORY}${FILETYPE_LOWER}/*.txt; do
    if [ -f "$file" ]; then
      iconv -f CP932 -t UTF-8 "$file" > "${file}.tmp" && mv "${file}.tmp" "$file"
    fi
  done
}

txt_to_csv() {
  # テキストファイルの拡張子を .csv に変更
  # 注意: parser.py は固定長テキストの文字位置に基づいてパースするため、
  # スペースをカンマに変換するとパースが破綻する。
  # ここでは拡張子変更のみを行い、固定長フォーマットを維持する。
  FILETYPE_LOWER=$(datatype_to_path $FILETYPE)

  # テキストファイル保存ディレクトリを作成
  TXT_BACKUP_DIR="${DOWNLOAD_FILE_OUTPUT_DIRECTORY}${FILETYPE_LOWER}/txt_backup"
  mkdir -p "$TXT_BACKUP_DIR"

  for txt_file in ${DOWNLOAD_FILE_OUTPUT_DIRECTORY}${FILETYPE_LOWER}/*.txt; do
    if [ -f "$txt_file" ]; then
      csv_file="${txt_file%.txt}.csv"
      # 元のテキストファイルをバックアップディレクトリにコピー
      cp "$txt_file" "$TXT_BACKUP_DIR/$(basename "$txt_file")"
      # 拡張子のみ変更（固定長フォーマットを維持）
      mv "$txt_file" "$csv_file"
    fi
  done
}

pre_process() {
  FILETYPE_LOWER=$(datatype_to_path $FILETYPE)
  mkdir -p ${DOWNLOAD_FILE_OUTPUT_DIRECTORY}${FILETYPE_LOWER}
}

post_process() {
  rm -f ${DOWNLOAD_FILE_OUTPUT_DIRECTORY}$(filepath)
}

is_csv_datatype() {
  case " $CSV_DATATYPES " in
    *" $1 "*) return 0 ;;
    *) return 1 ;;
  esac
}

convert_encoding_csv() {
  # CSVファイルを CP932 から UTF-8 に変換
  FILETYPE_LOWER=$(datatype_to_path $FILETYPE)
  CSV_FILE="${DOWNLOAD_FILE_OUTPUT_DIRECTORY}${FILETYPE_LOWER}/${FILETYPE}${FILEDATE}.csv"
  if [ -f "$CSV_FILE" ]; then
    iconv -f CP932 -t UTF-8 "$CSV_FILE" > "${CSV_FILE}.tmp" && mv "${CSV_FILE}.tmp" "$CSV_FILE"
  fi
}

download_single_file() {
  # ローカルにcsvが存在するかチェック（ダウンロード済みならスキップ）
  FILETYPE_LOWER=$(datatype_to_path $FILETYPE)
  LOCAL_CSV="${DOWNLOAD_FILE_OUTPUT_DIRECTORY}${FILETYPE_LOWER}/${FILETYPE}${FILEDATE}.csv"

  if [ -f "$LOCAL_CSV" ]; then
    echo "⏭ Skipping ${FILETYPE}${FILEDATE} - already downloaded"
    return 0
  fi

  pre_process

  status_code=`file_exists`
  if [ $status_code != 200 ]
  then
    echo "⚠ ${JRDB_BASE_URL}$(filepath) is not found. (HTTP $status_code)"
    return 1
  fi

  echo "✓ Downloading $(filepath)..."
  download

  # CSA/KSAの場合は解凍不要（直接csvでダウンロードされる）
  if is_csv_datatype "$FILETYPE"; then
    echo "✓ Converting encoding..."
    convert_encoding_csv
  else
    echo "✓ Decompressing..."
    decompression
    echo "✓ Converting encoding..."
    convert_encoding
    echo "✓ Converting to CSV..."
    txt_to_csv
    post_process
  fi

  echo "✓ Completed: $FILETYPE $FILEDATE"
  return 0
}

# 指定されたデータタイプの全lzhファイルの日付リストを取得
get_available_dates() {
  python3 "${DIR}/list_lzh_files.py" "$FILETYPE" "$JRDB_USER" "$JRDB_PASSWORD"
}

# 日付を比較（yymmdd形式）
# yyが90以上の場合（1990年代）は除外
is_date_after_or_equal() {
  local date=$1
  local start_date=$2

  # yyの部分を抽出（最初の2桁）
  local yy=$(echo "$date" | cut -c1-2)

  # yyが90以上の場合は除外（1990年代以前のデータ）
  if [ "$yy" -ge 90 ]; then
    return 1
  fi

  # それ以外は通常の比較
  [ "$date" -ge "$start_date" ]
}

main() {
  # データタイプを入力
  cat <<EOS

Datatype? (ex. KAA)
データタイプを入力してください。
EOS
  read FILETYPE

  # 入力チェック
  if [ -z "$FILETYPE" ]; then
    echo "Error: Datatype is required"
    exit 1
  fi

  # 大文字に変換
  FILETYPE=$(echo "$FILETYPE" | tr '[a-z]' '[A-Z]')

  # 開始日付を入力
  cat <<EOS

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
  echo "Fetching available files for $FILETYPE..."
  echo "========================================================================"

  # 利用可能な日付のリストを取得
  AVAILABLE_DATES=$(get_available_dates)

  if [ -z "$AVAILABLE_DATES" ]; then
    echo "Error: No files found for $FILETYPE"
    exit 1
  fi

  # 開始日付以降のファイルをフィルタリング
  FILES_TO_DOWNLOAD=""
  for date in $AVAILABLE_DATES; do
    if is_date_after_or_equal "$date" "$START_DATE"; then
      FILES_TO_DOWNLOAD="$FILES_TO_DOWNLOAD $date"
    fi
  done

  if [ -z "$FILES_TO_DOWNLOAD" ]; then
    echo "No files found after $START_DATE"
    exit 0
  fi

  # ダウンロード対象のファイル数をカウント
  FILE_COUNT=$(echo $FILES_TO_DOWNLOAD | wc -w | tr -d ' ')

  # 拡張子を取得
  FILE_EXT=$(get_extension $FILETYPE)

  echo ""
  echo "Found $FILE_COUNT file(s) to download:"
  echo "========================================================================"
  for date in $FILES_TO_DOWNLOAD; do
    echo "  • $FILETYPE$date$FILE_EXT"
  done
  echo "========================================================================"
  echo ""
  echo "Start downloading? (y/n)"
  read CONFIRM

  if [ "$CONFIRM" != "y" ] && [ "$CONFIRM" != "Y" ]; then
    echo "Download cancelled."
    exit 0
  fi

  # ダウンロード開始
  echo ""
  echo "========================================================================"
  echo "Starting download..."
  echo "========================================================================"

  SUCCESS_COUNT=0
  FAIL_COUNT=0

  for date in $FILES_TO_DOWNLOAD; do
    FILEDATE=$date
    echo ""
    echo "------------------------------------------------------------------------"
    echo "Processing: $FILETYPE$FILEDATE$FILE_EXT"
    echo "------------------------------------------------------------------------"

    if download_single_file; then
      SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
    else
      FAIL_COUNT=$((FAIL_COUNT + 1))
    fi
  done

  # 結果サマリー
  echo ""
  echo "========================================================================"
  echo "Download Summary"
  echo "========================================================================"
  echo "Total files: $FILE_COUNT"
  echo "Success: $SUCCESS_COUNT"
  echo "Failed: $FAIL_COUNT"
  echo "========================================================================"
}

main
