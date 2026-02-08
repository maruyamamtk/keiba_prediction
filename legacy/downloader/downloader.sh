#!/bin/sh

# Load environment variables
DIR=$(dirname "$0")
if [ -f "${DIR}/.env" ]; then
  set -a
  . "${DIR}/.env"
  set +a
fi

JRDB_BASE_URL=http://www.jrdb.com/member/data/
EXTENTION=.lzh

# dataindex.htmlから最新のデータタイプを取得
get_latest_datatypes() {
  python3 "${DIR}/extract_datatypes.py"
}

# データタイプからファイル名プレフィックスを取得
# 最新データタイプ（大文字）をフォルダ名に変換
# 例：KAA → Kaa, BAA → Baa, KYF → Kyf
datatype_to_path() {
  local first_char=$(echo $1 | cut -c1)
  local rest=$(echo $1 | cut -c2- | tr '[A-Z]' '[a-z]')
  echo "${first_char}${rest}"
}

filepath() {
  FILETYPE_LOWER=$(datatype_to_path $FILETYPE)
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
  # テキストファイルを CSV に変換
  FILETYPE_LOWER=$(datatype_to_path $FILETYPE)
  for txt_file in ${DOWNLOAD_FILE_OUTPUT_DIRECTORY}${FILETYPE_LOWER}/*.txt; do
    if [ -f "$txt_file" ]; then
      csv_file="${txt_file%.txt}.csv"
      # 固定長テキストをカンマで区切ったCSVに変換（行をそのままCSVの行にする）
      # JRDB のテキストはスペース区切りまたは固定長形式なので、タブをカンマに置換
      sed 's/[[:space:]]\+/,/g' "$txt_file" > "$csv_file"
      rm "$txt_file"
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

main() {
  pre_process

  status_code=`file_exists`
  if [ $status_code != 200 ]
  then
    echo "⚠ ${JRDB_BASE_URL}$(filepath) is not found. (HTTP $status_code)"
    exit 0
  fi

  echo "✓ Downloading $(filepath)..."
  download
  echo "✓ Decompressing..."
  decompression
  echo "✓ Converting encoding..."
  convert_encoding
  echo "✓ Converting to CSV..."
  txt_to_csv
  post_process
  echo "✓ Completed: $FILETYPE $FILEDATE"
}

# 利用可能なデータタイプを取得
LATEST_DATATYPES=$(get_latest_datatypes)

if [ -z "$LATEST_DATATYPES" ]; then
  echo "Error: Failed to extract data types from dataindex.html"
  exit 1
fi

cat <<EOS

Datatype? (ex. KAA)

以下のファイルタイプが選択できます。
（dataindex.htmlから自動抽出）
===================================
$LATEST_DATATYPES
===================================
EOS

read FILETYPE

cat <<EOS

Filedate? (ex. 220319)
yymmdd の形式で入力してください。
EOS

read FILEDATE

main
