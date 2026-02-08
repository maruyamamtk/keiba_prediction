#!/usr/bin/env python3
"""
JRDBの指定データタイプのindex.htmlから全lzh/csvファイルを抽出し、
ファイル名から日付（yymmdd）を抽出するスクリプト
"""

import urllib.request
import re
import sys
import os

# CSA/KSAは.csvファイルとして直接ダウンロード可能なデータタイプ
CSV_DATATYPES = {"CSA", "KSA"}

def datatype_to_folder(datatype):
    """
    データタイプからフォルダ名を生成
    例：KAA → Kaa, BAA → Baa
    例外：CSA → Cs, KSA → Ks（2文字のみ）
    """
    if datatype in CSV_DATATYPES:
        # CSA → Cs, KSA → Ks
        return datatype[0] + datatype[1].lower()
    else:
        # KAA → Kaa
        first_char = datatype[0]
        rest = datatype[1:].lower()
        return f"{first_char}{rest}"

def extract_files(datatype, username, password):
    """
    指定されたデータタイプのindex.htmlからlzh/csvファイルのリストを取得

    Args:
        datatype: データタイプ（例: KAA, CSA）
        username: JRDB認証ユーザー名
        password: JRDB認証パスワード

    Returns:
        list: 日付（yymmdd）のリスト
    """
    folder = datatype_to_folder(datatype)
    url = f"http://www.jrdb.com/member/data/{folder}/"

    # Basic認証を設定
    password_mgr = urllib.request.HTTPPasswordMgrWithDefaultRealm()
    password_mgr.add_password(None, url, username, password)
    handler = urllib.request.HTTPBasicAuthHandler(password_mgr)
    opener = urllib.request.build_opener(handler)
    urllib.request.install_opener(opener)

    try:
        response = urllib.request.urlopen(url)
        html = response.read().decode('utf-8', errors='ignore')
    except Exception as e:
        print(f"Error: Failed to access {url}", file=sys.stderr)
        print(f"Details: {e}", file=sys.stderr)
        sys.exit(1)

    # ファイルへのリンクを抽出
    # CSA/KSAは.csv、それ以外は.lzh
    # パターン: KAA241231.lzh または CSA241231.csv のような形式
    extension = 'csv' if datatype in CSV_DATATYPES else 'lzh'
    pattern = rf'{datatype}(\d{{6}})\.{extension}'
    matches = re.findall(pattern, html, re.IGNORECASE)

    # 日付のみのリストを返す（重複削除・ソート済み）
    dates = sorted(set(matches))

    return dates

def main():
    if len(sys.argv) < 4:
        print("Usage: python3 list_lzh_files.py <DATATYPE> <USERNAME> <PASSWORD>", file=sys.stderr)
        print("Example: python3 list_lzh_files.py KAA user pass", file=sys.stderr)
        print("Example: python3 list_lzh_files.py CSA user pass  (for CSV files)", file=sys.stderr)
        sys.exit(1)

    datatype = sys.argv[1].upper()
    username = sys.argv[2]
    password = sys.argv[3]

    dates = extract_files(datatype, username, password)

    extension = 'csv' if datatype in CSV_DATATYPES else 'lzh'
    if not dates:
        print(f"Warning: No {extension} files found for {datatype}", file=sys.stderr)
        sys.exit(0)

    # 日付のみを改行区切りで出力
    for date in dates:
        print(date)

if __name__ == '__main__':
    main()
