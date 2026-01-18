#!/usr/bin/env python3
"""
JRDBの指定データタイプのindex.htmlから全lzhファイルを抽出し、
ファイル名から日付（yymmdd）を抽出するスクリプト
"""

import urllib.request
import re
import sys
import os

def extract_lzh_files(datatype, username, password):
    """
    指定されたデータタイプのindex.htmlからlzhファイルのリストを取得

    Args:
        datatype: データタイプ（例: KAA）
        username: JRDB認証ユーザー名
        password: JRDB認証パスワード

    Returns:
        list: (filename, yymmdd) のタプルのリスト
    """
    # データタイプからフォルダ名を生成（KAA → Kaa）
    first_char = datatype[0]
    rest = datatype[1:].lower()
    folder = f"{first_char}{rest}"

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

    # lzhファイルへのリンクを抽出
    # パターン: KAA241231.lzh のような形式
    pattern = rf'{datatype}(\d{{6}})\.lzh'
    matches = re.findall(pattern, html, re.IGNORECASE)

    # 日付のみのリストを返す（重複削除・ソート済み）
    dates = sorted(set(matches))

    return dates

def main():
    if len(sys.argv) < 4:
        print("Usage: python3 list_lzh_files.py <DATATYPE> <USERNAME> <PASSWORD>", file=sys.stderr)
        print("Example: python3 list_lzh_files.py KAA user pass", file=sys.stderr)
        sys.exit(1)

    datatype = sys.argv[1].upper()
    username = sys.argv[2]
    password = sys.argv[3]

    dates = extract_lzh_files(datatype, username, password)

    if not dates:
        print(f"Warning: No lzh files found for {datatype}", file=sys.stderr)
        sys.exit(0)

    # 日付のみを改行区切りで出力
    for date in dates:
        print(date)

if __name__ == '__main__':
    main()
