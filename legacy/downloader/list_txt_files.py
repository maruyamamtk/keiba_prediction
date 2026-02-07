#!/usr/bin/env python3
"""
JRDB プログラムサイトから全TXTファイルを抽出
"""

import urllib.request
import re
from urllib.parse import urljoin
from collections import defaultdict
import sys

url = "https://jrdb.com/program/data.html"
response = urllib.request.urlopen(url)
html = response.read().decode('utf-8', errors='ignore')

# txtファイルへのリンクを抽出
txt_links = re.findall(r'href=["\']([^"\']*\.txt)["\']', html)

# 相対URLを絶対URLに変換
txt_links = [urljoin(url, link) for link in txt_links]

# 重複を削除
txt_links = sorted(set(txt_links))

# カテゴリ別に分類
categories = defaultdict(list)

for link in txt_links:
    if '/program/' in link:
        parts = link.split('/program/')
        if len(parts) == 2:
            file_path = parts[1]
            if '/' in file_path:
                folder = file_path.split('/')[0]
                filename = file_path.split('/')[-1]
            else:
                folder = 'root'
                filename = file_path
            categories[folder].append(filename)

# URLのみを出力するモード
if len(sys.argv) > 1 and sys.argv[1] == '--urls-only':
    for link in txt_links:
        print(link)
    sys.exit(0)

# 通常の表示
print("=" * 80)
print("JRDB プログラムサイト - すべてのTXTファイル一覧")
print("=" * 80)
print()

# ルートのコード表を先に表示
if 'root' in categories:
    print("【ルートのコード表】")
    for file in sorted(categories['root']):
        print(f"  • {file}")
    print()

# その他のカテゴリを表示
for folder in sorted(categories.keys()):
    if folder != 'root':
        print(f"【{folder.upper()}フォルダ】")
        for file in sorted(categories[folder]):
            print(f"  • {file}")
        print()

# 統計情報
print("=" * 80)
total_files = sum(len(files) for files in categories.values())
print(f"合計: {total_files} ファイル")
print(f"フォルダ数: {len(categories)}")
print("=" * 80)

# CSVファイルとしても出力
print("\n\n【CSV形式でのエクスポート】\n")
print("フォルダ,ファイル名,URL")
for folder in sorted(categories.keys()):
    for file in sorted(categories[folder]):
        if folder == 'root':
            url_path = f"https://jrdb.com/program/{file}"
        else:
            url_path = f"https://jrdb.com/program/{folder}/{file}"
        print(f"{folder},{file},{url_path}")

