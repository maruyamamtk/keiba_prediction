#!/usr/bin/env python3
"""
dataindex.htmlから各項目における一番右側（最新版）のデータタイプを抽出するスクリプト
"""

import re
import sys
from pathlib import Path

def extract_datatypes(html_file):
    """
    dataindex.htmlから最新のデータタイプを抽出
    
    Returns:
        list: 抽出されたデータタイプのリスト
    """
    with open(html_file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # 各行（<tr>タグで囲まれた行）を取得
    rows = re.findall(r'<tr>.*?</tr>', content, re.DOTALL)
    
    datatypes = []
    
    for row in rows:
        # リンクを抽出 (<a href="...">XXX</a>)
        links = re.findall(r'<a href="[^"]*">([A-Z0-9]+)</a>', row)
        
        if links:
            # 最後のリンク（最右側）を取得
            latest = links[-1]
            
            # ZIPまたはLZHの形式で保存されているもののみを対象
            # （複数回出現しないようにチェック）
            if latest not in datatypes:
                datatypes.append(latest)
    
    return datatypes

def main():
    html_file = Path.home() / 'Desktop' / 'keiba_prediction' / 'downloaded_files' / 'schema' / 'dataindex.html'
    
    if not html_file.exists():
        print(f"Error: {html_file} not found", file=sys.stderr)
        sys.exit(1)
    
    datatypes = extract_datatypes(str(html_file))
    
    # シェルスクリプト用の配列形式で出力
    print(' '.join(datatypes))

if __name__ == '__main__':
    main()
