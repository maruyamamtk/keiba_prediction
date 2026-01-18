"""
main.pyのテストスクリプト
"""

import sys
sys.path.insert(0, '.')

from main import extract_data_type, get_table_name

def test_extract_data_type():
    """ファイル名からデータタイプを抽出するテスト"""
    print("Testing extract_data_type...")

    test_cases = [
        # (入力, 期待される出力)
        ("BAA260104.csv", "BAA"),
        ("KYF260104.csv", "KYF"),
        ("Baa/BAA260104.csv", "BAA"),  # ディレクトリパス付き
        ("Kyf/KYF260110.csv", "KYF"),  # ディレクトリパス付き
        ("subfolder/nested/BAC260112.csv", "BAC"),  # ネストされたパス
        ("invalid.csv", None),  # 不正なファイル名
        ("BAA26010.csv", None),  # 不正な長さ
    ]

    all_passed = True
    for filename, expected in test_cases:
        result = extract_data_type(filename)
        if result == expected:
            print(f"  ✓ {filename} -> {result}")
        else:
            print(f"  ✗ {filename} -> Expected: {expected}, Got: {result}")
            all_passed = False

    return all_passed

def test_get_table_name():
    """データタイプからテーブル名を取得するテスト"""
    print("\nTesting get_table_name...")

    test_cases = [
        # (入力, 期待される出力)
        ("BAA", "race_info"),
        ("BAB", "race_info"),
        ("BAC", "race_info"),
        ("KYF", "horse_results"),
        ("KYG", "horse_results"),
        ("KYH", "horse_results"),
        ("SEC", "horse_results"),
        ("INVALID", None),
    ]

    all_passed = True
    for data_type, expected in test_cases:
        result = get_table_name(data_type)
        if result == expected:
            print(f"  ✓ {data_type} -> {result}")
        else:
            print(f"  ✗ {data_type} -> Expected: {expected}, Got: {result}")
            all_passed = False

    return all_passed

def main():
    """メインテスト"""
    print("=" * 60)
    print("Main Module Test")
    print("=" * 60)

    all_passed = True

    # テスト実行
    all_passed &= test_extract_data_type()
    all_passed &= test_get_table_name()

    print("\n" + "=" * 60)
    if all_passed:
        print("All tests passed! ✓")
    else:
        print("Some tests failed ✗")
    print("=" * 60)

if __name__ == "__main__":
    main()
