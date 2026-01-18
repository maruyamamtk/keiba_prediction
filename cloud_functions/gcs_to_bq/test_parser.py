"""
パーサーのテストスクリプト
"""

import sys
sys.path.insert(0, '.')

from parser import JRDBParser

def test_baa_parser():
    """BAAパーサーのテスト"""
    # 実際のBAAファイルから1行読み込み
    test_line = "06261101202601041005120021112A30223                                                           16 1　　　　        "

    print("Testing BAA parser...")
    result = JRDBParser.parse_baa_line(test_line)

    if result:
        print("✓ BAA parser successful")
        print(f"  Race ID: {result['race_id']}")
        print(f"  Venue: {result['venue_name']}")
        print(f"  Distance: {result['distance']}m")
        print(f"  Course: {result['course_type']}")
        print(f"  Horses: {result['num_horses']}")
    else:
        print("✗ BAA parser failed")
        return False

    return True

def test_race_id_parser():
    """レースIDパーサーのテスト"""
    print("\nTesting Race ID parser...")

    test_cases = [
        "06261101",  # 中山 2026年 1回1日 1R
        "05240305",  # 東京 2024年 3回5日 5R
    ]

    for race_key in test_cases:
        result = JRDBParser.parse_race_id(race_key)
        print(f"  {race_key} -> Venue: {result['venue_code']}, Year: {result['year']}, Race: {result['race_number']}")

    return True

def main():
    """メインテスト"""
    print("=" * 60)
    print("JRDB Parser Test")
    print("=" * 60)

    all_passed = True

    # テスト実行
    all_passed &= test_race_id_parser()
    all_passed &= test_baa_parser()

    print("\n" + "=" * 60)
    if all_passed:
        print("All tests passed! ✓")
    else:
        print("Some tests failed ✗")
    print("=" * 60)

if __name__ == "__main__":
    main()
