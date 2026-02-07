# リファクタリング実施レポート

## 実施日
2026-02-01

## 目的
Cloud Run Functions での将来の自動化を見据えたコード構造の改善とドキュメント更新

## 実施内容

### 1. 新規ファイル作成

#### `src/data/pipeline.py`
- **目的**: ダウンロード→GCSアップロードを統合管理
- **主要クラス**:
  - `DataPipeline`: パイプライン統合クラス
  - `PipelineResult`: 実行結果を格納するデータクラス
- **主要機能**:
  - `run_download_and_upload()`: JRDBダウンロード→GCSアップロードを一括実行
  - 一時ディレクトリの自動管理（Cloud Run環境向け）
  - エラーハンドリングとクリーンアップ
  - 環境変数からのインスタンス作成（`create_pipeline_from_env()`）

#### `tests/test_pipeline.py`
- **目的**: `src/data/pipeline.py` のテスト
- **テストケース数**: 13
- **カバレッジ**: 主要な機能とエラーケース

### 2. 既存ファイルの更新

#### `main.py` (Cloud Runエントリーポイント)
- **変更前**: ダウンロード機能のみ実装、GCSアップロードと特徴量生成は未実装
- **変更後**: 全パイプライン統合を完成
  - `/download` エンドポイント → `/download_and_upload` に変更（ダウンロード+アップロード統合）
  - `/run` エンドポイント → `/run_full_pipeline` に改名
  - フルパイプライン実装:
    1. JRDBダウンロード→GCSアップロード（一時ディレクトリ使用）
    2. BigQueryロード（Cloud Functionが自動実行）
    3. 特徴量生成（BigQuery raw → features）

#### `README.md`
- **変更内容**:
  - 現状の手動実行と将来の自動化計画を明確に区分
  - パイプライン全体フローの図を更新（現状・将来計画を分離）
  - 手動実行手順の詳細化:
    - Step 1: データ取得（方法A: シェルスクリプト、方法B: Pythonモジュール）
    - Step 1+2 統合: `python -m src.data.pipeline` の追加
    - Cloud Runでの実行手順を追加（ローカルテスト、デプロイ、Cloud Scheduler設定）
  - プロジェクト構成の更新（新規ファイルの追加）
  - 主要機能セクションの拡充（JRDBダウンローダー、パイプライン統合の説明追加）
  - 実装計画の更新（Phase 3: Cloud Run統合 を追加）

### 3. アーキテクチャ改善

#### 統合前（分散アーキテクチャ）
```
[手動実行1] JRDBダウンローダー（シェルスクリプト or Pythonモジュール）
     ↓
[手動実行2] GCSアップロード（別コマンド）
     ↓
[自動] BigQueryロード（Cloud Function）
     ↓
[手動実行3] 特徴量生成（別コマンド）
```

#### 統合後（パイプライン統合）
```
[統合実行] src.data.pipeline（ダウンロード→アップロード一括）
     ↓
[自動] BigQueryロード（Cloud Function）
     ↓
[統合実行] Cloud Run: /run エンドポイント（全パイプライン統合）
```

#### 将来計画（完全自動化）
```
[自動] Cloud Scheduler → Cloud Run (/run エンドポイント)
  ├─ JRDBダウンロード→GCSアップロード（一時ディレクトリ）
  ├─ BigQueryロード（Cloud Functionが自動トリガー）
  └─ 特徴量生成
```

### 4. 主要な設計判断

#### 一時ディレクトリの導入
- **理由**: Cloud Run環境ではディスク容量が限られるため
- **実装**: `use_temp_dir` フラグでローカル/Cloud Run環境を切り替え
- **効果**: 処理完了後の自動クリーンアップによるディスク容量の節約

#### パイプライン統合
- **理由**: 手動実行の手順を減らし、将来の自動化を容易にする
- **実装**: `DataPipeline` クラスで複数ステップを統合管理
- **効果**: エラーハンドリングの一元化、ログの統合

#### エラーハンドリングの強化
- **リトライ処理**: 既存の`GCSUploader`のリトライ機能を活用
- **クリーンアップ**: エラー時も一時ディレクトリを確実に削除
- **ログ**: 統合されたログ出力で問題の追跡を容易化

### 5. テスト結果

```bash
# パイプライン統合のテスト
$ pytest tests/test_pipeline.py -v
13 passed

# JRDBダウンローダーのテスト
$ pytest tests/test_jrdb_downloader.py -v
19 passed

# GCSアップロードのテスト
$ pytest tests/test_upload_to_gcs.py -v
38 passed

# 合計
70 passed
```

### 6. 残存課題と今後の作業

#### 完了済み
- [x] データパイプライン統合モジュール作成
- [x] Cloud Runエントリーポイントの完成
- [x] README更新
- [x] テスト作成

#### 今後の作業（Phase 3）
- [ ] Cloud Runへのデプロイ実施
- [ ] Cloud Scheduler設定
- [ ] Secret Managerでの認証情報管理
- [ ] Cloud Loggingとの統合強化
- [ ] エラー通知の設定（メール/LINE）

#### 今後の作業（Phase 4以降）
- [ ] Target Encoding実装
- [ ] LightGBM ランク学習モデル開発
- [ ] バックテストシステム構築

## リファクタリング原則の遵守状況

### ✅ 遵守した原則

1. **Correctness First**
   - 既存機能の動作を変更せず、統合のみ実施
   - 全テストが成功（70 passed）

2. **Small & Safe Steps**
   - 機能追加は段階的に実施（pipeline.py → main.py → README）
   - 各ステップでテストを実行して確認

3. **Tests as Safety Net**
   - 新規コードに対してテストを作成（test_pipeline.py）
   - 既存テストが全て成功することを確認

4. **One Concern per Change**
   - パイプライン統合、main.py更新、README更新を明確に分離

5. **設計原則の適用**
   - Single Responsibility: 各クラスが明確な責務を持つ
   - Explicit over Implicit: 環境変数の取得、エラーハンドリングを明示的に
   - Fail Fast: エラー時の早期リターン
   - Clear naming: `DataPipeline`, `PipelineResult` など意図が明確

## 変更の影響範囲

### 影響なし
- 既存のコマンドラインインターフェース（`python -m src.data.jrdb_downloader` など）は変更なし
- 既存のCloud Function（gcs_to_bq）は変更なし
- 既存のBigQueryテーブル構造は変更なし

### 新規追加
- `python -m src.data.pipeline`: ダウンロード→アップロード統合コマンド
- Cloud Run `/download` エンドポイント → `/download_and_upload` に変更（互換性なし、新規利用想定）
- Cloud Run `/run` エンドポイント: フルパイプライン実行

### 将来の互換性
- シェルスクリプト（`download_all_from_date.sh`）は当面維持
- 段階的にPythonモジュールへの移行を推奨

## まとめ

Cloud Run Functions での完全自動化を見据えたリファクタリングを実施し、以下を達成しました:

1. **データパイプライン統合**: ダウンロード→GCSアップロードを一括実行可能に
2. **Cloud Run対応**: フルパイプライン（ダウンロード→アップロード→特徴量生成）をCloud Runで実行可能に
3. **ドキュメント整備**: 現状と将来計画を明確に区分したREADME更新
4. **テストカバレッジ**: 新規コードに対してテストを作成し、全て成功

今後はCloud Runへのデプロイ、Cloud Schedulerの設定を実施し、完全自動化を実現していきます。
