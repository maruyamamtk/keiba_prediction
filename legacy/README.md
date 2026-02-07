# Legacy Code

このディレクトリにはレガシー/未使用のコードを格納しています。

## 内容

- `main.py` - 旧Flask APIエントリーポイント（FastAPIに移行済み）
- `data_pipeline.py` - 旧データパイプライン（DailyPipeline/FullLoadPipelineに移行済み）
- `cloud_functions/` - 旧Cloud Functions（Cloud Runに移行済み）
- `downloader/` - 旧シェルスクリプト版ダウンローダー（Python版に移行済み）

## 注意

- これらのコードは参照目的で保持しています
- 新規開発では使用しないでください
- 将来的に削除する予定です

## 移行先

### main.py → src/automation/api/app.py
- Flask → FastAPIに移行
- uvicornで起動: `uvicorn src.automation.api.app:app --host 0.0.0.0 --port 8080`

### data_pipeline.py → src/automation/pipeline/
- `DailyPipeline`: 日次データ処理（ダウンロード→GCS→BigQuery）
- `FullLoadPipeline`: 過去分全件ロード

### cloud_functions/ → src/automation/data/
- `gcs_to_bq/parser.py` → `src/automation/data/jrdb_parser.py`
- Cloud Functions → Cloud Run統合パイプラインに移行

### downloader/ → src/automation/data/jrdb_downloader.py
- シェルスクリプト → Pythonモジュール化
- `python -m src.automation.data.jrdb_downloader --start-date 240101`
