# Cloud Scheduler ジョブ一覧

このドキュメントでは、競馬予測MLシステムで稼働中のすべてのCloud Schedulerジョブを説明します。

---

## 稼働中ジョブ一覧

| ジョブ名 | スケジュール (JST) | cron式 | ターゲットエンドポイント | 用途 |
|---------|-----------------|--------|----------------------|------|
| `daily-data-pipeline` | 毎日 AM 6:00 | `0 6 * * *` | `POST /api/v1/load/daily/async` | JRDBデータロード |
| `race-day-predict` | 毎日 AM 8:00 | `0 8 * * *` | `POST /api/v1/predict/daily` | レース予測・BQ/GCS保存 |
| `race-day-odds-scrape` | 毎日 AM 8:15 | `15 8 * * *` | `POST /api/v1/odds/scrape` | netkeibaオッズ取得 |
| `race-day-strategy` | 毎日 AM 8:30 | `30 8 * * *` | `POST /api/v1/strategy/daily` | 投資戦略策定（dry_run=true） |
| `weekly-model-retrain` | 毎週月曜 AM 8:00 | `0 8 * * 1` | Cloud Run Jobs API（`keiba-model-retrain` 起動） | モデル週次再学習 |
| `race-day-purchase` | 土日 8:00〜17:00 5分おき | `*/5 8-17 * * 6,0` | `POST /api/v1/purchase/daily` | 発走直前IPAT自動馬券購入 |

**全ジョブ共通設定（`weekly-model-retrain` を除く）:**
- 認証: OIDCトークン（`keiba-pipeline-sa` サービスアカウント）
- タイムアウト: 900秒（15分）
- リトライ: 最大3回、バックオフ 5秒〜300秒
- タイムゾーン: Asia/Tokyo

**`weekly-model-retrain` ジョブの設定:**
- 認証: OAuth2（`keiba-pipeline-sa`、スコープ: `https://www.googleapis.com/auth/cloud-platform`）
- タイムアウト: 180秒（Cloud Run Jobs API 呼び出し完了まで。Job本体の実行は非同期）
- リトライ: 0回（再学習の重複実行を防ぐため）
- ターゲット: Cloud Run Jobs API（`keiba-model-retrain` Job を起動）

---

## 各ジョブの詳細

### daily-data-pipeline — 日次データロード

**スケジュール**: 毎日 AM 6:00 JST

**概要**: JRDBからその日のデータをダウンロードし、GCS→BigQueryへロードします。当日から直近7日間のファイルを対象とします。

**処理フロー**:
1. JRDBから当日データをダウンロード（lzh解凍・CP932→UTF-8変換）
2. GCSへアップロード（MD5重複チェック）
3. BigQueryへMERGE/UPSERTロード（`raw.load_history` で重複スキップ）
4. 特徴量生成（`features.training_data` 更新）

**リクエストボディ**: `{}` （空ボディ、当日日付を自動使用）

---

### race-day-predict — レース予測

**スケジュール**: 毎日 AM 8:00 JST

**概要**: GCSから最新の学習済みモデルを自動取得し、当日レースの着順予測スコアを計算してBigQueryとGCSに保存します。

**処理フロー**:
1. GCSバケット `{project}-keiba-models/lgbm_ranker/` から最新モデルを自動選択
2. `features.training_data` から当日レースデータを取得
3. LightGBM LambdaRankで予測スコアを計算
4. `predictions.daily_predictions` にUPSERT保存
5. GCS `gs://{project}-keiba-predictions/{date}/predictions.csv` にCSV保存

**リクエストボディ**: `{}` （空ボディ）

**前提条件**: `daily-data-pipeline` が正常完了していること。

---

### race-day-odds-scrape — リアルタイムオッズ取得

**スケジュール**: 毎日 AM 8:15 JST

**概要**: netkeibaから当日全レースの単勝・複勝オッズ、および組み合わせ馬券オッズをスクレイプしてBigQueryに保存します。

**処理フロー**:
1. `raw.race_info` から当日レースのrace_idリストを取得
2. Playwright (Chromium) で各レースのnetkeibaオッズページを取得
3. 単複オッズを `predictions.daily_odds` にUPSERT保存（キー: `race_id + horse_number`）
4. `include_combo=true` により組み合わせ馬券（馬連・ワイド・馬単・三連複）を `predictions.daily_odds_combo` にUPSERT保存

**リクエストボディ**: `{"include_combo": true}`

**前提条件**: `daily-data-pipeline` が正常完了していること。

---

### race-day-strategy — 投資戦略策定

**スケジュール**: 毎日 AM 8:30 JST

**概要**: 予測スコアとリアルタイムオッズを組み合わせて投資判断を策定します。

**重要**: このジョブは **`dry_run=true`（デフォルト）** で実行されます。BQへの保存は行われません。実際のBQ保存は、発走直前に `race-day-purchase` ジョブが `_refresh_investment_decisions_for_race()` を呼び出して最新オッズで上書き保存します（Issue #231）。

**処理フロー**（dry_run=true の場合）:
1. `predictions.daily_predictions` から当日予測スコアを取得
2. `predictions.daily_odds` / `predictions.daily_odds_combo` からオッズを取得
3. `config/strategy_config.yaml` のパラメータでKelly基準・期待回収率フィルタを適用
4. 投資判断を計算（BQ保存はスキップ）

**リクエストボディ**: `{}` （`dry_run` フィールドを省略するとデフォルト `true`）

> **BQ保存が必要な場合**: `race-day-purchase` の発走直前処理（`_refresh_investment_decisions_for_race`）が最新オッズで `predictions.investment_decisions` を上書き保存します。手動でBQ保存する場合は `{"dry_run": false}` を指定してください。

**前提条件**: `race-day-predict` と `race-day-odds-scrape` が正常完了していること。

---

### weekly-model-retrain — モデル週次再学習

**スケジュール**: 毎週月曜日 AM 8:00 JST

**概要**: Cloud Scheduler が Cloud Run Jobs API を呼び出し、`keiba-model-retrain` Job を起動します。Job は `python -m src.models.train --tune --project-id {PROJECT_ID}` を実行してLightGBM LambdaRankモデルを再学習し、GCSに保存します。

**重要**: このジョブは従来の `POST /api/v1/model/retrain/async`（FastAPI BackgroundTasks）ではなく、**Cloud Run Jobs**（`keiba-model-retrain`）を使用します。Cloud Run Jobsはタスク完了まで実行を保証し、最大24時間のタスクタイムアウトをサポートします。keiba-pipeline Serviceが`min-instances=0`でコールドスタートする場合でも再学習が中断されません。

**Cloud Run Jobs 設定**（`keiba-model-retrain`）:
- メモリ: 8Gi、CPU: 4
- タスクタイムアウト: 7200秒（2時間）
- 最大リトライ: 0回
- コマンド: `python -m src.models.train --tune --project-id {PROJECT_ID}`
- セットアップ: `./infrastructure/scripts/setup_cloud_run_jobs.sh`

**処理フロー**:
1. Cloud Scheduler が Cloud Run Jobs API（OAuth2認証）を呼び出し Job を非同期起動
2. `features.training_data` から学習データを取得
3. 時系列分割（学習/検証/推論）
4. Optuna ベイズ最適化でハイパーパラメータチューニング
5. NDCG@3・Recall@3・AUCを評価
6. GCS `gs://{project}-keiba-models/lgbm_ranker/{YYYYMMDD}/` に保存

**認証**: Cloud Scheduler → Cloud Run Jobs API は OAuth2（`cloud-platform` スコープ）を使用します（OIDCではなく）。

**手動実行**:
```bash
# Cloud Schedulerジョブ経由（Jobs API 呼び出し）
gcloud scheduler jobs run weekly-model-retrain --location=asia-northeast1

# Cloud Run Jobs を直接実行
gcloud run jobs execute keiba-model-retrain --region=asia-northeast1

# 実行状況の確認
gcloud run jobs executions list --job=keiba-model-retrain --region=asia-northeast1
```

---

### race-day-purchase — 発走直前IPAT自動馬券購入

**スケジュール**: 土日 AM 8:00〜PM 5:55 の5分おき

**概要**: 5分おきに起動し、現在時刻の5〜10分後に発走するレースが存在する場合に以下を実行します。netkeibaで最新オッズをスクレイピングして `daily_odds` を上書きし、投資戦略を再計算して `investment_decisions` を更新した上で、JRA IPAT SP版（`https://www.ipat.jra.go.jp/sp/`）で馬券を自動購入します。

**対応馬券種**: 単勝・複勝・馬連・ワイド・馬単・三連複

**処理フロー**（両モード共通 → dry_run 分岐）:
1. `raw.race_info` から当日の発走時刻付きレース一覧を取得
2. 現在時刻の **5〜10分後**に発走するレースを抽出（`window_minutes_before=10, window_minutes_after=5`）
   - 対象レースが0件の場合はそのまま終了（skipped）
3. 対象レースの最新オッズを netkeiba からリアルタイムスクレイピング → `predictions.daily_odds` に上書き保存
   - 失敗時はフォールバック（既存の `daily_odds` を使用）
4. `_refresh_investment_decisions_for_race()` で最新オッズを使い投資戦略を再計算 → `predictions.investment_decisions` を上書き保存
   - 失敗時はフォールバック（既存の `investment_decisions` を使用）
5. 推奨馬券を取得し、**dry_run に応じて分岐**:
   - `dry_run=true`: LINE通知のみ（IPATログイン・購入は行わない）
   - `dry_run=false`: IPAT SP版にログイン → ウィザード形式で馬券購入（`IpatPurchaser`） → `predictions.purchase_history` に保存 → LINE通知

**IPAT SP版購入ウィザード（`src/automation/data/ipat_purchaser.py`）**:
- ログイン: `https://www.ipat.jra.go.jp/sp/index.cgi`（SP版）
- 購入フロー: トップメニュー → 競馬場 → レース → 式別 → 投票形式 → 馬番選択 → 金額入力 → セット → 入力終了 → 合計金額確認 → 投票
- 馬番選択は jQuery Mobile の `tap` イベント（`trigger('tap')`）で制御（Playwright `click()` は JQM tap 非対応）
- 投票確認 confirm ダイアログは自動承認（`asyncio.create_task(d.accept())`）

**dry_run挙動**:

| 処理 | `dry_run=true` | `dry_run=false` |
|------|---------------|----------------|
| netkeibaスクレイピング（daily_odds更新） | 実行 | 実行 |
| investment_decisions 再計算・上書き保存 | 実行 | 実行 |
| IPATログイン | スキップ | 実行 |
| 馬券購入 | スキップ | 実行 |
| purchase_history 保存 | スキップ | 保存（実際の購入結果） |
| LINE通知 | 送信（「ドライラン」表記） | 送信 |

**リクエストボディ**: `{}` （`dry_run` デフォルトは `false`・本番購入モード）

**必要なSecret Manager設定**:

| シークレット名 | 説明 |
|-------------|------|
| `ipat-member-id` | 加入者番号 |
| `ipat-pin` | 暗証番号（4桁） |
| `ipat-pat-number` | PAT番号 |
| `line-channel-access-token` | LINE push通知用アクセストークン |
| `line-user-id` | LINE通知送信先ユーザーID |

---

## ジョブ操作コマンド

### 手動実行（テスト）

```bash
# 日次データロード
gcloud scheduler jobs run daily-data-pipeline --location=asia-northeast1

# レース予測
gcloud scheduler jobs run race-day-predict --location=asia-northeast1

# オッズ取得
gcloud scheduler jobs run race-day-odds-scrape --location=asia-northeast1

# 投資戦略策定
gcloud scheduler jobs run race-day-strategy --location=asia-northeast1

# モデル再学習
gcloud scheduler jobs run weekly-model-retrain --location=asia-northeast1

# 発走前購入（dry_run=falseの本番モード）
gcloud scheduler jobs run race-day-purchase --location=asia-northeast1
```

### 一時停止・再開

```bash
# 一時停止（例: race-day-purchase）
gcloud scheduler jobs pause race-day-purchase --location=asia-northeast1

# 再開
gcloud scheduler jobs resume race-day-purchase --location=asia-northeast1
```

### ジョブ一覧確認

```bash
gcloud scheduler jobs list --location=asia-northeast1
```

### 移行時の旧ジョブ削除

`monthly-model-retrain` から `weekly-model-retrain` へ移行する際は、旧ジョブを手動削除してください:

```bash
gcloud scheduler jobs delete monthly-model-retrain --location=asia-northeast1
```

### dry_runの本番切り替え

`race-day-purchase` を本番購入モード（dry_run=false）に切り替える:

```bash
gcloud scheduler jobs update http race-day-purchase \
  --location=asia-northeast1 \
  --message-body='{"dry_run": false}' \
  --project=<PROJECT_ID>
```

dry_runモードに戻す:

```bash
gcloud scheduler jobs update http race-day-purchase \
  --location=asia-northeast1 \
  --message-body='{}' \
  --project=<PROJECT_ID>
```

---

## ログ確認

### Cloud Schedulerジョブの実行履歴

```bash
# 直近10件のスケジューラログ
gcloud logging read 'resource.type="cloud_scheduler_job"' --limit=10

# 特定ジョブのログ
gcloud logging read 'resource.type="cloud_scheduler_job" AND resource.labels.job_id="race-day-purchase"' --limit=20
```

### Cloud Runのアプリケーションログ

```bash
# 直近50件
gcloud run services logs read keiba-pipeline --region=asia-northeast1 --limit=50

# ERRORレベルのみ
gcloud logging read 'resource.type="cloud_run_revision" AND severity>=ERROR' --limit=20
```

---

## 障害時の対応手順

### 日次データロードが失敗した場合

1. ログを確認し原因を特定:

```bash
gcloud logging read 'resource.type="cloud_run_revision" AND severity>=ERROR' --limit=20
```

2. 手動で再実行:

```bash
gcloud scheduler jobs run daily-data-pipeline --location=asia-northeast1
```

3. ロード状態を診断:

```bash
python3 scripts/diagnose_bq_load.py --show-errors
```

### レース予測が失敗した場合

1. モデルがGCSに存在するか確認:

```bash
gcloud storage ls gs://<PROJECT_ID>-keiba-models/lgbm_ranker/
```

2. 手動でCLI実行（デバッグ用）:

```bash
python3 -m src.models.predict --project-id <PROJECT_ID> \
  --model-path gs://<PROJECT_ID>-keiba-models/lgbm_ranker/<YYYYMMDD>/model.txt
```

### オッズ取得が失敗した場合

netkeibaのHTMLDOM変更やPlaywrightの問題が原因の可能性があります。

1. 手動でジョブ再実行:

```bash
gcloud scheduler jobs run race-day-odds-scrape --location=asia-northeast1
```

2. ローカルで手動スクレイプ（特定日付）:

```bash
python3 scripts/scrape_historical_odds.py \
  --project-id <PROJECT_ID> \
  --start-date <YYYY-MM-DD> \
  --end-date <YYYY-MM-DD> \
  --mode all
```

### 投資戦略策定が失敗した場合

予測データまたはオッズデータが不足している可能性があります。

1. BQに当日データが存在するか確認:

```bash
# 当日の予測件数確認
bq query --nouse_legacy_sql \
  "SELECT COUNT(*) FROM \`<PROJECT_ID>.predictions.daily_predictions\` WHERE race_date = CURRENT_DATE('Asia/Tokyo')"
```

2. 手動でスクリプト実行（dry_run=false でBQ保存）:

```bash
python3 scripts/run_strategy.py --project-id <PROJECT_ID> --target-date <YYYY-MM-DD>
```

### IPAT自動購入が失敗した場合

1. IPATのログイン情報（Secret Manager）を確認:

```bash
gcloud secrets list --project=<PROJECT_ID>
```

2. ローカルE2Eテストスクリプトで動作確認（実購入・ローカルのみ）:

```bash
IPAT_MEMBER_ID=xxx IPAT_PIN=xxx IPAT_PAT_NUMBER=xxx \
  .venv/bin/python scripts/test_ipat_e2e.py \
    --venue "中山(日)" --race 11 --bet-type wide --horses 1 12 --amount 100

# dry-runモード（ログインのみ、購入しない）
IPAT_MEMBER_ID=xxx IPAT_PIN=xxx IPAT_PAT_NUMBER=xxx \
  .venv/bin/python scripts/test_ipat_e2e.py --dry-run
```

3. dry_runモードでCloud Run経由の動作確認:

```bash
curl -X POST <CLOUD_RUN_URL>/api/v1/purchase/daily \
  -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
  -H "Content-Type: application/json" \
  -d '{"dry_run": true}'
```

---

## ジョブ設定スクリプト

### Cloud Run Jobs のセットアップ（初回のみ）

`weekly-model-retrain` Cloud Scheduler ジョブが依存する Cloud Run Job を作成します:

```bash
./infrastructure/scripts/setup_cloud_run_jobs.sh
```

### Cloud Scheduler ジョブの作成・更新

```bash
./infrastructure/scripts/setup_scheduler.sh
```

**注意**: `setup_scheduler.sh` は `keiba-model-retrain` Cloud Run Job が存在することを確認してから実行します。未作成の場合はエラーになるため、先に `setup_cloud_run_jobs.sh` を実行してください。

詳細なインフラセットアップ手順は [infrastructure/README.md](./infrastructure/README.md) を参照してください。
