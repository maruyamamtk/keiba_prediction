#!/bin/bash
#
# Cloud Schedulerジョブセットアップスクリプト
#
# Cloud Runサービス keiba-pipeline の日次パイプラインを自動実行する
# Cloud Schedulerジョブを作成する。
#
# 作成されるジョブ:
#   1. daily-data-pipeline   AM 6:00 JST  データロード
#   2. race-day-predict      AM 8:00 JST  翌日レース予測
#   3. race-day-odds-scrape  AM 8:15 JST  netkeibaオッズ取得（単複＋組み合わせ）
#   4. race-day-strategy     AM 8:30 JST  投資戦略策定・investment_decisions保存
#   5. race-day-notify       AM 9:00 JST  LINE推奨馬券通知（土日のみ）
#
# 使用方法:
#   ./infrastructure/scripts/setup_scheduler.sh
#
# 前提条件:
#   1. gcloud CLIがインストール済み
#   2. 適切な権限を持つユーザーでログイン済み (gcloud auth login)
#   3. .envファイルにGCP_PROJECT_IDが設定済み
#   4. Cloud Runサービス keiba-pipeline がデプロイ済み
#

set -e

# 色付きログ出力
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# スクリプトのディレクトリを取得
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# .envファイルを読み込み
if [ -f "${PROJECT_ROOT}/.env" ]; then
    log_info ".envファイルを読み込んでいます..."
    set -a
    source "${PROJECT_ROOT}/.env"
    set +a
else
    log_error ".envファイルが見つかりません: ${PROJECT_ROOT}/.env"
    exit 1
fi

# 必須変数のチェック
if [ -z "${GCP_PROJECT_ID}" ] || [ "${GCP_PROJECT_ID}" = "your-project-id" ]; then
    log_error "GCP_PROJECT_IDが設定されていません。.envファイルを確認してください。"
    exit 1
fi

# デフォルト値の設定
GCP_REGION="${GCP_REGION:-asia-northeast1}"
PIPELINE_SA_NAME="${PIPELINE_SA_NAME:-keiba-pipeline-sa}"
SERVICE_NAME="keiba-pipeline"
# データロードジョブ
LOAD_JOB_NAME="daily-data-pipeline"
LOAD_SCHEDULE="0 6 * * *"
# 予測ジョブ
PREDICT_JOB_NAME="race-day-predict"
PREDICT_SCHEDULE="0 8 * * *"
# オッズ取得ジョブ
ODDS_SCRAPE_JOB_NAME="race-day-odds-scrape"
ODDS_SCRAPE_SCHEDULE="15 8 * * *"
# 投資戦略ジョブ
STRATEGY_JOB_NAME="race-day-strategy"
STRATEGY_SCHEDULE="30 8 * * *"
# LINE通知ジョブ（土日のみ）
NOTIFY_JOB_NAME="race-day-notify"
NOTIFY_SCHEDULE="0 9 * * 0,6"
TIME_ZONE="Asia/Tokyo"

# サービスアカウントのメールアドレス
PIPELINE_SA_EMAIL="${PIPELINE_SA_NAME}@${GCP_PROJECT_ID}.iam.gserviceaccount.com"

log_info "=========================================="
log_info "Cloud Schedulerジョブをセットアップします"
log_info "=========================================="
log_info "プロジェクトID: ${GCP_PROJECT_ID}"
log_info "リージョン: ${GCP_REGION}"
log_info "データロードジョブ: ${LOAD_JOB_NAME} (${LOAD_SCHEDULE})"
log_info "予測ジョブ: ${PREDICT_JOB_NAME} (${PREDICT_SCHEDULE})"
log_info "オッズ取得ジョブ: ${ODDS_SCRAPE_JOB_NAME} (${ODDS_SCRAPE_SCHEDULE})"
log_info "投資戦略ジョブ: ${STRATEGY_JOB_NAME} (${STRATEGY_SCHEDULE})"
log_info "LINE通知ジョブ: ${NOTIFY_JOB_NAME} (${NOTIFY_SCHEDULE}) ※土日のみ"
log_info "タイムゾーン: ${TIME_ZONE}"
log_info "サービスアカウント: ${PIPELINE_SA_EMAIL}"
log_info "=========================================="

# ========================================
# 1. Cloud Runサービスの存在確認とURL取得
# ========================================
log_info "Cloud Runサービスを確認しています..."

SERVICE_URL=$(gcloud run services describe "${SERVICE_NAME}" \
    --region="${GCP_REGION}" \
    --format="value(status.url)" 2>/dev/null)

if [ -z "${SERVICE_URL}" ]; then
    log_error "Cloud Runサービスが見つかりません: ${SERVICE_NAME}"
    log_error "先にCloud Runにデプロイしてください:"
    log_error "  ./infrastructure/scripts/deploy_cloud_run.sh"
    exit 1
fi

log_info "サービスURL: ${SERVICE_URL}"

# ターゲットURL（データロード: 同期エンドポイント）
# ※ 非同期エンドポイント(/async)はCloud Runが即座にレスポンスを返すため
#    バックグラウンドタスク実行中にインスタンスがスケールダウンしてロードが中断される。
#    同期エンドポイントを使用してパイプライン完了まで接続を維持する。
LOAD_TARGET_URI="${SERVICE_URL}/api/v1/load/daily"
log_info "データロードURI: ${LOAD_TARGET_URI}"
# ターゲットURL（予測: 日次予測エンドポイント）
PREDICT_TARGET_URI="${SERVICE_URL}/api/v1/predict/daily"
log_info "予測URI: ${PREDICT_TARGET_URI}"
# ターゲットURL（オッズ取得: netkeibaスクレイパーエンドポイント）
ODDS_SCRAPE_TARGET_URI="${SERVICE_URL}/api/v1/odds/scrape"
log_info "オッズ取得URI: ${ODDS_SCRAPE_TARGET_URI}"
# ターゲットURL（投資戦略: 日次投資戦略エンドポイント）
STRATEGY_TARGET_URI="${SERVICE_URL}/api/v1/strategy/daily"
log_info "投資戦略URI: ${STRATEGY_TARGET_URI}"
# ターゲットURL（LINE通知: 土日AM9:00プッシュ通知エンドポイント）
NOTIFY_TARGET_URI="${SERVICE_URL}/api/v1/line/notify/daily"
log_info "LINE通知URI: ${NOTIFY_TARGET_URI}"

# ========================================
# 2. Cloud Scheduler APIの有効化確認
# ========================================
log_info "Cloud Scheduler APIを確認しています..."

if ! gcloud services list --enabled --filter="name:cloudscheduler.googleapis.com" --format="value(name)" 2>/dev/null | grep -q "cloudscheduler"; then
    log_info "Cloud Scheduler APIを有効化しています..."
    gcloud services enable cloudscheduler.googleapis.com --quiet
    log_info "Cloud Scheduler APIを有効化しました"
else
    log_info "Cloud Scheduler APIは既に有効です"
fi

# ========================================
# 3. サービスアカウントにCloud Run起動権限を付与
# ========================================
log_info "サービスアカウントにCloud Run起動権限を付与しています..."

# Cloud Run Invoker権限を付与（Cloud SchedulerがCloud Runを呼び出すために必要）
gcloud run services add-iam-policy-binding "${SERVICE_NAME}" \
    --region="${GCP_REGION}" \
    --member="serviceAccount:${PIPELINE_SA_EMAIL}" \
    --role="roles/run.invoker" \
    --quiet 2>/dev/null || {
    log_warn "Cloud Run Invoker権限の付与に失敗しました（既に付与されている可能性があります）"
}

log_info "権限設定が完了しました"

# ========================================
# 4. Cloud Schedulerジョブの作成/更新
# ========================================

# ジョブ作成/更新の共通関数
# 引数: job_name schedule uri [deadline] [message_body]
create_or_update_job() {
    local job_name="$1"
    local schedule="$2"
    local uri="$3"
    local deadline="${4:-900s}"
    local message_body="${5:-{}}"

    if gcloud scheduler jobs describe "${job_name}" \
        --location="${GCP_REGION}" > /dev/null 2>&1; then
        log_info "既存のジョブ ${job_name} を更新します..."

        gcloud scheduler jobs update http "${job_name}" \
            --location="${GCP_REGION}" \
            --schedule="${schedule}" \
            --time-zone="${TIME_ZONE}" \
            --uri="${uri}" \
            --http-method=POST \
            --update-headers="Content-Type=application/json" \
            --message-body="${message_body}" \
            --oidc-service-account-email="${PIPELINE_SA_EMAIL}" \
            --oidc-token-audience="${SERVICE_URL}" \
            --attempt-deadline="${deadline}" \
            --max-retry-attempts=3 \
            --min-backoff=5s \
            --max-backoff=300s \
            --quiet

        log_info "ジョブを更新しました: ${job_name}"
    else
        log_info "新しいジョブ ${job_name} を作成します..."

        gcloud scheduler jobs create http "${job_name}" \
            --location="${GCP_REGION}" \
            --schedule="${schedule}" \
            --time-zone="${TIME_ZONE}" \
            --uri="${uri}" \
            --http-method=POST \
            --headers="Content-Type=application/json" \
            --message-body="${message_body}" \
            --oidc-service-account-email="${PIPELINE_SA_EMAIL}" \
            --oidc-token-audience="${SERVICE_URL}" \
            --attempt-deadline="${deadline}" \
            --max-retry-attempts=3 \
            --min-backoff=5s \
            --max-backoff=300s \
            --quiet

        log_info "ジョブを作成しました: ${job_name}"
    fi
}

log_info "Cloud Schedulerジョブを設定しています..."

# 4-1. データロードジョブ（daily-data-pipeline）
log_info "--- データロードジョブの設定 ---"
create_or_update_job \
    "${LOAD_JOB_NAME}" \
    "${LOAD_SCHEDULE}" \
    "${LOAD_TARGET_URI}" \
    "900s"

# 4-2. 予測ジョブ（race-day-predict）
# attempt-deadline は Cloud Run タイムアウト(900s)より短く設定し競合を回避
log_info "--- 予測ジョブの設定 ---"
create_or_update_job \
    "${PREDICT_JOB_NAME}" \
    "${PREDICT_SCHEDULE}" \
    "${PREDICT_TARGET_URI}" \
    "800s"

# 4-3. オッズ取得ジョブ（race-day-odds-scrape）
# include_combo=true で単複オッズ(daily_odds) + 組み合わせオッズ(daily_odds_combo) を両方取得
# race-day-predict(AM 8:00) 完了後の AM 8:15 に実行
log_info "--- オッズ取得ジョブの設定 ---"
create_or_update_job \
    "${ODDS_SCRAPE_JOB_NAME}" \
    "${ODDS_SCRAPE_SCHEDULE}" \
    "${ODDS_SCRAPE_TARGET_URI}" \
    "800s" \
    '{"include_combo": true}'

# 4-4. 投資戦略ジョブ（race-day-strategy）
# daily_predictions + daily_odds を参照して investment_decisions に保存
# race-day-odds-scrape(AM 8:15) 完了後の AM 8:30 に実行
log_info "--- 投資戦略ジョブの設定 ---"
create_or_update_job \
    "${STRATEGY_JOB_NAME}" \
    "${STRATEGY_SCHEDULE}" \
    "${STRATEGY_TARGET_URI}" \
    "800s"

# 4-5. LINE通知ジョブ（race-day-notify）
# investment_decisions を参照してLINEプッシュ通知を送信
# 土日 AM 9:00 のみ実行（平日はエンドポイント側でスキップ）
log_info "--- LINE通知ジョブの設定 ---"
create_or_update_job \
    "${NOTIFY_JOB_NAME}" \
    "${NOTIFY_SCHEDULE}" \
    "${NOTIFY_TARGET_URI}" \
    "60s"

# ========================================
# 5. ジョブの確認
# ========================================
log_info ""
log_info "=========================================="
log_info "セットアップ完了"
log_info "=========================================="
log_info ""
log_info "ジョブ一覧:"

gcloud scheduler jobs list \
    --location="${GCP_REGION}" \
    --format="table(name,schedule,timeZone,state,httpTarget.uri)"

log_info ""
log_info "=== 操作コマンド ==="
log_info ""
log_info "【データロードジョブ (${LOAD_JOB_NAME})】"
log_info "  手動実行:    gcloud scheduler jobs run ${LOAD_JOB_NAME} --location=${GCP_REGION}"
log_info "  一時停止:    gcloud scheduler jobs pause ${LOAD_JOB_NAME} --location=${GCP_REGION}"
log_info "  再開:        gcloud scheduler jobs resume ${LOAD_JOB_NAME} --location=${GCP_REGION}"
log_info "  実行履歴:    gcloud logging read 'resource.type=\"cloud_scheduler_job\" AND resource.labels.job_id=\"${LOAD_JOB_NAME}\"' --limit=10"
log_info ""
log_info "【予測ジョブ (${PREDICT_JOB_NAME})】"
log_info "  手動実行:    gcloud scheduler jobs run ${PREDICT_JOB_NAME} --location=${GCP_REGION}"
log_info "  一時停止:    gcloud scheduler jobs pause ${PREDICT_JOB_NAME} --location=${GCP_REGION}"
log_info "  再開:        gcloud scheduler jobs resume ${PREDICT_JOB_NAME} --location=${GCP_REGION}"
log_info "  実行履歴:    gcloud logging read 'resource.type=\"cloud_scheduler_job\" AND resource.labels.job_id=\"${PREDICT_JOB_NAME}\"' --limit=10"
log_info ""
log_info "【オッズ取得ジョブ (${ODDS_SCRAPE_JOB_NAME})】"
log_info "  手動実行:    gcloud scheduler jobs run ${ODDS_SCRAPE_JOB_NAME} --location=${GCP_REGION}"
log_info "  一時停止:    gcloud scheduler jobs pause ${ODDS_SCRAPE_JOB_NAME} --location=${GCP_REGION}"
log_info "  再開:        gcloud scheduler jobs resume ${ODDS_SCRAPE_JOB_NAME} --location=${GCP_REGION}"
log_info "  実行履歴:    gcloud logging read 'resource.type=\"cloud_scheduler_job\" AND resource.labels.job_id=\"${ODDS_SCRAPE_JOB_NAME}\"' --limit=10"
log_info ""
log_info "【投資戦略ジョブ (${STRATEGY_JOB_NAME})】"
log_info "  手動実行:    gcloud scheduler jobs run ${STRATEGY_JOB_NAME} --location=${GCP_REGION}"
log_info "  一時停止:    gcloud scheduler jobs pause ${STRATEGY_JOB_NAME} --location=${GCP_REGION}"
log_info "  再開:        gcloud scheduler jobs resume ${STRATEGY_JOB_NAME} --location=${GCP_REGION}"
log_info "  実行履歴:    gcloud logging read 'resource.type=\"cloud_scheduler_job\" AND resource.labels.job_id=\"${STRATEGY_JOB_NAME}\"' --limit=10"
log_info ""
log_info "【LINE通知ジョブ (${NOTIFY_JOB_NAME})】 ※土日 AM 9:00 のみ"
log_info "  手動実行:    gcloud scheduler jobs run ${NOTIFY_JOB_NAME} --location=${GCP_REGION}"
log_info "  一時停止:    gcloud scheduler jobs pause ${NOTIFY_JOB_NAME} --location=${GCP_REGION}"
log_info "  再開:        gcloud scheduler jobs resume ${NOTIFY_JOB_NAME} --location=${GCP_REGION}"
log_info "  実行履歴:    gcloud logging read 'resource.type=\"cloud_scheduler_job\" AND resource.labels.job_id=\"${NOTIFY_JOB_NAME}\"' --limit=10"
log_info ""

# ========================================
# 6. 失敗時アラートの設定
# ========================================
log_info "--- 失敗時アラートの設定 ---"
log_info "Cloud MonitoringによるCloud Schedulerジョブ失敗アラートを設定します..."

# Monitoring APIの有効化確認
if ! gcloud services list --enabled --filter="name:monitoring.googleapis.com" --format="value(name)" 2>/dev/null | grep -q "monitoring"; then
    log_info "Cloud Monitoring APIを有効化しています..."
    gcloud services enable monitoring.googleapis.com --quiet
fi

# ログベースアラートポリシー用JSONを一時ファイルに作成
ALERT_POLICY_FILE=$(mktemp /tmp/scheduler_alert_policy_XXXXXX.json)

cat > "${ALERT_POLICY_FILE}" << EOF
{
  "displayName": "Cloud Scheduler ジョブ失敗アラート (keiba-pipeline)",
  "conditions": [
    {
      "displayName": "Cloud Scheduler ジョブ失敗",
      "conditionMatchedLog": {
        "filter": "resource.type=\"cloud_scheduler_job\" AND severity=ERROR"
      }
    }
  ],
  "alertStrategy": {
    "notificationRateLimit": {
      "period": "3600s"
    }
  },
  "combiner": "OR",
  "enabled": true
}
EOF

# アラートポリシーの作成/更新
EXISTING_POLICY=$(gcloud monitoring policies list \
    --filter="displayName=\"Cloud Scheduler ジョブ失敗アラート (keiba-pipeline)\"" \
    --format="value(name)" 2>/dev/null | head -1)

if [ -n "${EXISTING_POLICY}" ]; then
    log_info "既存のアラートポリシーが見つかりました（更新はスキップ）: ${EXISTING_POLICY}"
else
    if gcloud monitoring policies create --policy-from-file="${ALERT_POLICY_FILE}" --quiet 2>/dev/null; then
        log_info "アラートポリシーを作成しました"
    else
        log_warn "アラートポリシーの作成に失敗しました"
        log_warn "手動での設定手順:"
        log_warn "  1. GCPコンソール > Monitoring > Alerting を開く"
        log_warn "  2. 「アラートポリシーの作成」をクリック"
        log_warn "  3. ログフィルタ: resource.type=\"cloud_scheduler_job\" AND severity=ERROR"
        log_warn "  4. 通知チャンネルを設定（メール等）"
    fi
fi

# 一時ファイルの削除
rm -f "${ALERT_POLICY_FILE}"

log_info ""
log_info "アラート設定の確認:"
log_info "  GCPコンソール > Monitoring > Alerting で確認できます"
log_info "  通知チャンネル（メール等）は別途コンソールから設定してください"
log_info ""
