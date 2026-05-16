#!/bin/bash
# deploy_cloud_run.sh 実行後に Cloud Run の min-instances を自動チェックするフック
# PostToolUse(Bash) で起動され、stdin から JSON を受け取る

INPUT=$(cat)

# deploy_cloud_run.sh が実行されたときのみチェックする
if ! echo "$INPUT" | grep -q "deploy_cloud_run"; then
    exit 0
fi

# チェック実行（gcloud が使えない環境ではスキップ）
if ! command -v gcloud &>/dev/null; then
    exit 0
fi

MIN_SCALE=$(gcloud run services describe keiba-pipeline \
    --region=asia-northeast1 \
    --format="value(spec.template.metadata.annotations['autoscaling.knative.dev/minScale'])" \
    2>/dev/null)

MEMORY=$(gcloud run services describe keiba-pipeline \
    --region=asia-northeast1 \
    --format="value(spec.template.spec.containers[0].resources.limits.memory)" \
    2>/dev/null)

# 問題があれば警告を stderr に出力（Claude Code に表示される）
WARNINGS=""

if [ -n "$MIN_SCALE" ] && [ "$MIN_SCALE" -gt 0 ] 2>/dev/null; then
    WARNINGS="${WARNINGS}\n⚠️  [コスト警告] min-instances=${MIN_SCALE} が設定されています。アイドル課金が発生します（月約¥7,000）。deploy_cloud_run.sh の設定を確認してください。"
fi

if [ "$MEMORY" = "8Gi" ]; then
    WARNINGS="${WARNINGS}\n⚠️  [コスト警告] memory=8Gi が設定されています。通常運用は 4Gi で十分です。"
fi

if [ -n "$WARNINGS" ]; then
    echo -e "\n========== Cloud Run コスト設定チェック ==========${WARNINGS}\n=================================================" >&2
    exit 1  # Claude Code にフック失敗として通知
else
    echo "[OK] Cloud Run コスト設定: min-instances=${MIN_SCALE:-0}, memory=${MEMORY}" >&2
fi
