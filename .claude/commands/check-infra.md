Cloud Run / Cloud Scheduler のインフラ設定をチェックし、コスト・安全性リスクを報告してください。

## 確認コマンド

```bash
echo "=== Cloud Run Service ===" && \
gcloud run services describe keiba-pipeline \
  --region=asia-northeast1 \
  --format="yaml(
    spec.template.metadata.annotations,
    spec.template.spec.containers[0].resources,
    status.latestReadyRevisionName
  )" && \
echo "" && \
echo "=== Cloud Scheduler Jobs ===" && \
gcloud scheduler jobs list \
  --location=asia-northeast1 \
  --format="table(name,schedule,state,httpTarget.uri)"
```

## チェックリスト（結果を以下の基準で評価してください）

### Cloud Run Service（keiba-pipeline）

| 項目 | 期待値 | 問題時のリスク |
|---|---|---|
| `autoscaling.knative.dev/minScale` | `0`（または未設定） | アイドル課金が発生（月¥7,000〜） |
| memory | `4Gi` | 8Gi だとメモリ課金が増加 |
| cpu | `2` | 変更不要 |

### モデル再学習について

- モデル再学習は **ローカル月次自動フロー**（`scripts/monthly_retrain.py`・launchd
  `com.keiba.monthly-retrain`・毎月第1月曜 AM1:00）に移行済み。
- 旧 `weekly-model-retrain`（Cloud Scheduler）と `keiba-model-retrain`（Cloud Run Job）は
  **廃止済み**。これらが再び存在していたら削除対象（OOM でサイレント失敗するため）。

### Cloud Scheduler

- `weekly-model-retrain` が存在していたら**異常**（削除済みのはず）。見つかったら削除する。
- 不明なジョブが ENABLED になっていないか確認

## 問題があった場合の対処

**min-instances が 1 以上の場合:**

```bash
# 直ちに /deploy を実行して min-instances=0 を反映する
# （deploy_cloud_run.sh には min-instances=0 が設定済み）
```

**不明な Scheduler ジョブが存在する場合:**

```bash
# ジョブの停止
gcloud scheduler jobs pause <JOB_NAME> --location=asia-northeast1
```
