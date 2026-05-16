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
echo "=== Cloud Run Jobs ===" && \
gcloud run jobs describe keiba-model-retrain \
  --region=asia-northeast1 \
  --format="yaml(
    spec.template.spec.template.spec.containers[0].resources,
    spec.template.spec.taskCount,
    spec.template.spec.maxRetries
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

### Cloud Run Jobs（keiba-model-retrain）

| 項目 | 期待値 | 備考 |
|---|---|---|
| memory | `8Gi` | 学習時のみ起動するため許容範囲 |
| maxRetries | `0` | 再学習の重複実行を防ぐ |

### Cloud Scheduler

- `weekly-model-retrain` が ENABLED でも問題なし（Cloud Run Jobs を起動するが、ローカル学習が主フローのため実害は小さい）
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
