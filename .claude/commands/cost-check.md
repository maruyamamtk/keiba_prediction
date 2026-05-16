GCPコストに影響する設定と状態を確認してください。以下のコマンドを順番に実行し、結果を日本語で解説してください。

## 1. Cloud Run Service の設定確認

```bash
gcloud run services describe keiba-pipeline \
  --region=asia-northeast1 \
  --format="table(
    metadata.name,
    spec.template.metadata.annotations['autoscaling.knative.dev/minScale'],
    spec.template.spec.containers[0].resources.limits.memory,
    spec.template.spec.containers[0].resources.limits.cpu
  )"
```

**チェックポイント:**
- `min-instances` が 0 であること（1以上だとアイドル課金が発生）
- `memory` が 4Gi 以下であること（8Gi はコストが高い）

## 2. Cloud Run Jobs の設定確認

```bash
gcloud run jobs describe keiba-model-retrain \
  --region=asia-northeast1 \
  --format="table(
    metadata.name,
    spec.template.spec.template.spec.containers[0].resources.limits.memory,
    spec.template.spec.template.spec.containers[0].resources.limits.cpu
  )"
```

## 3. Cloud Scheduler ジョブの状態確認

```bash
gcloud scheduler jobs list \
  --location=asia-northeast1 \
  --format="table(name,schedule,state)"
```

**チェックポイント:** 不要なジョブが ENABLED になっていないこと。

## 4. GCP 予算アラートの確認

```bash
BILLING_ACCOUNT=$(gcloud billing projects describe keiba-prediction-1768734113 \
  --format="value(billingAccountName)" | sed 's|billingAccounts/||')
echo "Billing Account: ${BILLING_ACCOUNT}"
gcloud billing budgets list --billing-account="${BILLING_ACCOUNT}" \
  --format="table(displayName,amount.specifiedAmount.units,thresholdRules[0].thresholdPercent)"
```

## 5. 結果の解釈

以下の観点でコストリスクを評価し、問題があれば対処法を提示してください：

- min-instances が 1 以上 → 直ちに 0 に戻すよう `/deploy` 実行を促す
- memory が 8Gi 以上の Service → 4Gi への変更を提案
- 不要な Scheduler ジョブが ENABLED → 停止コマンドを提示
- 予算アラートが未設定 → 設定手順を案内（後述）

**予算アラートが未設定の場合の設定手順:**

```bash
BILLING_ACCOUNT=$(gcloud billing projects describe keiba-prediction-1768734113 \
  --format="value(billingAccountName)" | sed 's|billingAccounts/||')

gcloud billing budgets create \
  --billing-account="${BILLING_ACCOUNT}" \
  --display-name="keiba-prediction 月次予算アラート" \
  --budget-amount=5000JPY \
  --threshold-rule=percent=80 \
  --threshold-rule=percent=100
```

作成後、GCPコンソール（Billing > Budgets & alerts）から通知先メールアドレスを設定してください。
