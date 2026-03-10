# LINE Messaging API セットアップ・動作確認手順

このドキュメントでは、LINE Messaging API Webhook Bot（Issue #25）のセットアップと動作確認の手順を説明します。

---

## 1. LINE Developers コンソールでのチャネル作成

1. [LINE Developers コンソール](https://developers.line.biz/) にログイン
2. プロバイダーを作成（または既存のものを選択）
3. **Messaging API** チャネルを新規作成
4. チャネル作成後、以下の情報を控える

| 項目 | 確認場所 |
|------|---------|
| `LINE_CHANNEL_SECRET` | チャネル基本設定 → チャネルシークレット |
| `LINE_CHANNEL_ACCESS_TOKEN` | Messaging API設定 → チャネルアクセストークン（長期） → 発行 |
| `LINE_USER_ID` | Messaging API設定 → あなたのユーザーID（テスト送信先） |

---

## 2. 環境変数の設定

### ローカル開発時

```bash
export LINE_CHANNEL_ACCESS_TOKEN="your_channel_access_token"
export LINE_CHANNEL_SECRET="your_channel_secret"
export LINE_USER_ID="your_user_id"
export GCP_PROJECT_ID="your_project_id"
```

`.env` ファイルを使う場合:

```
LINE_CHANNEL_ACCESS_TOKEN=your_channel_access_token
LINE_CHANNEL_SECRET=your_channel_secret
LINE_USER_ID=your_user_id
GCP_PROJECT_ID=your_project_id
```

### Cloud Run（本番）

Secret Manager に登録してCloud Runの環境変数として参照する（詳細は [infrastructure/README.md](../infrastructure/README.md) 参照）。

---

## 3. 動作確認手順

### 3.1 ユニットテスト（BigQuery不要・最も手軽）

```bash
python3 -m pytest tests/test_line_webhook.py -v
```

35件のテストで以下を確認できます:
- 署名検証ロジック（正常・異常系）
- 「日付 競馬場名 レース番号」形式のパース
- 予測テーブル・推奨馬券メッセージのフォーマット
- BQデータなし時のエラーメッセージ返答
- LINE API 呼び出し（モック）

---

### 3.2 メッセージパースの確認

「日付 競馬場名 レース番号」形式が正しく解析されるか確認します。

```bash
python3 -c "
from src.automation.api.line_webhook import parse_race_query

tests = [
    '2026-03-08 阪神 12R',    # フルフォーマット
    '2026/03/08 東京 11R',    # スラッシュ区切り
    '03/08 阪神 12R',         # 年省略
    '03/08 阪神 12',          # R なし
    '今日のレース',            # -> None（無視）
    'ヘルプ',                  # -> None（無視）
]
for t in tests:
    result = parse_race_query(t)
    print(f'{t!r:30} -> {result}')
"
```

---

### 3.3 送信テスト（プッシュ通知）

自分のLINEアカウントにテストメッセージを送信します。

```bash
python3 -c "
import os
from dotenv import load_dotenv
load_dotenv()  # .env から環境変数を読み込む

from src.utils.line_notify import push_messages, text_message

push_messages(
    channel_access_token=os.environ['LINE_CHANNEL_ACCESS_TOKEN'],
    to=os.environ['LINE_USER_ID'],
    messages=[text_message('テスト送信: 動作確認OK')]
)
print('送信成功')
"
```

---

### 3.4 受信テスト（Webhook エンドポイント）

#### ステップ1: ローカルサーバーを起動

```bash
uvicorn src.automation.api.app:app --reload --port 8080
```

#### ステップ2: ngrok で外部公開（LINEからのWebhookを受信するため）

> **注意: ngrok はローカル開発時のみ必要です。**
> 本番（Cloud Run）では最初から公開 HTTPS URL が発行されるため、ngrok は不要です。

```bash
# ngrok がない場合: brew install ngrok
ngrok http 8080
```

ngrokが発行した `https://xxxx.ngrok.io` を LINE Developers コンソール → Messaging API設定 → Webhook URL に設定。
「検証」ボタンを押して `{"statusCode": 200}` が返れば接続成功。

**本番（Cloud Run）の場合:**
Cloud Run デプロイ後に発行される URL をそのまま設定します。ngrok・サインアップは不要です。
```
https://xxx.run.app/api/v1/line/webhook
```

#### ステップ3: ローカルで直接 Webhook リクエストを模擬

```bash
python3 -c "
import hashlib, hmac, json, requests, os
from base64 import b64encode
from dotenv import load_dotenv
load_dotenv()  # .env から環境変数を読み込む

secret = os.environ.get('LINE_CHANNEL_SECRET', '')
body = json.dumps({
    'events': [{
        'type': 'message',
        'replyToken': 'ffffffffffffffffffffffffffffffff',
        'message': {'type': 'text', 'text': '2026-03-08 阪神 12R'}
    }]
}).encode()

sig = b64encode(hmac.new(secret.encode(), body, hashlib.sha256).digest()).decode()

resp = requests.post(
    'http://localhost:8080/api/v1/line/webhook',
    data=body,
    headers={'Content-Type': 'application/json', 'X-Line-Signature': sig}
)
print(resp.status_code, resp.json())
"
```

> **注意**: ローカルテストでは BQ にデータがない場合「見つかりませんでした」のリプライが送信されます。
> また `replyToken` は本番では1回限り有効・60秒で期限切れのため、実際のLINEへのリプライは失敗します（エラーログを確認）。

---

### 3.5 BQ データを使ったエンドツーエンドテスト

BQ に当日の予測データとオッズデータが入っている状態で確認します。

```bash
# BQ 認証
gcloud auth application-default login

# ローカルサーバー起動（.env の値が自動で読み込まれる）
uvicorn src.automation.api.app:app --reload --port 8080
```

その後、3.4 のステップ3のリクエストを実行。サーバーログで BQ クエリが走り、2通のメッセージが生成されることを確認します。

---

## 4. 受け付けるメッセージ形式

| 形式 | 例 |
|------|----|
| `YYYY-MM-DD 競馬場名 NNR` | `2026-03-08 阪神 12R` |
| `YYYY/MM/DD 競馬場名 NNR` | `2026/03/08 東京 11R` |
| `MM/DD 競馬場名 NNR` | `03/08 阪神 12R`（年は今年） |
| `MM-DD 競馬場名 NNR` | `03-08 中山 5R` |
| R なし | `2026-03-08 阪神 12` |

**対応競馬場**: 札幌・函館・福島・新潟・東京・中山・中京・京都・阪神・小倉

上記以外のメッセージ（「今日のレース」「ヘルプ」など）は**無視**されます（返答なし）。

---

## 5. 返答メッセージの例

### メッセージ1: 予測テーブル

```
============================================
Race: 阪神 12R (2026-03-08)
============================================
  予測順  馬番 馬名              スコア    複勝率  オッズ  期待値
--------------------------------------------
    1   12 ニューオーリンズ  +0.6762  52.0%   2.5倍   1.30
    2   13 チュウワチーフ    +0.6613  51.2%   3.0倍   1.54
    3   11 ペイドラロワール  +0.4155  40.0%   4.2倍   1.68
    ...
```

### メッセージ2: 推奨馬券リスト

```
🏇 推奨馬券リスト
【阪神 12R】(2026-03-08)
パターン: 標準型
──────────────────────────────
  [複勝] 馬番12 ニューオーリンズ
    オッズ: 2.5倍  推奨額: ¥2,000
  [複勝] 馬番13 チュウワチーフ
    オッズ: 3.0倍  推奨額: ¥1,500
──────────────────────────────
合計投資額: ¥3,500
```

---

## 6. 関連ファイル

| ファイル | 役割 |
|----------|------|
| `src/automation/api/line_webhook.py` | Webhookハンドラ（署名検証・メッセージパース・返答生成） |
| `src/utils/line_notify.py` | LINE リプライ/プッシュ送信ヘルパー |
| `src/automation/api/app.py` | `POST /api/v1/line/webhook` エンドポイント |
| `tests/test_line_webhook.py` | ユニットテスト（35件） |

---

## 7. トラブルシューティング

| 症状 | 原因・対処 |
|------|-----------|
| `400 Invalid signature` | `LINE_CHANNEL_SECRET` が間違っているか未設定 |
| `500 GCP_PROJECT_IDが未設定です` | 環境変数 `GCP_PROJECT_ID` を設定する |
| 返答が来ない | メッセージ形式が「日付 競馬場名 レース番号」になっているか確認 |
| 「見つかりませんでした」と返答 | BQ に当日の `predictions.daily_predictions` データがない |
| LINE リプライが届かない | `replyToken` の期限切れ（60秒）またはアクセストークンが無効 |
