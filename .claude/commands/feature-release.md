---
description: 特徴量追加Issueを実装し、リーク検証・モデル比較を経てPR作成・マージまで行う
argument-hint: Issue番号 (例: 267)
user-invocable: true
---

Issue#$ARGUMENTS を以下の手順で実装してください。

## 実装フロー

### 1. ブランチ作成
```bash
git checkout -b feature/issue-$ARGUMENTS
```

### 2. 実装 & テスト作成
- SQL・特徴量ファイルを変更する場合は `/check-leak` を実行してデータリーク確認
- 対応するテストを追加・修正する

### 3. テスト実行
```bash
.venv/bin/pytest tests/ -q --tb=short
```
失敗があれば修正してから次へ。

### 4. モデル精度比較（特徴量・SQLを変更した場合は必須）

`/model-compare` を実行してLGBMRankerのベースライン比較を確認・提示:
- 既存モデルと新モデルの学習期間・検証期間
- NDCG@3 / AUC / Recall@3 の比較と差分
- 総合判定（マージ可 / 要修正）

**多段階モデルの追加評価（任意）**: 全指標に大きな変化がある場合は ranker_multi / regression / classifier を `--skip-gcs-upload` で個別に確認する。

総合判定が ❌ の場合はユーザーに確認を求めてから続行するか判断する。

### 5. コミット & PR 作成
コミット後、PR 本文には以下を必ず含める:
- モデル比較の学習・検証期間表
- 精度比較表（NDCG@3 / AUC / Recall@3）
- 総合判定
- テスト計画（`/check-leak` 実施済み・pytest 通過を明記）

### 6. 自己レビュー
`gh pr diff` でコードレビューを実施し、要修正事項を列挙:
- マージ前の修正事項があれば修正 → 再レビュー
- なければ次のステップへ

### 7. マージ
```bash
gh pr merge <PR番号> --squash --delete-branch
```

### 8. コンテキスト圧縮
`/compact` を実行する。

## Rate Limit 対応
使用量が 90% を超えた場合は `/hangover` を実行して作業状況を保存してください。
