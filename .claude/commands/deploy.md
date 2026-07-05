以下のスクリプトを順番に実行して、変更をCloud Runに反映させてください。

## 前提条件チェック（校正本番反映時・Issue #417）

校正済みモデル（meta.json にアイソトニック校正器を持つ）をデプロイする場合は、以下が揃って
いることを確認してから進んでください（揃っていなければ `/retrain-and-deploy` のステップ3.5を先に実施）:

- [ ] 校正済みモデルが GCS にアップロード済み（meta.json に `calibration_isotonic` を含む）。
- [ ] **校正済み確率で再最適化済みの `config/strategy_config.yaml`**（`prob_weight_r: 1.0` 固定）。
      校正済み確率と本番の確率分布を一致させるため、再最適化していない戦略パラメータはデプロイしない。

1. `./infrastructure/scripts/build_and_push.sh`
2. `./infrastructure/scripts/deploy_cloud_run.sh`
3. `./infrastructure/scripts/setup_scheduler.sh`

各スクリプトが正常に完了したことを確認してから次のステップに進んでください。
エラーが発生した場合は直ちに停止し、原因を報告してください。

**注意**: このスキルはCloud Runのコード変更を反映するものです。
モデルの再学習が必要な場合は `/retrain-and-deploy` を使用してください。
