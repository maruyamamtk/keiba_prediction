以下のスクリプトを順番に実行して、変更をCloud Runに反映させてください。

1. `./infrastructure/scripts/build_and_push.sh`
2. `./infrastructure/scripts/setup_cloud_run_jobs.sh`
3. `./infrastructure/scripts/deploy_cloud_run.sh`
4. `./infrastructure/scripts/setup_scheduler.sh`

各スクリプトが正常に完了したことを確認してから次のステップに進んでください。
エラーが発生した場合は直ちに停止し、原因を報告してください。

**注意**: このスキルはCloud Runのコード変更を反映するものです。
モデルの再学習が必要な場合は `/retrain-and-deploy` を使用してください。
