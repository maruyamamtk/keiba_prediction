"""
パイプラインAPI

Cloud Run用HTTPエンドポイントを提供する。
- 日次パイプライン: Cloud Schedulerからトリガー
- 全件ロード: 手動トリガー（初回セットアップ/データ補完）
- 特徴量生成: 手動トリガーまたはパイプライン統合
- 予測実行: 翌日レース予測 + BigQuery保存

Issue #57: 日次パイプラインの実装
Issue #58: 過去分全件ロード処理の実装
Issue #59: 特徴量生成パイプラインのCloud Run統合
Issue #20: 日次予測パイプラインの実装
"""

import datetime
import logging
import os
import uuid
from datetime import date
from typing import Optional

from fastapi import BackgroundTasks, FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, field_validator

from src.automation.pipeline.daily_pipeline import DailyPipeline, PipelineResult
from src.automation.pipeline.full_load_pipeline import FullLoadPipeline, FullLoadResult

# ロギング設定（Cloud Logging連携）
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# FastAPIアプリケーション
app = FastAPI(
    title="JRDB Pipeline API",
    description="JRDBデータ処理パイプラインAPI（日次/全件ロード）",
    version="1.1.0",
)


class DailyLoadRequest(BaseModel):
    """日次ロードリクエスト"""

    target_date: Optional[str] = Field(
        default=None,
        description="対象日付（YYYY-MM-DD形式、省略時は当日）",
        json_schema_extra={"example": "2024-01-15"},
    )

    @field_validator("target_date")
    @classmethod
    def validate_date_format(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        try:
            DailyPipeline.parse_target_date(v)
            return v
        except ValueError as e:
            raise ValueError(str(e)) from e


class DailyLoadResponse(BaseModel):
    """日次ロードレスポンス"""

    status: str = Field(description="処理ステータス (success/partial/failed)")
    target_date: str = Field(description="対象日付")
    files_downloaded: int = Field(default=0, description="ダウンロードしたファイル数")
    files_uploaded: int = Field(default=0, description="アップロードしたファイル数")
    files_loaded: int = Field(default=0, description="ロードしたファイル数")
    records_loaded: int = Field(default=0, description="ロードしたレコード数")
    duration_seconds: float = Field(default=0.0, description="処理時間（秒）")
    error_message: Optional[str] = Field(default=None, description="エラーメッセージ")


class FullLoadRequest(BaseModel):
    """全件ロードリクエスト"""

    start_date: Optional[str] = Field(
        default=None,
        description="開始日付（YYYY-MM-DD形式、省略時は全期間）",
        json_schema_extra={"example": "2020-01-01"},
    )
    end_date: Optional[str] = Field(
        default=None,
        description="終了日付（YYYY-MM-DD形式、省略時は全期間）",
        json_schema_extra={"example": "2024-12-31"},
    )

    @field_validator("start_date", "end_date")
    @classmethod
    def validate_date_format(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        try:
            FullLoadPipeline.parse_date(v)
            return v
        except ValueError as e:
            raise ValueError(str(e)) from e


class FullLoadResponse(BaseModel):
    """全件ロードレスポンス"""

    status: str = Field(description="処理ステータス (started/success/partial/failed)")
    job_id: str = Field(description="ジョブID")
    start_date: str = Field(description="開始日付")
    end_date: str = Field(description="終了日付")
    message: str = Field(description="メッセージ")
    files_downloaded: int = Field(default=0, description="ダウンロードしたファイル数")
    files_uploaded: int = Field(default=0, description="アップロードしたファイル数")
    files_loaded: int = Field(default=0, description="ロードしたファイル数")
    records_loaded: int = Field(default=0, description="ロードしたレコード数")
    duration_seconds: float = Field(default=0.0, description="処理時間（秒）")
    error_message: Optional[str] = Field(default=None, description="エラーメッセージ")


class FeatureGenerateRequest(BaseModel):
    """特徴量生成リクエスト"""

    start_date: str = Field(
        description="開始日付（YYYY-MM-DD形式）",
        json_schema_extra={"example": "2024-01-01"},
    )
    end_date: str = Field(
        description="終了日付（YYYY-MM-DD形式）",
        json_schema_extra={"example": "2024-12-31"},
    )

    @field_validator("start_date", "end_date")
    @classmethod
    def validate_date_format(cls, v: str) -> str:
        from datetime import datetime

        try:
            datetime.strptime(v, "%Y-%m-%d")
        except ValueError:
            raise ValueError(
                f"日付はYYYY-MM-DD形式の有効な日付で指定してください: '{v}'"
            )
        return v


class FeatureGenerateResponse(BaseModel):
    """特徴量生成レスポンス"""

    status: str = Field(description="処理ステータス (success/failed)")
    start_date: str = Field(description="開始日付")
    end_date: str = Field(description="終了日付")
    deleted_rows: int = Field(default=0, description="削除した行数")
    inserted_rows: int = Field(default=0, description="挿入した行数")
    elapsed_time: float = Field(default=0.0, description="処理時間（秒）")
    error_message: Optional[str] = Field(default=None, description="エラーメッセージ")


class PredictDailyRequest(BaseModel):
    """翌日予測リクエスト"""

    model_path: str = Field(
        description="モデルファイルパス（ローカルパスまたは gs:// URI）",
        json_schema_extra={"example": "gs://my-project-keiba-models/models/20260101/lgbm_ranker.txt"},
    )
    save_to_bq: bool = Field(
        default=True,
        description="予測結果をBigQueryに保存するか",
    )


class PredictOnDemandRequest(BaseModel):
    """任意日付予測リクエスト"""

    model_path: str = Field(
        description="モデルファイルパス（ローカルパスまたは gs:// URI）",
        json_schema_extra={"example": "gs://my-project-keiba-models/models/20260101/lgbm_ranker.txt"},
    )
    target_dates: list[str] = Field(
        description="予測対象日（YYYY-MM-DD形式、複数指定可）",
        json_schema_extra={"example": ["2026-02-14", "2026-02-15"]},
    )
    save_to_bq: bool = Field(
        default=True,
        description="予測結果をBigQueryに保存するか",
    )

    @field_validator("target_dates")
    @classmethod
    def validate_target_dates(cls, v: list[str]) -> list[str]:
        for d in v:
            try:
                datetime.date.fromisoformat(d)
            except ValueError:
                raise ValueError(
                    f"日付はYYYY-MM-DD形式の有効な日付で指定してください: '{d}'"
                )
        return v


class PredictResponse(BaseModel):
    """予測レスポンス"""

    status: str = Field(description="処理ステータス (success/failed)")
    target_dates: list[str] = Field(description="予測対象日のリスト")
    num_races: int = Field(default=0, description="予測したレース数")
    num_horses: int = Field(default=0, description="予測した頭数")
    saved_to_bq: bool = Field(default=False, description="BigQueryに保存されたか")
    saved_rows: int = Field(default=0, description="BigQueryに保存した行数")
    error_message: Optional[str] = Field(default=None, description="エラーメッセージ")


class HealthResponse(BaseModel):
    """ヘルスチェックレスポンス"""

    status: str = Field(description="サービスステータス")
    version: str = Field(description="APIバージョン")


# グローバルパイプラインインスタンス（遅延初期化）
_pipeline: Optional[DailyPipeline] = None


def get_pipeline() -> DailyPipeline:
    """パイプラインインスタンスを取得（シングルトン）"""
    global _pipeline
    if _pipeline is None:
        _pipeline = DailyPipeline()
    return _pipeline


@app.get("/", response_model=HealthResponse)
async def root():
    """ルートエンドポイント（ヘルスチェック用）"""
    return HealthResponse(status="ok", version="1.1.0")


@app.get("/health", response_model=HealthResponse)
async def health_check():
    """ヘルスチェックエンドポイント"""
    return HealthResponse(status="healthy", version="1.1.0")


@app.post("/api/v1/load/daily", response_model=DailyLoadResponse)
async def load_daily(request: DailyLoadRequest):
    """
    日次データロードを実行

    処理フロー:
    1. JRDBから指定日のデータをダウンロード
    2. GCSにアップロード
    3. BigQueryにロード

    Args:
        request: リクエストボディ

    Returns:
        処理結果
    """
    logger.info(f"日次ロードリクエスト受信: target_date={request.target_date}")

    try:
        pipeline = get_pipeline()
        result = pipeline.run(request.target_date)

        response = DailyLoadResponse(
            status=result.status,
            target_date=result.target_date,
            files_downloaded=result.files_downloaded,
            files_uploaded=result.files_uploaded,
            files_loaded=result.files_loaded,
            records_loaded=result.records_loaded,
            duration_seconds=round(result.duration_seconds, 2),
            error_message=result.error_message,
        )

        if result.status == "failed":
            logger.error(f"日次ロード失敗: {result.error_message}")
            return JSONResponse(status_code=500, content=response.model_dump())

        logger.info(
            f"日次ロード完了: status={result.status}, "
            f"files={result.files_loaded}, records={result.records_loaded}"
        )
        return response

    except ValueError as e:
        logger.warning(f"バリデーションエラー: {e}")
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        logger.error(f"日次ロードエラー: {e}")
        raise HTTPException(status_code=500, detail=str(e)) from e


@app.post("/api/v1/load/daily/async")
async def load_daily_async(
    request: DailyLoadRequest, background_tasks: BackgroundTasks
):
    """
    日次データロードを非同期で実行

    処理はバックグラウンドで実行され、すぐにレスポンスを返す。
    Cloud Schedulerからの呼び出し時に使用。

    Args:
        request: リクエストボディ
        background_tasks: バックグラウンドタスク

    Returns:
        受付結果
    """
    target_date = request.target_date or date.today().strftime("%Y-%m-%d")
    logger.info(f"非同期日次ロードリクエスト受付: target_date={target_date}")

    def run_pipeline():
        try:
            pipeline = DailyPipeline()
            result = pipeline.run(request.target_date)
            logger.info(
                f"非同期日次ロード完了: status={result.status}, "
                f"files={result.files_loaded}, records={result.records_loaded}"
            )
        except Exception as e:
            logger.error(f"非同期日次ロードエラー: {e}")

    background_tasks.add_task(run_pipeline)

    return {
        "status": "accepted",
        "target_date": target_date,
        "message": "パイプラインをバックグラウンドで実行中",
    }


@app.post("/api/v1/load/full", response_model=FullLoadResponse)
async def load_full(
    request: FullLoadRequest, background_tasks: BackgroundTasks
):
    """
    過去分全件ロードを実行（バックグラウンド）

    長時間処理のため、バックグラウンドで実行しすぐにレスポンスを返す。
    初回セットアップやデータ欠損の補完に使用。

    Args:
        request: リクエストボディ
        background_tasks: バックグラウンドタスク

    Returns:
        受付結果
    """
    job_id = str(uuid.uuid4())[:8]
    start_date_str = request.start_date or "全期間"
    end_date_str = request.end_date or "全期間"

    logger.info(
        f"全件ロードリクエスト受付: job_id={job_id}, "
        f"期間={start_date_str}〜{end_date_str}"
    )

    def run_full_load():
        try:
            pipeline = FullLoadPipeline()
            result = pipeline.run(request.start_date, request.end_date)
            logger.info(
                f"全件ロード完了: job_id={job_id}, "
                f"status={result.status}, files={result.files_loaded}, "
                f"records={result.records_loaded}"
            )
        except Exception as e:
            logger.error(f"全件ロードエラー: job_id={job_id}, error={e}")

    background_tasks.add_task(run_full_load)

    return FullLoadResponse(
        status="started",
        job_id=job_id,
        start_date=start_date_str,
        end_date=end_date_str,
        message=f"全件ロードを開始しました（{start_date_str}〜{end_date_str}）",
    )


@app.post("/api/v1/load/full/sync", response_model=FullLoadResponse)
async def load_full_sync(request: FullLoadRequest):
    """
    過去分全件ロードを同期実行

    処理完了まで待機する。テスト用・少量データ用。

    Args:
        request: リクエストボディ

    Returns:
        処理結果
    """
    logger.info(
        f"同期全件ロードリクエスト受信: "
        f"期間={request.start_date}〜{request.end_date}"
    )

    try:
        pipeline = FullLoadPipeline()
        result = pipeline.run(request.start_date, request.end_date)

        response = FullLoadResponse(
            status=result.status,
            job_id=result.job_id,
            start_date=result.start_date,
            end_date=result.end_date,
            message=f"全件ロード{result.status}: {result.files_loaded}ファイル, "
            f"{result.records_loaded}レコード",
            files_downloaded=result.files_downloaded,
            files_uploaded=result.files_uploaded,
            files_loaded=result.files_loaded,
            records_loaded=result.records_loaded,
            duration_seconds=round(result.duration_seconds, 2),
            error_message=result.error_message,
        )

        if result.status == "failed":
            logger.error(f"同期全件ロード失敗: {result.error_message}")
            return JSONResponse(status_code=500, content=response.model_dump())

        return response

    except ValueError as e:
        logger.warning(f"バリデーションエラー: {e}")
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        logger.error(f"同期全件ロードエラー: {e}")
        raise HTTPException(status_code=500, detail=str(e)) from e


@app.post("/api/v1/features/generate", response_model=FeatureGenerateResponse)
async def generate_features(request: FeatureGenerateRequest):
    """
    特徴量生成を同期実行

    指定した日付範囲の特徴量を生成してfeatures.training_dataに書き込む。

    Args:
        request: リクエストボディ

    Returns:
        処理結果
    """
    logger.info(
        f"特徴量生成リクエスト受信: {request.start_date} 〜 {request.end_date}"
    )

    try:
        project_id = os.environ.get("GCP_PROJECT_ID")
        if not project_id:
            raise HTTPException(
                status_code=500, detail="GCP_PROJECT_IDが未設定です"
            )

        from src.ml.features.feature_pipeline import FeaturePipeline

        pipeline = FeaturePipeline(project_id=project_id)
        result = pipeline.run(request.start_date, request.end_date)

        response = FeatureGenerateResponse(
            status="success",
            start_date=result["start_date"],
            end_date=result["end_date"],
            deleted_rows=result["deleted_rows"],
            inserted_rows=result["inserted_rows"],
            elapsed_time=round(result["elapsed_time"], 2),
        )

        logger.info(
            f"特徴量生成完了: inserted={result['inserted_rows']}, "
            f"elapsed={result['elapsed_time']:.2f}s"
        )
        return response

    except ValueError as e:
        logger.warning(f"バリデーションエラー: {e}")
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        logger.error(f"特徴量生成エラー: {e}")
        raise HTTPException(status_code=500, detail=str(e)) from e


@app.post("/api/v1/features/generate/async")
async def generate_features_async(
    request: FeatureGenerateRequest, background_tasks: BackgroundTasks
):
    """
    特徴量生成を非同期実行

    処理はバックグラウンドで実行され、すぐにレスポンスを返す。

    Args:
        request: リクエストボディ
        background_tasks: バックグラウンドタスク

    Returns:
        受付結果
    """
    logger.info(
        f"非同期特徴量生成リクエスト受付: {request.start_date} 〜 {request.end_date}"
    )

    def run_feature_generation():
        try:
            project_id = os.environ.get("GCP_PROJECT_ID")
            if not project_id:
                logger.error("GCP_PROJECT_IDが未設定です")
                return

            from src.ml.features.feature_pipeline import FeaturePipeline

            pipeline = FeaturePipeline(project_id=project_id)
            result = pipeline.run(request.start_date, request.end_date)
            logger.info(
                f"非同期特徴量生成完了: inserted={result['inserted_rows']}, "
                f"elapsed={result['elapsed_time']:.2f}s"
            )
        except Exception as e:
            logger.error(f"非同期特徴量生成エラー: {e}")

    background_tasks.add_task(run_feature_generation)

    return {
        "status": "accepted",
        "start_date": request.start_date,
        "end_date": request.end_date,
        "message": "特徴量生成をバックグラウンドで実行中",
    }


def _resolve_model_path(model_path: str, project_id: str) -> tuple[str, Optional[str]]:
    """
    モデルファイルパスを解決する

    GCS URI（gs://...）の場合はローカルの一時ディレクトリにダウンロードし、
    ローカルパスを返す。ローカルパスの場合はそのまま返す。

    Args:
        model_path: モデルファイルパス（ローカルパスまたは gs:// URI）
        project_id: GCPプロジェクトID

    Returns:
        (ローカルパス, 一時ディレクトリパス) のタプル。
        ローカルパスの場合は一時ディレクトリは None。
    """
    if not model_path.startswith("gs://"):
        return model_path, None

    import tempfile
    from google.cloud import storage as gcs

    # gs://bucket-name/path/to/model.txt を解析
    gcs_path = model_path[len("gs://"):]
    bucket_name, blob_name = gcs_path.split("/", 1)

    tmpdir = tempfile.mkdtemp(prefix="keiba_model_")
    local_file = os.path.join(tmpdir, os.path.basename(blob_name))

    logger.info(f"GCSからモデルをダウンロード: {model_path} -> {local_file}")
    client = gcs.Client(project=project_id)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.download_to_filename(local_file)
    logger.info("モデルのダウンロード完了")

    return local_file, tmpdir


def _run_predict(
    model_path: str,
    target_dates: list[datetime.date],
    save_to_bq: bool,
    project_id: str,
) -> dict:
    """
    予測パイプラインを実行して結果を返す内部関数

    Args:
        model_path: モデルファイルパス（ローカルパスまたは gs:// URI）
        target_dates: 予測対象日のリスト
        save_to_bq: BigQueryに保存するか
        project_id: GCPプロジェクトID

    Returns:
        予測結果の辞書（num_races, num_horses, saved_to_bq, saved_rows）
    """
    import shutil

    from src.models.predict import predict_pipeline, save_predictions_to_bq
    from src.models.train import load_config

    # GCS URI の場合はダウンロードしてローカルパスに変換
    local_model_path, tmpdir = _resolve_model_path(model_path, project_id)

    try:
        config = load_config()
        result_df = predict_pipeline(
            project_id=project_id,
            execution_date=datetime.date.today(),
            config=config,
            model_path=local_model_path,
            target_dates=target_dates,
        )

        num_races = int(result_df["race_id"].nunique()) if len(result_df) > 0 else 0
        num_horses = len(result_df)
        bq_saved = False
        saved_rows = 0

        if save_to_bq and len(result_df) > 0:
            saved_rows = save_predictions_to_bq(
                result_df=result_df,
                project_id=project_id,
            )
            bq_saved = True

        return {
            "num_races": num_races,
            "num_horses": num_horses,
            "saved_to_bq": bq_saved,
            "saved_rows": saved_rows,
        }
    finally:
        # GCS からダウンロードした一時ファイルを削除
        if tmpdir is not None:
            shutil.rmtree(tmpdir, ignore_errors=True)
            logger.info(f"一時ディレクトリを削除しました: {tmpdir}")


@app.post("/api/v1/predict/daily", response_model=PredictResponse)
async def predict_daily(request: PredictDailyRequest):
    """
    翌日レースの予測を実行してBigQueryに保存する

    実行日の翌日（土日）のレースを予測対象とする。
    Cloud Schedulerから前日PM 9:00に呼び出されることを想定。

    Args:
        request: リクエストボディ

    Returns:
        予測結果
    """
    logger.info(f"日次予測リクエスト受信: model_path={request.model_path}")

    project_id = os.environ.get("GCP_PROJECT_ID")
    if not project_id:
        raise HTTPException(status_code=500, detail="GCP_PROJECT_IDが未設定です")

    try:
        from src.models.train import compute_week_boundaries

        # 翌日を基準に今週の土日を算出
        tomorrow = datetime.date.today() + datetime.timedelta(days=1)
        saturday, sunday = compute_week_boundaries(tomorrow)
        target_dates = [saturday, sunday]

        result = _run_predict(
            model_path=request.model_path,
            target_dates=target_dates,
            save_to_bq=request.save_to_bq,
            project_id=project_id,
        )

        logger.info(
            f"日次予測完了: {result['num_races']}レース, {result['num_horses']}頭, "
            f"saved={result['saved_to_bq']}"
        )
        return PredictResponse(
            status="success",
            target_dates=[d.isoformat() for d in target_dates],
            **result,
        )

    except Exception as e:
        logger.error(f"日次予測エラー: {e}")
        raise HTTPException(status_code=500, detail=str(e)) from e


@app.post("/api/v1/predict/on-demand", response_model=PredictResponse)
async def predict_on_demand(request: PredictOnDemandRequest):
    """
    任意日付を指定してレース予測を実行し、BigQueryに保存する

    指定した日付のレースを予測対象とする。
    手動実行やバックテスト用途での利用を想定。

    Args:
        request: リクエストボディ

    Returns:
        予測結果
    """
    logger.info(
        f"オンデマンド予測リクエスト受信: "
        f"target_dates={request.target_dates}, model_path={request.model_path}"
    )

    project_id = os.environ.get("GCP_PROJECT_ID")
    if not project_id:
        raise HTTPException(status_code=500, detail="GCP_PROJECT_IDが未設定です")

    try:
        target_dates = [
            datetime.date.fromisoformat(d) for d in request.target_dates
        ]

        result = _run_predict(
            model_path=request.model_path,
            target_dates=target_dates,
            save_to_bq=request.save_to_bq,
            project_id=project_id,
        )

        logger.info(
            f"オンデマンド予測完了: {result['num_races']}レース, {result['num_horses']}頭, "
            f"saved={result['saved_to_bq']}"
        )
        return PredictResponse(
            status="success",
            target_dates=request.target_dates,
            **result,
        )

    except Exception as e:
        logger.error(f"オンデマンド予測エラー: {e}")
        raise HTTPException(status_code=500, detail=str(e)) from e


def create_app() -> FastAPI:
    """アプリケーションファクトリ"""
    return app


if __name__ == "__main__":
    import uvicorn

    port = int(os.environ.get("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)
