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

import asyncio
import datetime
import logging
import os
import uuid
from datetime import date

from zoneinfo import ZoneInfo

from typing import Optional

from fastapi import BackgroundTasks, FastAPI, HTTPException, Request
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


def _today_jst() -> datetime.date:
    """JSTの今日の日付を返す（Cloud RunはUTCで動くため date.today() では日付がズレる）"""
    return datetime.datetime.now(ZoneInfo("Asia/Tokyo")).date()


# 長時間バックグラウンドタスクの参照を保持してGCによる早期終了を防ぐ
_long_running_tasks: set[asyncio.Task] = set()

# IPAT購入tick（_purchase_pipeline_async）1回あたりの処理時間予算（秒）。
# Cloud Runのリクエストタイムアウト（900秒）に対し十分な余裕を残し、超過後は
# 残りのレースを次tickに委ねる（Issue #433, /code-review指摘）。
TICK_TIME_BUDGET_SECONDS = 600

# FastAPIアプリケーション
app = FastAPI(
    title="JRDB Pipeline API",
    description="JRDBデータ処理パイプラインAPI（日次/全件ロード）",
    version="1.1.0",
)


class DailyLoadRequest(BaseModel):
    """日次ロードリクエスト"""

    target_date: str | None = Field(
        default=None,
        description="対象日付（YYYY-MM-DD形式、省略時は当日）",
        json_schema_extra={"example": "2024-01-15"},
    )

    @field_validator("target_date")
    @classmethod
    def validate_date_format(cls, v: str | None) -> str | None:
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
    error_message: str | None = Field(default=None, description="エラーメッセージ")


class FullLoadRequest(BaseModel):
    """全件ロードリクエスト"""

    start_date: str | None = Field(
        default=None,
        description="開始日付（YYYY-MM-DD形式、省略時は全期間）",
        json_schema_extra={"example": "2020-01-01"},
    )
    end_date: str | None = Field(
        default=None,
        description="終了日付（YYYY-MM-DD形式、省略時は全期間）",
        json_schema_extra={"example": "2024-12-31"},
    )

    @field_validator("start_date", "end_date")
    @classmethod
    def validate_date_format(cls, v: str | None) -> str | None:
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
    error_message: str | None = Field(default=None, description="エラーメッセージ")


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
    error_message: str | None = Field(default=None, description="エラーメッセージ")


class PredictDailyRequest(BaseModel):
    """翌日予測リクエスト"""

    model_path: str | None = Field(
        default=None,
        description="モデルファイルパス（ローカルパスまたは gs:// URI）。未指定時はGCSから最新モデルを自動取得。",
        json_schema_extra={"example": "gs://my-project-keiba-models/lgbm_ranker_multi/20260101/lgbm_ranker_multi_20260101.txt"},
    )
    save_to_bq: bool = Field(
        default=True,
        description="予測結果をBigQueryに保存するか",
    )
    save_to_gcs: bool = Field(
        default=True,
        description="予測結果をGCSに保存するか",
    )


class PredictOnDemandRequest(BaseModel):
    """任意日付予測リクエスト"""

    model_path: str | None = Field(
        default=None,
        description="モデルファイルパス（ローカルパスまたは gs:// URI）。未指定時はGCSから最新モデルを自動取得。",
        json_schema_extra={"example": "gs://my-project-keiba-models/lgbm_ranker_multi/20260101/lgbm_ranker_multi_20260101.txt"},
    )
    target_dates: list[str] = Field(
        description="予測対象日（YYYY-MM-DD形式、複数指定可）",
        json_schema_extra={"example": ["2026-02-14", "2026-02-15"]},
    )
    save_to_bq: bool = Field(
        default=True,
        description="予測結果をBigQueryに保存するか",
    )
    save_to_gcs: bool = Field(
        default=False,
        description="予測結果をGCSに保存するか",
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
    saved_to_gcs: bool = Field(default=False, description="GCSに保存されたか")
    gcs_uri: str | None = Field(default=None, description="GCS保存先URI")
    error_message: str | None = Field(default=None, description="エラーメッセージ")


class HealthResponse(BaseModel):
    """ヘルスチェックレスポンス"""

    status: str = Field(description="サービスステータス")
    version: str = Field(description="APIバージョン")


class RetrainRequest(BaseModel):
    """モデル再学習リクエスト"""

    execution_date: str | None = Field(
        default=None,
        description="実行日（YYYY-MM-DD形式、省略時は今日）",
        json_schema_extra={"example": "2026-04-07"},
    )
    n_trials: int | None = Field(
        default=None,
        description="Optuna trial数（省略時はconfigから取得）",
        json_schema_extra={"example": 50},
    )
    tune_timeout: int | None = Field(
        default=None,
        description="チューニングタイムアウト秒数（省略時はconfigから取得）",
        json_schema_extra={"example": 3600},
    )

    @field_validator("execution_date")
    @classmethod
    def validate_date_format(cls, v: str | None) -> str | None:
        if v is None:
            return v
        try:
            datetime.date.fromisoformat(v)
        except ValueError:
            raise ValueError(
                f"日付はYYYY-MM-DD形式の有効な日付で指定してください: '{v}'"
            )
        return v


class RetrainResponse(BaseModel):
    """モデル再学習レスポンス"""

    status: str = Field(description="処理ステータス (success/failed)")
    execution_date: str = Field(description="実行日")
    gcs_uri: str | None = Field(default=None, description="GCS保存先URI")
    metrics: dict = Field(default_factory=dict, description="検証指標（ndcg@3, recall@3, auc, num_races）")
    tuning: dict | None = Field(default=None, description="チューニング結果（best_value, n_trials 等）")
    error_message: str | None = Field(default=None, description="エラーメッセージ")


# グローバルパイプラインインスタンス（遅延初期化）
_pipeline: DailyPipeline | None = None


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
    target_date = request.target_date or _today_jst().strftime("%Y-%m-%d")
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
async def load_full(request: FullLoadRequest):
    """
    過去分全件ロードを実行（バックグラウンド）

    長時間処理のため、asyncio.create_task でリクエストライフサイクルから切り離して実行。
    BackgroundTasks は timeoutSeconds でキャンセルされるため使用しない。
    初回セットアップやデータ欠損の補完に使用。

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

    # asyncio.create_task でリクエストタイムアウトから独立させる
    # タスク参照を保持してGCによる早期終了を防ぐ
    task = asyncio.create_task(asyncio.to_thread(run_full_load))
    _long_running_tasks.add(task)
    task.add_done_callback(_long_running_tasks.discard)

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

        from src.automation.pipeline.full_load_pipeline import rebuild_pedigree_table
        from src.ml.features.feature_pipeline import FeaturePipeline

        logger.info("raw.pedigree 再構築を開始します")
        try:
            pedigree_stats = rebuild_pedigree_table(project_id)
            logger.info(
                f"raw.pedigree 再構築完了: dam_id解決率={pedigree_stats['resolution_pct']}%"
            )
        except Exception as e:
            logger.warning(f"raw.pedigree 再構築エラー（特徴量生成は継続）: {e}")

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


class TeDailyRequest(BaseModel):
    as_of_date: Optional[str] = Field(
        default=None,
        description="TE計算対象日 (YYYY-MM-DD)。未指定時は実行日（JST当日）を使用",
    )


class TeDailyResponse(BaseModel):
    status: str
    as_of_date: str
    inserted_rows: int
    elapsed_time: float


@app.post("/api/v1/features/te-daily", response_model=TeDailyResponse)
async def run_te_daily(request: TeDailyRequest):
    """
    entity_te_daily 日次バッチを同期実行

    指定日時点のエンティティ別 Target Encoding 値を GROUP BY で計算し、
    features.entity_te_daily に追記する。
    毎朝 7:45 JST に Cloud Scheduler から呼び出す。as_of_date 省略時は当日を使用。
    """
    import datetime as _dt
    as_of_date = request.as_of_date or _dt.datetime.now(_dt.timezone(
        _dt.timedelta(hours=9)
    )).date().isoformat()
    logger.info(f"entity_te_daily バッチ開始: {as_of_date}")
    try:
        project_id = os.environ.get("GCP_PROJECT_ID")
        if not project_id:
            raise HTTPException(status_code=500, detail="GCP_PROJECT_IDが未設定です")

        from src.ml.features.feature_pipeline import FeaturePipeline

        pipeline = FeaturePipeline(project_id=project_id)
        result = pipeline.run_te_daily(as_of_date)

        logger.info(
            f"entity_te_daily バッチ完了: inserted={result['inserted_rows']}, "
            f"elapsed={result['elapsed_time']:.2f}s"
        )
        return TeDailyResponse(
            status="success",
            as_of_date=result["as_of_date"],
            inserted_rows=result["inserted_rows"],
            elapsed_time=round(result["elapsed_time"], 2),
        )
    except ValueError as e:
        logger.warning(f"バリデーションエラー: {e}")
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        logger.error(f"entity_te_daily バッチエラー: {e}")
        raise HTTPException(status_code=500, detail=str(e)) from e


def _get_latest_model_from_gcs(project_id: str, bucket_suffix: str = "keiba-models", prefix: str = "lgbm_ranker_multi/") -> str:
    """
    GCSから最新のモデルファイルURIを取得する

    gs://{project_id}-{bucket_suffix}/{prefix} 以下のファイルを日付フォルダ名で降順ソートし、
    最新の日付フォルダにある .txt ファイルの gs:// URIを返す。

    GCS上のパス形式: {prefix}{date}/{model_name}.txt
    例: lgbm_ranker_multi/20260217/lgbm_ranker_multi_20260217.txt

    Args:
        project_id: GCPプロジェクトID
        bucket_suffix: バケット名のサフィックス
        prefix: GCS内のプレフィックス（例: "lgbm_ranker_multi/"）

    Returns:
        最新モデルの gs:// URI

    Raises:
        FileNotFoundError: モデルファイルが見つからない場合
    """
    from google.cloud import storage as gcs

    bucket_name = f"{project_id}-{bucket_suffix}"
    logger.info(f"GCSから最新モデルを検索: gs://{bucket_name}/{prefix}")

    client = gcs.Client(project=project_id)
    bucket = client.bucket(bucket_name)

    blobs = list(bucket.list_blobs(prefix=prefix))
    model_blobs = [b for b in blobs if b.name.endswith(".txt")]

    if not model_blobs:
        raise FileNotFoundError(
            f"モデルファイルが見つかりません: gs://{bucket_name}/{prefix}"
        )

    # 日付フォルダ名（YYYYMMDD）を明示的に抽出して降順ソート
    # パス形式: {prefix}{date}/{model_name}.txt 例: lgbm_ranker_multi/20260217/lgbm_ranker_multi_20260217.txt
    latest_blob = max(model_blobs, key=lambda b: b.name.split("/")[-2])
    gcs_uri = f"gs://{bucket_name}/{latest_blob.name}"
    logger.info(f"最新モデルを取得: {gcs_uri}")
    return gcs_uri


def _run_predict(
    model_path: str | None,
    target_dates: list[datetime.date],
    save_to_bq: bool,
    project_id: str,
    save_to_gcs: bool = False,
) -> dict:
    """
    予測パイプラインを実行して結果を返す内部関数

    Args:
        model_path: モデルファイルパス（ローカルパスまたは gs:// URI）。
                    None の場合はGCSから最新モデルを自動取得。
        target_dates: 予測対象日のリスト
        save_to_bq: BigQueryに保存するか
        project_id: GCPプロジェクトID
        save_to_gcs: GCSに保存するか

    Returns:
        予測結果の辞書（num_races, num_horses, saved_to_bq, saved_rows, saved_to_gcs, gcs_uri）
    """
    from src.models.predict import predict_pipeline, save_predictions_to_bq, save_predictions_to_gcs
    from src.models.train import load_config

    # model_path が None の場合のみ GCS から最新モデルの gs:// URI を解決する。
    # gs:// URI / ローカルパスはそのまま predict_pipeline に渡す。
    # GCS からのダウンロード（キャリブレーション meta.json の取得を含む）は
    # predict_pipeline 側（_download_model_from_gcs）に一元化する。ここで先に
    # ローカル .txt へ解決してしまうと meta.json が落とされず校正器が適用されない
    # （no metadata）不具合になるため、URI のまま委譲する。
    resolved_model_path = (
        model_path if model_path is not None else _get_latest_model_from_gcs(project_id)
    )

    config = load_config()
    result_df = predict_pipeline(
        project_id=project_id,
        execution_date=_today_jst(),
        config=config,
        model_path=resolved_model_path,
        target_dates=target_dates,
    )

    num_races = int(result_df["race_id"].nunique()) if len(result_df) > 0 else 0
    num_horses = len(result_df)
    bq_saved = False
    saved_rows = 0
    gcs_saved = False
    gcs_uri = None

    if save_to_bq and len(result_df) > 0:
        saved_rows = save_predictions_to_bq(
            result_df=result_df,
            project_id=project_id,
        )
        bq_saved = True

    if save_to_gcs and len(result_df) > 0:
        gcs_uri = save_predictions_to_gcs(
            result_df=result_df,
            project_id=project_id,
            race_date=target_dates[0] if target_dates else None,
        )
        gcs_saved = True

    return {
        "num_races": num_races,
        "num_horses": num_horses,
        "saved_to_bq": bq_saved,
        "saved_rows": saved_rows,
        "saved_to_gcs": gcs_saved,
        "gcs_uri": gcs_uri,
    }


@app.post("/api/v1/predict/daily", response_model=PredictResponse)
async def predict_daily(request: PredictDailyRequest):
    """
    当日（および同週の土日）レースの予測を実行してBigQueryに保存する

    実行日を基準に当週の土日を予測対象とする。
    Cloud Schedulerから当日 AM 8:00 JST に呼び出されることを想定。

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

        # 実行日（JST）を基準に今週の土日を算出
        # 注意: _today_jst() を使う。UTC ベースの date.today() では日 AM 8:00 JST が
        # 土 23:00 UTC となり日付がズレるため、JST で正しい曜日を取得する必要がある。
        # tomorrow 基準にすると日曜に実行した場合に翌月曜を起点として来週土日を返してしまう。
        today = _today_jst()
        saturday, sunday = compute_week_boundaries(today)
        target_dates = [saturday, sunday]

        result = _run_predict(
            model_path=request.model_path,
            target_dates=target_dates,
            save_to_bq=request.save_to_bq,
            project_id=project_id,
            save_to_gcs=request.save_to_gcs,
        )

        logger.info(
            f"日次予測完了: {result['num_races']}レース, {result['num_horses']}頭, "
            f"bq_saved={result['saved_to_bq']}, gcs_saved={result['saved_to_gcs']}"
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
            save_to_gcs=request.save_to_gcs,
        )

        logger.info(
            f"オンデマンド予測完了: {result['num_races']}レース, {result['num_horses']}頭, "
            f"bq_saved={result['saved_to_bq']}, gcs_saved={result['saved_to_gcs']}"
        )
        return PredictResponse(
            status="success",
            target_dates=request.target_dates,
            **result,
        )

    except Exception as e:
        logger.error(f"オンデマンド予測エラー: {e}")
        raise HTTPException(status_code=500, detail=str(e)) from e


class OddsScrapeRequest(BaseModel):
    """オッズスクレイプリクエスト"""

    execution_date: str | None = Field(
        default=None,
        description="対象日付（YYYY-MM-DD形式、省略時は当日）",
        json_schema_extra={"example": "2026-03-07"},
    )
    include_combo: bool = Field(
        default=False,
        description="組み合わせ馬券（馬連・馬単・ワイド・三連複）オッズも取得してdaily_odds_comboに保存するか",
    )

    @field_validator("execution_date")
    @classmethod
    def validate_date_format(cls, v: str | None) -> str | None:
        if v is None:
            return v
        try:
            datetime.date.fromisoformat(v)
            return v
        except ValueError:
            raise ValueError(
                f"日付はYYYY-MM-DD形式の有効な日付で指定してください: '{v}'"
            )


class OddsScrapeResponse(BaseModel):
    """オッズスクレイプレスポンス"""

    status: str = Field(description="処理ステータス (success/failed)")
    execution_date: str = Field(description="対象日付")
    races_scraped: int = Field(default=0, description="オッズを取得できたレース数")
    horses_scraped: int = Field(default=0, description="取得した馬数（行数）")
    saved_rows: int = Field(default=0, description="BigQueryに保存した行数（単複）")
    combo_rows_saved: int = Field(default=0, description="BigQueryに保存した行数（組み合わせ馬券）")
    error_message: str | None = Field(default=None, description="エラーメッセージ")


class StrategyDailyRequest(BaseModel):
    """日次投資戦略リクエスト"""

    execution_date: str | None = Field(
        default=None,
        description="対象日付（YYYY-MM-DD形式、省略時は当日）",
        json_schema_extra={"example": "2026-03-07"},
    )
    dry_run: bool = Field(
        default=True,
        description="Trueの場合BQ保存をスキップして結果のみ返す（デフォルト: True。BQへの保存は発走直前の上書き処理で行う）",
    )
    initial_capital: float = Field(
        default=100_000.0,
        description="初期資金（Kelly計算用、単位:円）",
    )

    @field_validator("execution_date")
    @classmethod
    def validate_date_format(cls, v: str | None) -> str | None:
        if v is None:
            return v
        try:
            datetime.date.fromisoformat(v)
            return v
        except ValueError:
            raise ValueError(
                f"日付はYYYY-MM-DD形式の有効な日付で指定してください: '{v}'"
            )


class StrategyDailyResponse(BaseModel):
    """日次投資戦略レスポンス"""

    status: str = Field(description="処理ステータス (success/failed)")
    execution_date: str = Field(description="対象日付")
    decisions_count: int = Field(default=0, description="投資判断件数")
    total_bet_amount: float = Field(default=0.0, description="総賭け金（円）")
    dry_run: bool = Field(default=False, description="ドライランモードか")
    error_message: str | None = Field(default=None, description="エラーメッセージ")


@app.post("/api/v1/odds/scrape", response_model=OddsScrapeResponse)
async def scrape_odds(request: OddsScrapeRequest, background_tasks: BackgroundTasks):
    """
    netkeibaから当日レースの単勝・複勝オッズをスクレイピングしてBigQueryに保存する

    処理フロー:
    1. netkeibaのレース一覧ページから当日のレース情報を取得
    2. 各レースの単複オッズを取得
    3. JRDB形式のrace_idに変換
    4. predictions.daily_odds テーブルにUPSERT保存
    5. include_combo=True の場合、組み合わせオッズをバックグラウンドで取得・保存

    Args:
        request: リクエストボディ
        background_tasks: FastAPI BackgroundTasks（組み合わせオッズの非同期保存に使用）

    Returns:
        スクレイプ結果（組み合わせオッズはバックグラウンドで処理継続）
    """
    execution_date_str = request.execution_date or _today_jst().isoformat()
    execution_date = datetime.date.fromisoformat(execution_date_str)

    logger.info(f"オッズスクレイプリクエスト受信: execution_date={execution_date_str}")

    project_id = os.environ.get("GCP_PROJECT_ID")
    if not project_id:
        raise HTTPException(status_code=500, detail="GCP_PROJECT_IDが未設定です")

    try:
        from src.automation.data.netkeiba_scraper import (
            save_odds_to_bq,
            scrape_today_combo_odds,
            scrape_today_odds,
        )

        # Playwright Sync API は asyncio ループ内で直接呼び出せないため
        # スレッドプールで実行する
        odds_df = await asyncio.to_thread(
            scrape_today_odds,
            date=execution_date,
            project_id=project_id,
        )

        races_scraped = 0
        horses_scraped = 0
        saved_rows = 0

        if not odds_df.empty:
            races_scraped = int(odds_df["race_id"].nunique())
            horses_scraped = len(odds_df)
            saved_rows = await asyncio.to_thread(save_odds_to_bq, odds_df, project_id=project_id)
            logger.info(
                f"単複オッズスクレイプ完了: {races_scraped}レース, {horses_scraped}頭, "
                f"saved={saved_rows}行"
            )
        else:
            logger.warning(f"単複オッズを取得できませんでした: {execution_date_str}")

        # 組み合わせオッズはバックグラウンドで処理（タイムアウト回避のため即座に200を返す）
        if request.include_combo:
            background_tasks.add_task(
                scrape_today_combo_odds,
                date=execution_date,
                project_id=project_id,
            )
            logger.info("組み合わせオッズスクレイプをバックグラウンドで開始")

        return OddsScrapeResponse(
            status="success",
            execution_date=execution_date_str,
            races_scraped=races_scraped,
            horses_scraped=horses_scraped,
            saved_rows=saved_rows,
            combo_rows_saved=0,  # バックグラウンド処理のため結果は非同期
        )

    except Exception as e:
        logger.error(f"オッズスクレイプエラー: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e)) from e


@app.post("/api/v1/strategy/daily", response_model=StrategyDailyResponse)
async def run_strategy_daily(request: StrategyDailyRequest):
    """
    日次投資戦略を策定する（デフォルトはBQ保存なし）

    config/strategy_config.yaml のパラメータを読み込み、当日の予測結果と
    リアルタイムオッズを JOIN して投資判断を実行する。
    Cloud Schedulerから毎朝AM 8:30に呼び出されることを想定。

    デフォルト（dry_run=True）では BQ への保存を行わない。
    BQ への保存は発走直前の _refresh_investment_decisions_for_race() が行う。
    dry_run=False を明示した場合のみ investment_decisions テーブルへ保存する。

    前提条件:
      - predictions.daily_predictions に当日の予測データが存在すること
      - predictions.daily_odds に当日のオッズデータが存在すること
      - config/strategy_config.yaml にパラメータが設定されていること
        （scripts/run_strategy_optimization.py を一度実行すること）

    Args:
        request: リクエストボディ

    Returns:
        投資判断結果
    """
    execution_date_str = request.execution_date or _today_jst().isoformat()
    execution_date = datetime.date.fromisoformat(execution_date_str)

    logger.info(
        f"日次投資戦略リクエスト受信: execution_date={execution_date_str}, "
        f"dry_run={request.dry_run}"
    )

    project_id = os.environ.get("GCP_PROJECT_ID")
    if not project_id:
        raise HTTPException(status_code=500, detail="GCP_PROJECT_IDが未設定です")

    try:
        from scripts.run_strategy import run_daily_strategy

        decisions = await asyncio.to_thread(
            run_daily_strategy,
            project_id=project_id,
            target_date=execution_date,
            dry_run=request.dry_run,
        )

        total_bet = sum(d["bet_amount"] for d in decisions) if decisions else 0.0
        logger.info(
            f"日次投資戦略完了: {len(decisions)}件, 総賭け金={total_bet:,.0f}円"
        )
        return StrategyDailyResponse(
            status="success",
            execution_date=execution_date_str,
            decisions_count=len(decisions),
            total_bet_amount=total_bet,
            dry_run=request.dry_run,
        )

    except FileNotFoundError as e:
        logger.error(f"設定ファイルが見つかりません: {e}")
        raise HTTPException(status_code=500, detail=str(e)) from e
    except Exception as e:
        logger.error(f"日次投資戦略エラー: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e)) from e


class LineWebhookResponse(BaseModel):
    """LINE Webhook レスポンス"""

    status: str = Field(description="処理ステータス (ok)")


@app.post("/api/v1/line/webhook", response_model=LineWebhookResponse)
async def line_webhook(request: Request):
    """
    LINE Messaging API Webhook エンドポイント

    ユーザーが「日付 競馬場名 レース番号」形式のメッセージを送信した場合のみ返答する。
    - メッセージ1: 予測テーブル（予測順・馬番・馬名・スコア・複勝率・オッズ・期待値）
    - メッセージ2: 推奨馬券リスト（馬券種・馬番・馬名・オッズ・賭け金・合計）

    環境変数:
      LINE_CHANNEL_SECRET       : 署名検証用シークレット
      LINE_CHANNEL_ACCESS_TOKEN : リプライ送信用アクセストークン
      GCP_PROJECT_ID            : BigQuery プロジェクト ID
    """
    channel_secret = os.environ.get("LINE_CHANNEL_SECRET", "")
    channel_access_token = os.environ.get("LINE_CHANNEL_ACCESS_TOKEN", "")
    project_id = os.environ.get("GCP_PROJECT_ID")

    if not project_id:
        raise HTTPException(status_code=500, detail="GCP_PROJECT_IDが未設定です")

    body = await request.body()
    signature = request.headers.get("X-Line-Signature", "")

    # 署名検証（channel_secret が設定されている場合のみ）
    if channel_secret:
        from src.automation.api.line_webhook import verify_line_signature

        if not verify_line_signature(channel_secret, body, signature):
            logger.warning("LINE署名検証失敗")
            raise HTTPException(status_code=400, detail="Invalid signature")

    import json

    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    events = payload.get("events", [])
    if events:
        from src.automation.api.line_webhook import process_webhook_events

        await asyncio.to_thread(
            process_webhook_events,
            events=events,
            project_id=project_id,
            channel_access_token=channel_access_token,
        )

    return LineWebhookResponse(status="ok")


class PurchaseDailyRequest(BaseModel):
    """IPAT日次自動購入リクエスト"""

    target_date: str | None = Field(
        default=None,
        description="対象日付（YYYY-MM-DD形式、省略時は当日）",
    )
    dry_run: bool = Field(
        default=False,
        description="Trueの場合、実際の購入をせず推奨馬券内容をLINE通知のみ行う（デフォルト: False）",
    )


class PurchaseRaceResult(BaseModel):
    """1レース分の購入結果"""

    race_id: str
    bets_purchased: int
    bets_failed: int
    bets_skipped_budget: int
    bets_need_confirmation: int = 0
    amount: int
    status: str


class PurchaseDailyResponse(BaseModel):
    """IPAT日次自動購入レスポンス"""

    status: str = Field(description="処理ステータス (success/skipped/error)")
    execution_date: str = Field(description="対象日付")
    dry_run: bool = Field(default=True, description="ドライランモードか")
    purchased_races: int = Field(default=0, description="購入処理を行ったレース数")
    total_amount: int = Field(default=0, description="当日累計購入金額（円）")
    results: list[PurchaseRaceResult] = Field(default_factory=list)


@app.post("/api/v1/purchase/daily", response_model=PurchaseDailyResponse)
async def purchase_daily(request: PurchaseDailyRequest):
    """
    発走5分前のレースの推奨馬券を JRA IPAT で自動購入する。

    Cloud Scheduler から土日 8:00〜17:00 の5分おきに呼び出されることを想定。
    現在時刻の0〜5分後に発走するレースを対象とし、
    predictions.investment_decisions の推奨馬券を購入する。

    前提条件:
      - predictions.investment_decisions に当日分のデータが存在すること
        （POST /api/v1/strategy/daily 完了後）
      - raw.race_info に start_time カラムが存在すること（Issue #214）
      - scripts/create_purchase_history_table.py でBQテーブルが作成済みであること

    環境変数:
      IPAT_MEMBER_ID          : JRA IPAT 加入者番号
      IPAT_PIN                : JRA IPAT 暗証番号（4桁）
      IPAT_PAT_NUMBER         : JRA IPAT PAT番号
      GCP_PROJECT_ID          : BigQuery プロジェクト ID
      LINE_CHANNEL_ACCESS_TOKEN : LINE プッシュ通知用アクセストークン（失敗通知用）
      LINE_USER_ID            : LINE 通知先ユーザーID
    """
    target_date = (
        datetime.date.fromisoformat(request.target_date)
        if request.target_date
        else _today_jst()
    )
    execution_date_str = target_date.isoformat()
    dry_run = request.dry_run
    logger.info(f"IPAT日次購入リクエスト受信: execution_date={execution_date_str}, dry_run={dry_run}")

    project_id = os.environ.get("GCP_PROJECT_ID")
    member_id = os.environ.get("IPAT_MEMBER_ID", "")
    pin = os.environ.get("IPAT_PIN", "")
    pat_number = os.environ.get("IPAT_PAT_NUMBER", "")
    channel_access_token = os.environ.get("LINE_CHANNEL_ACCESS_TOKEN", "")
    line_user_id = os.environ.get("LINE_USER_ID", "")

    if not project_id:
        raise HTTPException(status_code=500, detail="GCP_PROJECT_IDが未設定です")
    # dry_run=False の場合のみ IPAT 認証情報を必須チェック
    if not dry_run and (not member_id or not pin or not pat_number):
        raise HTTPException(
            status_code=500,
            detail="IPAT認証情報が未設定です（IPAT_MEMBER_ID / IPAT_PIN / IPAT_PAT_NUMBER）",
        )

    try:
        result = await asyncio.to_thread(
            _run_purchase_pipeline,
            project_id=project_id,
            target_date=target_date,
            member_id=member_id,
            pin=pin,
            pat_number=pat_number,
            channel_access_token=channel_access_token,
            line_user_id=line_user_id,
            dry_run=dry_run,
        )
        return PurchaseDailyResponse(
            status=result["status"],
            execution_date=execution_date_str,
            dry_run=dry_run,
            purchased_races=result["purchased_races"],
            total_amount=result["total_amount"],
            results=[PurchaseRaceResult(**r) for r in result["results"]],
        )
    except Exception as e:
        logger.error(f"IPAT日次購入エラー: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e)) from e


def _run_purchase_pipeline(
    project_id: str,
    target_date: datetime.date,
    member_id: str,
    pin: str,
    pat_number: str,
    channel_access_token: str,
    line_user_id: str,
    dry_run: bool = True,
) -> dict:
    """
    IPAT自動購入パイプラインの同期ラッパー（asyncio.to_thread で実行）。
    """
    import asyncio as _asyncio

    return _asyncio.run(
        _purchase_pipeline_async(
            project_id=project_id,
            target_date=target_date,
            member_id=member_id,
            pin=pin,
            pat_number=pat_number,
            channel_access_token=channel_access_token,
            line_user_id=line_user_id,
            dry_run=dry_run,
        )
    )


def _refresh_investment_decisions_for_race(
    project_id: str,
    race_id: str,
    target_date: datetime.date,
) -> bool:
    """
    発走直前に最新オッズで investment_decisions を1レース分上書きする。

    predictions.daily_odds / daily_odds_combo から最新オッズを取得し、
    select_bets_for_race() で推奨馬券を再計算して predictions.investment_decisions へ
    MERGE UPSERT する。

    データが取得できない場合は既存の investment_decisions をそのまま使用（フォールバック）。

    Args:
        project_id: GCP プロジェクト ID
        race_id: 対象レース ID
        target_date: 対象日

    Returns:
        True: 上書き成功, False: フォールバック（既存データ使用）
    """
    from google.cloud import bigquery as _bq
    from scripts.run_strategy import (
        build_race_df,
        fetch_combo_odds_for_date,
        load_strategy_config,
        save_decisions_to_bq,
        _build_decision_row,
    )
    from src.backtest.strategy import select_bets_for_race

    try:
        client = _bq.Client(project=project_id)

        # 対象レースの予測データを取得
        pred_query = f"""
        SELECT race_id, race_date, horse_id, horse_number, horse_name,
               venue_code, race_number, win_place_prob, pred_score, rank_in_race
        FROM `{project_id}.predictions.daily_predictions`
        WHERE race_date = '{target_date.isoformat()}'
          AND race_id = '{race_id}'
        ORDER BY rank_in_race
        """
        import pandas as _pd
        pred_df = client.query(pred_query).to_dataframe()

        if pred_df.empty:
            logger.warning(f"_refresh: race_id={race_id} の予測データなし → フォールバック")
            return False

        # 対象レースの単複オッズを取得
        odds_query = f"""
        SELECT race_id, horse_number, win_odds, place_odds_min, place_odds_max, scraped_at
        FROM `{project_id}.predictions.daily_odds`
        WHERE race_date = '{target_date.isoformat()}'
          AND race_id = '{race_id}'
        """
        odds_df = client.query(odds_query).to_dataframe()

        if odds_df.empty:
            logger.warning(f"_refresh: race_id={race_id} のオッズデータなし → フォールバック")
            return False

        # race_df を構築（predictions + odds JOIN）
        race_df = build_race_df(pred_df, odds_df)
        race_df = race_df.dropna(subset=["win_place_prob", "odds"])
        if len(race_df) < 3:
            logger.warning(f"_refresh: race_id={race_id} の有効データ不足 → フォールバック")
            return False

        # 組み合わせオッズを取得
        combo_df = fetch_combo_odds_for_date(client, project_id, [race_id])
        race_combo_df = combo_df[combo_df["race_id"] == race_id] if not combo_df.empty else _pd.DataFrame()

        # 戦略パラメータを読み込み
        config = load_strategy_config()
        expected_return_threshold = float(config.get("expected_return_threshold", 1.2))
        top_n = int(config.get("top_n", 5))
        budget_per_race = float(config.get("budget_per_race", 3000.0))
        min_bet_amount = float(config.get("min_bet_amount", 100.0))
        min_prob_threshold = float(config.get("min_prob_threshold", 0.10))
        prob_weight_r = float(config.get("prob_weight_r", 1.0))
        _mwo = config.get("max_wide_odds", None)
        max_wide_odds = float(_mwo) if _mwo is not None else None
        enabled_bet_types = config.get("enabled_bet_types")
        gamma = float(config.get("gamma", 1.0))
        use_harville = bool(config.get("use_harville", False))

        # 推奨馬券を再計算
        bets = select_bets_for_race(
            race_df=race_df,
            combo_odds_df=race_combo_df if not race_combo_df.empty else None,
            budget_per_race=budget_per_race,
            expected_return_threshold=expected_return_threshold,
            min_bet_amount=min_bet_amount,
            min_prob_threshold=min_prob_threshold,
            prob_weight_r=prob_weight_r,
            top_n=top_n,
            max_wide_odds=max_wide_odds,
            enabled_bet_types=enabled_bet_types,
            gamma=gamma,
            use_harville=use_harville,
        )

        # 投資判断 dict を構築して MERGE UPSERT 保存
        meta_row = race_df.iloc[0]
        created_at = datetime.datetime.now(datetime.timezone.utc)
        decisions = [
            _build_decision_row(
                race_id=race_id,
                target_date=target_date,
                meta_row=meta_row,
                race_group=race_df,
                bet=bet,
                race_pattern_str="unified",
                created_at=created_at,
            )
            for bet in bets
            if bet.get("horse_numbers")
        ]

        save_decisions_to_bq(decisions, project_id)
        logger.info(
            f"_refresh: race_id={race_id} の investment_decisions を更新 "
            f"({len(decisions)}件, パターン=unified)"
        )
        return True

    except Exception as e:
        logger.warning(
            f"_refresh: race_id={race_id} の更新失敗 → フォールバック: {e}",
            exc_info=True,
        )
        return False


async def _purchase_pipeline_async(
    project_id: str,
    target_date: datetime.date,
    member_id: str,
    pin: str,
    pat_number: str,
    channel_access_token: str,
    line_user_id: str,
    dry_run: bool = True,
) -> dict:
    """
    IPAT自動購入パイプラインの非同期実装。

    dry_run=True（デフォルト）:
      - IPATへのログイン・購入を一切行わない
      - 発走5分前レースの推奨馬券内容をLINEに通知するのみ

    dry_run=False:
      - 通常フロー（IPAT購入 + 履歴BQ保存）

    フロー:
      1. raw.race_info から当日の発走時刻を取得
      2. 現在時刻の-5〜5分後に発走するレースを特定
         （マイナス側は直前tickでの購入失敗を次tickで再挑戦するためのウィンドウ。Issue #433）
      3. 対象レースが0件なら skipped を返す
      4. 対象レースのオッズをリアルタイムスクレイピング（netkeiba）
         失敗時はフォールバック（既存の daily_odds を使用）
      5. [dry_run=False] 各レースについて、ログイン前に「購入する価値があるか」を軽くチェックする
         5a. 既に購入成功済み／要確認（need_confirmation）／処理中（in_progress・並行tick対策）
             なら二重購入防止のためスキップ
         5b. 最新オッズで investment_decisions を上書き（_refresh_investment_decisions_for_race）
             → investment_decisions はこの関数でしか書き込まれないため、ここで
               refreshしないと「まだ一度もrefreshされていないレース」が永久に
               購入対象と判定されないデッドロックになる（過去の実装ミス）
             失敗時はフォールバック（既存の investment_decisions を使用）
         5c. 推奨馬券を取得。0件ならスキップ
         購入対象レースが1件もなければ IPAT へのログイン自体を行わない
      6. [dry_run=False] 購入対象レースが1件以上ある場合のみ IPAT ログイン
      7. 各レースについて:
         [dry_run=True]  LINE通知のみ
         [dry_run=False]
           7a. 購入ロックをアトミックに取得（try_acquire_purchase_lock。取得
               できなければ他tickが既に処理済みのためスキップ。Issue #435）
           7b. 最新オッズで investment_decisions を再度上書き
               → 5bからの経過時間がある分、購入直前にもう一度refreshすることで
                 オッズの鮮度を保つ（意図的な二重refresh。5b/6/7参照）
               失敗時はフォールバック（既存の investment_decisions を使用）
           7c. 予算チェック → 購入（送信前フェーズは自動リトライ） → 履歴保存
               → 結果確定後、購入ロックを最終ステータスへ更新（finalize_purchase_lock）
      8. [dry_run=False] ログアウト
    """
    from src.automation.data.ipat_purchaser import (
        IpatLoginError,
        IpatPurchaser,
        fetch_daily_spent_amount,
        fetch_recommended_bets,
        fetch_today_races_with_start_time,
        fetch_target_races,
        finalize_purchase_lock,
        has_purchase_lock,
        save_purchase_record,
        try_acquire_purchase_lock,
        BET_TYPE_MAP,
        DAILY_BUDGET_LIMIT,
    )
    from src.automation.data.netkeiba_scraper import scrape_odds_for_race
    from src.utils.line_notify import push_messages, text_message

    def _send_line(msg: str) -> None:
        if channel_access_token and line_user_id:
            try:
                push_messages(channel_access_token, line_user_id, [text_message(msg)])
            except Exception as e:
                logger.warning(f"LINE通知失敗（無視します）: {e}")

    def _refresh_and_fetch_bets(race_id: str, log_prefix: str = "") -> list[dict]:
        """
        最新オッズで investment_decisions を上書きしてから推奨馬券を取得する。

        dry_run分岐・本番の事前チェック・本番の実購入直前の3箇所で全く同じ
        refresh→fetchの手順が必要なため、ここに集約する（重複による将来の
        実装ズレを防ぐ・/code-review指摘）。呼び出し元がBQ例外を捕捉すること
        （このヘルパー自体は例外を送出しうる）。
        """
        refreshed = _refresh_investment_decisions_for_race(project_id, race_id, target_date)
        if refreshed:
            logger.info(f"{log_prefix}race_id={race_id}: 最新オッズで investment_decisions を更新済み")
        else:
            logger.info(f"{log_prefix}race_id={race_id}: フォールバック（既存 investment_decisions を使用）")
        return fetch_recommended_bets(project_id, race_id, target_date)

    # tick全体の処理時間を計測する。Cloud Runのリクエストタイムアウト（900秒）に
    # 対し、PRE_SUBMIT_MAX_ATTEMPTS回のリトライ（再ログイン含む）が複数レース分
    # 積み重なると近づきうる（/code-review指摘）。タイムアウトでプロセスごと
    # 強制終了されると、「投票」送信直後〜結果確定前でも例外処理・LINE通知が
    # 一切行われない（asyncio.CancelledErrorすら発生しない、プロセスの唐突な
    # 終了）ため、TICK_TIME_BUDGET_SECONDS を十分な余裕を持って設定し、
    # 超過した時点で残りのレースは次tickに委ねる（次tickのウィンドウ判定・
    # 購入ロック（has_purchase_lock/try_acquire_purchase_lock）による
    # 通常の再挑戦フローに乗る）。
    # 注意: Cloud Scheduler側の attempt-deadline
    # （infrastructure/scripts/setup_scheduler.sh の race-day-purchase(-summer)）
    # が本値より短いと、Scheduler自体がCloud Run処理継続中にタイムアウト・
    # 再試行し同一tickが重複実行されうる。両者は必ずセットで見直すこと
    # （/code-review指摘: 過去に180s vs 600sの不整合が実在した）。
    tick_start = datetime.datetime.now(datetime.timezone.utc)

    # 1. 発走時刻付きレース一覧取得
    all_races = fetch_today_races_with_start_time(project_id, target_date)
    if not all_races:
        logger.info(f"{target_date}: start_time付きレースが存在しません")
        return {"status": "skipped", "purchased_races": 0, "total_amount": 0, "results": []}

    # 2. 対象レースを抽出（現在時刻の-5〜5分。マイナス側は直前tickでの購入失敗の再挑戦用）
    # start_time は JST で格納されているため、now も JST で取得する
    # 5分おきスケジューラで window_minutes_after=0 のままだと、1回失敗したレースは
    # 二度と対象にならず購入機会を完全に失っていた（Issue #433, 2026-09-19本番障害）。
    # ウィンドウを10分に拡張し、二重購入は購入ロック（Issue #435）で防止する。
    now = datetime.datetime.now(ZoneInfo("Asia/Tokyo"))
    target_races = fetch_target_races(all_races, now, window_minutes_before=5, window_minutes_after=-5)

    if not target_races:
        logger.info(f"{target_date}: 現在時刻 {now.strftime('%H:%M')} に対象レースなし")
        return {"status": "skipped", "purchased_races": 0, "total_amount": 0, "results": []}

    logger.info(
        f"対象レース {len(target_races)}件: {[r['race_id'] for r in target_races]}"
        f" [{'ドライラン' if dry_run else '本番購入'}]"
    )

    # 3. 対象レースのオッズをリアルタイムスクレイピング
    for race in target_races:
        race_id = race["race_id"]
        try:
            success = await asyncio.to_thread(
                scrape_odds_for_race,
                race_id,
                target_date,
                project_id,
            )
            if success:
                logger.info(f"race_id={race_id}: リアルタイムオッズ取得完了")
            else:
                logger.warning(
                    f"race_id={race_id}: リアルタイムオッズ取得失敗（既存データを使用）"
                )
        except Exception as e:
            logger.warning(
                f"race_id={race_id}: リアルタイムオッズ取得中に例外発生（フォールバック）: {e}"
            )

    race_results = []

    # --- ドライランモード ---
    if dry_run:
        dry_run_error_count = 0
        dry_run_no_bets_count = 0
        for race in target_races:
            race_id = race["race_id"]
            venue_name = race.get("venue_name", "")
            race_number = race.get("race_number", "")

            # 1レースの処理中の想定外エラー（BQ一時障害等）でtick全体（＝他の全レースの
            # LINE通知）が失われないよう、このレースだけスキップして次に進む
            # （/code-review指摘。本番購入ループと同じ問題が同じ関数の別箇所にあった）。
            try:
                bets = _refresh_and_fetch_bets(race_id, log_prefix="[DRY RUN] ")
                if not bets:
                    logger.info(f"[DRY RUN] race_id={race_id}: 推奨馬券なし → スキップ")
                    dry_run_no_bets_count += 1
                    continue

                # 推奨馬券をLINE通知
                lines = [f"【ドライラン】{venue_name}{race_number}R 購入予定馬券"]
                total_amount = 0
                for bet in bets:
                    bet_type = bet["bet_type"]
                    horse_numbers = bet["horse_numbers"]
                    amount = int(bet["bet_amount"])
                    horse_str = "-".join(str(h) for h in horse_numbers)
                    lines.append(f"  {bet_type} {horse_str} {amount:,}円")
                    total_amount += amount
                lines.append(f"  合計: {total_amount:,}円")
                _send_line("\n".join(lines))
                logger.info(f"[DRY RUN] race_id={race_id}: {len(bets)}件通知")

                race_results.append({
                    "race_id": race_id,
                    "bets_purchased": 0,
                    "bets_failed": 0,
                    "bets_skipped_budget": 0,
                    "amount": 0,
                    "status": "dry_run",
                })
            except Exception as e:
                dry_run_error_count += 1
                logger.error(
                    f"[DRY RUN] race_id={race_id}: 処理中に想定外のエラー（このレースをスキップ）: {e}",
                    exc_info=True,
                )
                continue

        # ドライラン（AM8:30）は当日のIPAT自動購入tick（AM8:00〜）が始まる前の
        # 唯一の人間向け早期警告機会。本番購入ループと同じ「事前チェック対象
        # 全滅」検知がここに無いと、BQ全面障害時でも他の正常スキップと区別が
        # つかずstatus="success"のまま静かに終わり、本番購入直前に気づく機会を
        # 逃してしまう（/code-review指摘）。
        dry_run_attempted_count = dry_run_error_count + len(race_results)
        if dry_run_error_count > 0 and dry_run_error_count == dry_run_attempted_count:
            msg = (
                f"【エラー】ドライラン事前確認で対象{dry_run_attempted_count}レース"
                f"（全{len(target_races)}レース中、推奨馬券なし{dry_run_no_bets_count}件の"
                f"正常スキップを除く）全てで想定外のエラーが発生しました。"
                f"本番のIPAT自動購入が正常に動作しない可能性があります。"
            )
            logger.error(msg)
            _send_line(msg)

        notified_races = len(race_results)
        logger.info(f"ドライラン完了: {notified_races}レース分をLINE通知")
        return {
            "status": "success",
            "purchased_races": 0,
            "total_amount": 0,
            "results": race_results,
        }

    def _debug_suffix(debug: dict | None) -> str:
        """LINE通知・ログに付与する失敗時デバッグ情報（画面文言・スクリーンショットパス）"""
        if not debug:
            return ""
        parts = []
        if debug.get("text_snippet"):
            parts.append(f"画面: {debug['text_snippet'][:120]}")
        if debug.get("screenshot_gcs_path"):
            parts.append(f"SS: {debug['screenshot_gcs_path']}")
        return ("\n" + "\n".join(parts)) if parts else ""

    def _format_bet_summary(bets: list[dict]) -> str:
        """
        LINE通知・ログに使う馬券サマリ文字列を組み立てる。

        BET_TYPE_MAP で日本語ラベル（例: "複勝"）に変換する。ipat_purchaser.py の
        「一括購入開始」ログも同じマップで日本語表示しており、変換せず
        bet_type コード（例: "place"）のまま出すと、同じ購入についての
        LINE通知とCloud Runログで表示が食い違い、障害調査時に混乱を招く
        （/code-review指摘）。未知のコードはそのまま表示する。
        """
        return ", ".join(
            f"{BET_TYPE_MAP.get(b['bet_type'], b['bet_type'])} "
            f"{'-'.join(str(h) for h in b['horse_numbers'])} {b['amount']}円"
            for b in bets
        )

    def _safe_save_purchase_record(*args, **kwargs) -> None:
        """
        save_purchase_record() を例外安全に呼ぶラッパー。

        1レースにつき複数回 save_purchase_record を呼ぶ箇所（1馬券ごとに1回）で、
        そのうちの1回がBQ一時障害等で失敗すると、例外が1レース分の処理を囲む
        try/exceptまで伝播し、「想定外のエラー」用のfailedマーカーで
        上書きされてしまう。しかし実際にはIPATへの投票が既にsuccess/
        need_confirmationとして確定している場合、それをfailed（＝次tickで
        自動再購入してよい）に見せかけてしまうのは二重購入に直結する
        （/code-review指摘）。個々の保存呼び出しの失敗はログのみに留め、
        レース全体の処理を止めない。

        1回だけ即時リトライする（/code-review指摘）。完全な保証にはならないが、
        単発の一時的な書き込みエラーはこれで大半吸収できる。
        """
        try:
            save_purchase_record(*args, **kwargs)
            return
        except Exception as e:
            logger.warning(f"purchase_history への保存に失敗、1回だけ再試行します: {e}")
        try:
            save_purchase_record(*args, **kwargs)
        except Exception as e:
            logger.error(
                f"purchase_history への保存にリトライ後も失敗しました（続行します）: {e}",
                exc_info=True,
            )

    def _safe_finalize_purchase_lock(*args, **kwargs) -> None:
        """
        finalize_purchase_lock() を例外安全に呼ぶラッパー（Issue #435）。

        購入結果確定後の唯一のロック解放手段（例: skipped_budget記録・no_bets
        スキップ・想定外エラー時）であり、ここが無保護のまま失敗すると例外が
        1レース分の処理を止めてしまう上、ロックが有効期限
        （IN_PROGRESS_STALE_MINUTES）まで解放されないままになる。
        _safe_save_purchase_record() と同様、1回だけ即時リトライする。
        """
        try:
            finalize_purchase_lock(*args, **kwargs)
            return
        except Exception as e:
            logger.warning(f"購入ロックの更新に失敗、1回だけ再試行します: {e}")
        try:
            finalize_purchase_lock(*args, **kwargs)
        except Exception as e:
            logger.error(
                f"購入ロックの更新にリトライ後も失敗しました（続行します）: {e}",
                exc_info=True,
            )

    def _safe_fetch_daily_spent_amount() -> int:
        """
        fetch_daily_spent_amount() を例外安全に呼ぶラッパー（0円にフォールバック）。

        BQ障害を検知して status='error' の応答を組み立てる箇所（事前チェック
        全滅時・購入対象なし時）でこの関数が無保護のまま呼ばれていると、
        同じBQ障害でこの呼び出し自体も失敗し、意図した「きれいなerror応答」
        ではなく未処理の例外（HTTP 500）になってしまう（/code-review指摘）。
        """
        try:
            return fetch_daily_spent_amount(project_id, target_date)
        except Exception as e:
            logger.warning(f"当日累計購入額の取得に失敗（0円として扱います）: {e}")
            return 0

    # --- 本番購入モード ---
    # 3. ログイン前に「実際に購入すべきレース」を確定する。
    #    投資判断の更新・推奨馬券取得はIPATセッション不要のため、これをログインより先に
    #    行うことで、購入対象が0件のtickで無駄なログインを発生させない（Issue #433）。
    #    ウィンドウ拡張（-5〜+5分）により同一レースが複数tickで対象になり得るため、
    #    既に購入成功済み／要確認（need_confirmation）のレースはここで除外し二重購入を防ぐ。
    _WEEKDAY_JP = ["月", "火", "水", "木", "金", "土", "日"]
    weekday_suffix = f"({_WEEKDAY_JP[target_date.weekday()]})"

    # 重要: investment_decisions は _refresh_investment_decisions_for_race() でしか
    # 書き込まれない（race-day-strategy の8:30ジョブは dry_run=true がデフォルトで
    # BQ保存を行わない）。そのため、ここでrefreshを一切行わずに「既存の
    # investment_decisions」だけを見て購入要否を判定すると、そのレースについて
    # まだ一度もrefreshが実行されていない（＝1日のうち最初にこのレースがウィンドウに
    # 入ったtick）場合、常に0件と判定されてしまい、refresh自体が永久に呼ばれず
    # 当該レースが決して購入されないという致命的なデッドロックになる
    # （/code-review指摘。当初はこれを避けるためrefreshを省略していたが、それ自体が
    # 誤りだった）。
    # したがって事前チェックでもrefreshは必ず行う。実購入直前（ログイン後のループ内）
    # でも同じレースに対してもう一度refresh+再取得するため、二重に計算コストが
    # かかるが、これは「無駄なログインを避ける」ことと「購入直前までオッズを鮮度良く
    # 保つ」ことを両立するための意図的なトレードオフである。
    races_to_purchase: list[dict] = []
    precheck_error_count = 0
    precheck_already_done_count = 0
    precheck_no_bets_count = 0
    for race in target_races:
        race_id = race["race_id"]

        # 事前チェックも各レースにつき最大2回のBQ round-trip（refresh+fetch）を
        # 行うため、対象レースが多いtickではCloud Runの900秒タイムアウトに
        # 近づきうる（/code-review指摘: 本番購入ループ側だけの対策では
        # 事前チェック段階の時間超過をカバーできていなかった）。
        elapsed_seconds = (
            datetime.datetime.now(datetime.timezone.utc) - tick_start
        ).total_seconds()
        if elapsed_seconds > TICK_TIME_BUDGET_SECONDS:
            logger.warning(
                f"race_id={race_id}: tick開始から{elapsed_seconds:.0f}秒経過したため、"
                f"事前チェックの残りは次tickに委ねます（Cloud Runタイムアウト対策）"
            )
            break

        # has_purchase_lock/refresh/fetch はいずれもBQ呼び出しであり
        # 例外送出しうる。ここを保護しないと、1レースでのBQ一時障害がtick全体
        # （事前チェック中の他の全レース）を巻き込んで中断させてしまう
        # （/code-review指摘。本番購入ループ側は既に保護済みだったが、事前チェック
        # ループ側が同じ問題を抱えたまま残っていた）。
        try:
            if has_purchase_lock(project_id, target_date, race_id):
                logger.info(f"race_id={race_id}: 既に購入成功済み/要確認済み/処理中 → スキップ")
                precheck_already_done_count += 1
                continue

            bets = _refresh_and_fetch_bets(race_id)
            if not bets:
                logger.info(f"race_id={race_id}: 推奨馬券なし（refresh後） → スキップ")
                precheck_no_bets_count += 1
                continue

            races_to_purchase.append(race)
        except Exception as e:
            precheck_error_count += 1
            logger.error(
                f"race_id={race_id}: 事前チェック中に想定外のエラー（このレースをスキップ）: {e}",
                exc_info=True,
            )
            continue

    if not races_to_purchase:
        # BQ障害等で対象レース全件が事前チェックで例外になった場合、「今tickは
        # 単に購入対象がなかっただけ」と区別できず、status='success'のまま
        # 静かに終わってしまう（/code-review指摘。本番購入ループ側の全滅検知
        # （race_resultsベース）はこの事前チェック段階の全滅を検知できない
        # ——事前チェックで弾かれたレースはrace_resultsに一切追加されないため）。
        # 「既に購入成功済み等でスキップ」「推奨馬券なしでスキップ」はいずれも
        # 正常系のためエラー母数から除外する（/code-review指摘: ウィンドウ拡張
        # により一部レースは正常にスキップされつつ、残りの実際に評価すべき
        # レースが全滅するケースを見逃していた。当初は「購入済み等」しか
        # 除外しておらず、推奨馬券なしの正常スキップが混在すると
        # precheck_error_count が分母に一致せず、実際には評価対象レースが
        # 全滅していてもBQ障害アラートが発火しなかった）。
        precheck_attempted_count = precheck_error_count + len(races_to_purchase)
        if precheck_error_count > 0 and precheck_error_count == precheck_attempted_count:
            msg = (
                f"【エラー】事前チェック対象{precheck_attempted_count}レース"
                f"（全{len(target_races)}レース中、購入済み等{precheck_already_done_count}件・"
                f"推奨馬券なし{precheck_no_bets_count}件の正常スキップを除く）"
                f"全てで想定外のエラーが発生しました。BigQuery等のデータ基盤に"
                f"問題がある可能性があります。"
            )
            logger.error(msg)
            _send_line(msg)
            return {
                "status": "error",
                "purchased_races": 0,
                "total_amount": _safe_fetch_daily_spent_amount(),
                "results": [],
            }

        logger.info("購入対象レースがないため、IPATへのログインをスキップします")
        return {
            "status": "success",
            "purchased_races": 0,
            "total_amount": _safe_fetch_daily_spent_amount(),
            "results": [],
        }

    # 4. IPAT ログイン
    async with IpatPurchaser(member_id, pin, pat_number, project_id=project_id) as purchaser:
        try:
            logged_in = await purchaser.login()
        except IpatLoginError as e:
            msg = f"IPATログインに失敗しました: {e}"
            logger.error(msg)
            _send_line(msg)
            return {"status": "error", "purchased_races": 0, "total_amount": 0, "results": []}

        if not logged_in:
            msg = "IPATログインに失敗しました（認証情報を確認してください）" + _debug_suffix(
                purchaser.last_login_debug
            )
            logger.error(msg)
            _send_line(msg)
            return {"status": "error", "purchased_races": 0, "total_amount": 0, "results": []}

        # 5. 各レースの購入処理
        for race in races_to_purchase:
            race_id = race["race_id"]
            venue_name = race.get("venue_name", "")
            race_number = race.get("race_number", 0)
            start_time = race.get("start_time")
            # IPATのSP版に表示される競馬場名（「中山(土)」形式）
            venue_name_with_day = f"{venue_name}{weekday_suffix}"

            # tick開始からの経過時間がCloud Runタイムアウト（900秒）に近づいて
            # いる場合、このレースには着手せず次tickに委ねる。まだ
            # try_acquire_purchase_lock() を呼んでいないため、次tickの
            # 通常の事前チェックで問題なく再評価される（/code-review指摘）。
            elapsed_seconds = (
                datetime.datetime.now(datetime.timezone.utc) - tick_start
            ).total_seconds()
            if elapsed_seconds > TICK_TIME_BUDGET_SECONDS:
                logger.warning(
                    f"race_id={race_id}: tick開始から{elapsed_seconds:.0f}秒経過したため、"
                    f"このレース以降は次tickに委ねます（Cloud Runタイムアウト対策）"
                )
                break

            # ここから先で想定外の例外（BQ一時障害によるtry_acquire_purchase_lockの
            # 失敗、不正な投資判断データによるValueError/IpatPurchaseError等）が
            # 発生すると、ロックが未解放のままtick全体が中断し、この後に続く他レース
            # の購入機会も失ってしまう（/code-review指摘）。1レース分の処理全体を
            # try で囲み、失敗時はロックを解放した上でこのレースだけスキップして
            # 次レースの処理を継続する。
            budget_exceeded = False
            try:
                # 事前チェックからここまでに時間が空くため（ログイン待ち・前レースの
                # 処理時間）、購入ロックをBigQuery MERGE文でアトミックに取得する
                # （Issue #435）。「チェック→マーカー書き込み」の2クエリ構成だった
                # 旧実装と異なり、取得判定とマーカー書き込みが単一のDML文で行われる
                # ため、並行tickによる二重購入を真に排他制御できる。
                if not try_acquire_purchase_lock(project_id, target_date, race_id):
                    logger.info(f"race_id={race_id}: 直前の再確認で既に処理済みと判明 → スキップ")
                    continue

                # 購入直前にもう一度refresh（事前チェックからの経過時間の分、鮮度を保つ）
                bets = _refresh_and_fetch_bets(race_id)
                if not bets:
                    logger.info(f"race_id={race_id}: 推奨馬券なし（refresh後） → スキップ")
                    # ロックを解放しておく。解放しないと、実際には何も購入していない
                    # にもかかわらずロックが IN_PROGRESS_STALE_MINUTES 分間ブロックし
                    # 続け、当該レースの残り購入ウィンドウ（-5〜5分＝10分間）を
                    # ほぼ使い切ってしまう（/code-review指摘）。
                    _safe_finalize_purchase_lock(project_id, target_date, race_id, "failed")
                    continue

                bets_purchased = 0
                bets_failed = 0
                bets_skipped_budget = 0
                bets_need_confirmation = 0
                race_amount = 0
                race_status = "processed"

                # 予算内の馬券のみ選別
                spent = fetch_daily_spent_amount(project_id, target_date)
                valid_bets: list[dict] = []
                cumulative = 0
                for bet in bets:
                    amount = int(bet["bet_amount"])
                    if spent + cumulative + amount > DAILY_BUDGET_LIMIT:
                        msg = f"本日の購入上限（{DAILY_BUDGET_LIMIT:,}円）に達しました（累計: {spent:,}円）"
                        logger.warning(msg)
                        _send_line(msg)
                        _safe_save_purchase_record(
                            project_id, target_date, race_id,
                            bet["bet_type"], bet["horse_numbers"], amount, "skipped_budget",
                        )
                        bets_skipped_budget += 1
                        budget_exceeded = True
                    else:
                        cumulative += amount
                        valid_bets.append({
                            "bet_type": bet["bet_type"],
                            "horse_numbers": bet["horse_numbers"],
                            "amount": amount,
                        })

                # 予算内の馬券を1レース分まとめて購入
                if valid_bets:
                    result = await purchaser.purchase_bets_for_race(
                        valid_bets, venue_name_with_day, race_number, start_time=start_time
                    )
                    status = result["status"]
                    error_message = result.get("error_message")
                    debug = result.get("debug")

                    if status == "success":
                        bets_purchased = len(valid_bets)
                        race_amount = result.get("total_amount", cumulative)
                        for bet in valid_bets:
                            _safe_save_purchase_record(
                                project_id, target_date, race_id,
                                bet["bet_type"], bet["horse_numbers"], bet["amount"], "success",
                            )
                    elif status == "need_confirmation":
                        # 投票送信後にエラーが発生 = 実際に購入済みの可能性がある（二重購入防止のためリトライ済みでない）。
                        # 実際に投票されていた場合の金額を race_results / LINE通知に正しく反映する。
                        bets_need_confirmation = len(valid_bets)
                        race_amount = result.get("total_amount", cumulative)
                        race_status = "need_confirmation"
                        bet_summary = _format_bet_summary(valid_bets)
                        msg = (
                            f"【要確認】馬券購入結果不明: {venue_name}{race_number}R [{bet_summary}] "
                            f"- {error_message}{_debug_suffix(debug)}"
                        )
                        logger.error(msg)
                        _send_line(msg)
                        for bet in valid_bets:
                            _safe_save_purchase_record(
                                project_id, target_date, race_id,
                                bet["bet_type"], bet["horse_numbers"], bet["amount"],
                                "need_confirmation", error_message,
                            )
                    else:
                        bets_failed = len(valid_bets)
                        bet_summary = _format_bet_summary(valid_bets)
                        msg = (
                            f"馬券購入失敗: {venue_name}{race_number}R [{bet_summary}] "
                            f"- {error_message}{_debug_suffix(debug)}"
                        )
                        # 購入ウィンドウを-5〜+5分に拡張したことで（Issue #433）、
                        # 直前tickで未購入だったレースが発走後（既に締切済み）に
                        # 再挑戦され、「締め切られました」で失敗するのは設計上
                        # 想定内の挙動。これを他の失敗と同じ警告レベルでLINE
                        # 通知すると、既に終わったレースについて運用担当者に
                        # 「異常が起きた」と誤解させるノイズになる
                        # （/code-review指摘）。BQへの記録は他の失敗と同様に行う。
                        # フェーズ1（投票送信前）の失敗は「購入画面エラー（3回試行）:
                        # 締め切られました」のように元エラーを包んだ合成メッセージに
                        # なるため、完全一致ではなくsubstringで判定する必要がある
                        # （14回目の/code-review指摘）。完全一致のままだと、発走後の
                        # 締切がフェーズ1側で先に検知されたケースが素通りし、既に
                        # 終わったレースへの無駄なリトライ・ノイズアラートを防げて
                        # いなかった。
                        if error_message and any(
                            pat in error_message for pat in ("締め切られました", "締め切り")
                        ):
                            logger.info(f"[想定内: 発走済みのため締切] {msg}")
                        else:
                            logger.warning(msg)
                            _send_line(msg)
                        for bet in valid_bets:
                            _safe_save_purchase_record(
                                project_id, target_date, race_id,
                                bet["bet_type"], bet["horse_numbers"], bet["amount"], "failed", error_message,
                            )

                # need_confirmation（投票結果不明・要手動確認）は、一部の馬券が予算超過で
                # skipped_budgetになっていても最優先で表面化させる。skipped_budgetで
                # 上書きすると「実際には投票され金額が動いたかもしれない」状態が
                # race_results/APIレスポンス上で見えなくなってしまう（/code-review指摘）。
                if race_status == "need_confirmation":
                    final_status = "need_confirmation"
                elif budget_exceeded:
                    final_status = "skipped_budget"
                else:
                    final_status = race_status

                # 購入ロックの最終ステータスは、レポート用の final_status（"processed"
                # 等、API応答用のラベル）とは別に、「実際に金銭が動いた可能性が
                # あるか」で独立に判定する（Issue #435）。need_confirmationは
                # サーバに送信済みの可能性があるため最優先でブロック、1件でも
                # 購入成功していればsuccessとして恒久的にブロックする（一部の
                # 馬券がskipped_budgetでも同様）。いずれにも該当しなければ
                # （全馬券が失敗/予算超過等）、次tickでの再挑戦を許可する。
                if bets_need_confirmation > 0:
                    lock_status = "need_confirmation"
                elif bets_purchased > 0:
                    lock_status = "success"
                else:
                    lock_status = "failed"
                _safe_finalize_purchase_lock(project_id, target_date, race_id, lock_status)

                race_results.append({
                    "race_id": race_id,
                    "bets_purchased": bets_purchased,
                    "bets_failed": bets_failed,
                    "bets_skipped_budget": bets_skipped_budget,
                    "bets_need_confirmation": bets_need_confirmation,
                    "amount": race_amount,
                    "status": final_status,
                })
            except Exception as e:
                logger.error(f"race_id={race_id}: 購入処理中に想定外のエラー: {e}", exc_info=True)
                # failedで解放し次tickでの再挑戦を許可する。想定外エラーの詳細
                # （purchase_history相当の監査証跡）はログ・LINE通知に残るため、
                # purchase_locksには結果ステータスのみ記録する（Issue #435）。
                _safe_finalize_purchase_lock(project_id, target_date, race_id, "failed")
                _send_line(
                    f"【エラー】{venue_name}{race_number}R の購入処理中に想定外のエラーが発生しました: {e}"
                )
                race_results.append({
                    "race_id": race_id,
                    "bets_purchased": 0,
                    "bets_failed": 0,
                    "bets_skipped_budget": 0,
                    "bets_need_confirmation": 0,
                    "amount": 0,
                    "status": "error",
                })
                # このレース内で既に予算超過が判明していた場合（一部の馬券が
                # skipped_budgetになった後、購入呼び出し自体が例外を送出したケース）、
                # except節のcontinueがこのチェックを素通りしてしまうと以降のレースの
                # 処理を止められない（/code-review指摘）。もっとも、次レースの予算
                # 判定はBQから毎回取得する実際の使用済み金額（spent）に基づいて
                # 独立に行われるため上限自体は超過しない（安全性バグではない）が、
                # 明らかに無駄な試行を避けるため同様にbreakする。
                if budget_exceeded:
                    logger.warning("予算上限到達のため以降のレースをスキップします")
                    break
                if not purchaser.is_session_alive:
                    # ブラウザセッションが失われた（再ログイン失敗等）場合、同一
                    # IpatPurchaserインスタンスを使い回す以降の全レースも確実に
                    # 同じ理由で失敗する。1レースごとに紛らわしい「想定外の
                    # エラー」を繰り返す代わりに、ここで一度だけ明確に通知して
                    # tickの残りを打ち切る（/code-review指摘）。
                    msg = (
                        "【エラー】ブラウザセッションが失われたため、このtickの"
                        "残りのレース購入処理を中断しました（再ログインに失敗した"
                        "可能性があります）。"
                    )
                    logger.error(msg)
                    _send_line(msg)
                    break
                continue

            if budget_exceeded:
                logger.warning("予算上限到達のため以降のレースをスキップします")
                break
            if not purchaser.is_session_alive:
                # purchase_bets_for_race() はセッション断（再ログイン失敗）時も
                # 例外を送出せず status="failed" を返す場合がある（フェーズ1の
                # リトライが尽きて _pre_submit_failed() 経由で正常returnする
                # パス）。except節側のチェックだけでは、この正常returnパスを
                # 素通りしてしまい、1レース分無駄に試行してから次レースの例外で
                # ようやく気づく、という遠回りになっていた（/code-review指摘）。
                msg = (
                    "【エラー】ブラウザセッションが失われたため、このtickの"
                    "残りのレース購入処理を中断しました（再ログインに失敗した"
                    "可能性があります）。"
                )
                logger.error(msg)
                _send_line(msg)
                break

    total_spent = _safe_fetch_daily_spent_amount()
    # need_confirmation（投票結果不明・要手動確認）のレースも total_amount（=
    # fetch_daily_spent_amount が success/need_confirmation を合算）には
    # 実際に使われた可能性がある金額として計上されているため、purchased_races
    # からも除外しない。除外すると「金額は動いたのに購入件数は0件」という
    # 矛盾したレスポンスになり、要確認レースの見落としにつながる（/code-review指摘）。
    purchased_races = sum(
        1 for r in race_results if r["bets_purchased"] > 0 or r["bets_need_confirmation"] > 0
    )

    logger.info(
        f"IPAT日次購入完了: 購入レース={purchased_races}件, 当日累計={total_spent:,}円"
    )

    # 1レース単位の例外保護（Issue #433対応）により、以前は不正なstrategy_config.yaml
    # デプロイ等の「全レース共通の問題」がHTTP 500として大きく可視化されていたのが、
    # 今は各レースごとにstatus='error'として静かに握りつぶされ、レスポンス全体は
    # status='success'のままになってしまう（/code-review指摘: レジリエンス設計の
    # トレードオフ）。対象レースが1件以上あり、その全てがerrorだった場合のみ、
    # 個別レース保護は維持したまま、レスポンス全体のstatusでシステム的な問題を
    # 検知できるようにする。
    if race_results and all(r["status"] == "error" for r in race_results):
        msg = (
            f"【エラー】本日購入対象の全{len(race_results)}レースで想定外のエラーが"
            f"発生しました。investment_decisions等のデータ異常の可能性があります。"
        )
        logger.error(msg)
        _send_line(msg)
        return {
            "status": "error",
            "purchased_races": purchased_races,
            "total_amount": total_spent,
            "results": race_results,
        }

    return {
        "status": "success",
        "purchased_races": purchased_races,
        "total_amount": total_spent,
        "results": race_results,
    }


def _run_retrain(
    project_id: str,
    execution_date: datetime.date,
    n_trials: int | None,
    tune_timeout: int | None,
) -> dict:
    """
    モデル再学習パイプラインを同期実行する内部関数。

    train_pipeline() を tune=True で呼び出し、GCSへ保存したモデルのURIと
    検証指標・チューニング結果を返す。
    """
    from src.models.train import load_config, train_pipeline

    config = load_config()
    result = train_pipeline(
        project_id=project_id,
        execution_date=execution_date,
        config=config,
        tune=True,
        n_trials=n_trials,
        tune_timeout=tune_timeout,
    )
    return result


@app.post("/api/v1/model/retrain", response_model=RetrainResponse)
async def retrain_model(request: RetrainRequest):
    """
    LightGBMモデルを再学習してGCSに保存する（同期実行）。

    Optunaによるハイパーパラメータチューニングを実施したうえでモデルを学習し、
    GCS（gs://{project}-keiba-models/lgbm_ranker_multi/{YYYYMMDD}/）へ保存する。
    次回の /api/v1/predict/daily 呼び出し時に自動的に最新モデルが使用される。

    実行時間の目安: 1〜2時間（チューニング込み）。
    長時間処理が懸念される場合は /api/v1/model/retrain/async を使用すること。

    Cloud Schedulerから毎週月曜日 AM 8:00 JST に呼び出されることを想定。
    """
    execution_date = (
        datetime.date.fromisoformat(request.execution_date)
        if request.execution_date
        else _today_jst()
    )
    logger.info(
        f"モデル再学習リクエスト受信: execution_date={execution_date}, "
        f"n_trials={request.n_trials}, tune_timeout={request.tune_timeout}"
    )

    project_id = os.environ.get("GCP_PROJECT_ID")
    if not project_id:
        raise HTTPException(status_code=500, detail="GCP_PROJECT_IDが未設定です")

    try:
        result = await asyncio.to_thread(
            _run_retrain,
            project_id=project_id,
            execution_date=execution_date,
            n_trials=request.n_trials,
            tune_timeout=request.tune_timeout,
        )
        logger.info(
            f"モデル再学習完了: gcs_uri={result.get('gcs_uri')}, "
            f"metrics={result.get('metrics')}"
        )
        return RetrainResponse(
            status="success",
            execution_date=str(execution_date),
            gcs_uri=result.get("gcs_uri"),
            metrics=result.get("metrics", {}),
            tuning=result.get("tuning"),
        )
    except Exception as e:
        logger.error(f"モデル再学習エラー: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e)) from e


@app.post("/api/v1/model/retrain/async")
async def retrain_model_async(request: RetrainRequest, background_tasks: BackgroundTasks):
    """
    LightGBMモデルを再学習してGCSに保存する（非同期実行）。

    処理はバックグラウンドで実行され、すぐに受付レスポンスを返す。
    Cloud Schedulerから呼び出す場合はこちらを使用する（attempt-deadline超過を防ぐため）。
    """
    execution_date = (
        datetime.date.fromisoformat(request.execution_date)
        if request.execution_date
        else _today_jst()
    )
    logger.info(
        f"非同期モデル再学習リクエスト受付: execution_date={execution_date}"
    )

    project_id = os.environ.get("GCP_PROJECT_ID")
    if not project_id:
        raise HTTPException(status_code=500, detail="GCP_PROJECT_IDが未設定です")

    def run_retrain_task():
        try:
            result = _run_retrain(
                project_id=project_id,
                execution_date=execution_date,
                n_trials=request.n_trials,
                tune_timeout=request.tune_timeout,
            )
            logger.info(
                f"非同期モデル再学習完了: gcs_uri={result.get('gcs_uri')}, "
                f"metrics={result.get('metrics')}"
            )
        except Exception as e:
            logger.error(f"非同期モデル再学習エラー: {e}", exc_info=True)

    background_tasks.add_task(run_retrain_task)

    return {
        "status": "accepted",
        "execution_date": str(execution_date),
        "message": "モデル再学習をバックグラウンドで開始しました",
    }


def create_app() -> FastAPI:
    """アプリケーションファクトリ"""
    return app


if __name__ == "__main__":
    import uvicorn

    port = int(os.environ.get("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)
