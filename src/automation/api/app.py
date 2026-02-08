"""
パイプラインAPI

Cloud Run用HTTPエンドポイントを提供する。
- 日次パイプライン: Cloud Schedulerからトリガー
- 全件ロード: 手動トリガー（初回セットアップ/データ補完）

Issue #57: 日次パイプラインの実装
Issue #58: 過去分全件ロード処理の実装
"""

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


def create_app() -> FastAPI:
    """アプリケーションファクトリ"""
    return app


if __name__ == "__main__":
    import uvicorn

    port = int(os.environ.get("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)
