"""
日次パイプラインAPI

Cloud Run用HTTPエンドポイントを提供する。
Cloud Schedulerからトリガーされ、日次データ処理を実行する。

Issue #57: 日次パイプラインの実装
"""

import logging
import os
from datetime import date
from typing import Optional

from fastapi import BackgroundTasks, FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, field_validator

from src.pipeline.daily_pipeline import DailyPipeline, PipelineResult

# ロギング設定（Cloud Logging連携）
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# FastAPIアプリケーション
app = FastAPI(
    title="JRDB Daily Pipeline API",
    description="JRDBデータの日次処理パイプラインAPI",
    version="1.0.0",
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
    return HealthResponse(status="ok", version="1.0.0")


@app.get("/health", response_model=HealthResponse)
async def health_check():
    """ヘルスチェックエンドポイント"""
    return HealthResponse(status="healthy", version="1.0.0")


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


def create_app() -> FastAPI:
    """アプリケーションファクトリ"""
    return app


if __name__ == "__main__":
    import uvicorn

    port = int(os.environ.get("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)
