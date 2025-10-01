from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from enum import Enum


class StorageType(str, Enum):
    CLICKHOUSE = "clickhouse"
    POSTGRESQL = "postgresql"
    HDFS = "hdfs"


class DataFormat(str, Enum):
    CSV = "csv"
    JSON = "json"
    XML = "xml"


class ScheduleHint(str, Enum):
    REALTIME = "realtime"
    HOURLY = "hourly"
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"


class DataProfile(BaseModel):
    format: DataFormat
    record_count: int
    field_count: int
    has_temporal: bool
    has_numeric: bool
    has_text: bool
    has_categorical: bool
    has_spatial: bool
    has_nested: bool
    unique_ids: List[str]
    temporal_range: Optional[List[str]] = None
    estimated_size_mb: float


class RecommendationResponse(BaseModel):
    target: StorageType
    confidence: float = Field(..., ge=0.0, le=1.0)
    rationale: str
    schedule_hint: ScheduleHint
    ddl_hints: List[str]
    ddl_script: str
    data_profile: DataProfile
    file_info: Optional[Dict[str, Any]] = None
    validation_errors: Optional[List[str]] = None
    validation_warnings: Optional[List[str]] = None


class AnalysisRequest(BaseModel):
    file_path: str
    format: DataFormat


class AnalysisResponse(BaseModel):
    data_profile: DataProfile
    features: Dict[str, Any]
    file_info: Optional[Dict[str, Any]] = None