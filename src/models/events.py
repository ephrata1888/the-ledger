from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional
from uuid import UUID

from pydantic import BaseModel, Field


class OptimisticConcurrencyError(RuntimeError):
    pass


class DomainError(RuntimeError):
    pass


class BaseEvent(BaseModel):
    event_type: str = Field(..., description="PascalCase, past-tense ubiquitous language name")
    event_version: int = Field(default=1, ge=1, le=32767)
    payload: Dict[str, Any]


class StoredEvent(BaseModel):
    event_id: UUID
    stream_id: str
    stream_position: int
    global_position: int
    event_type: str
    event_version: int
    payload: Dict[str, Any]
    metadata: Dict[str, Any]
    recorded_at: datetime


class StreamMetadata(BaseModel):
    stream_id: str
    aggregate_type: str
    current_version: int
    created_at: datetime
    archived_at: Optional[datetime] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)

