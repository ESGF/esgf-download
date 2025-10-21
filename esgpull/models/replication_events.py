from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, model_validator
from pydantic.types import NonNegativeInt


class ReplicationStatus(str, Enum):
    """Replication operation status"""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class ReplicationOperation(BaseModel):
    """Replication operation status"""
    operation_id: str = Field(..., description="Unique operation identifier")
    dataset_id: str = Field(..., description="Dataset being replicated")
    source_node: str = Field(..., description="Source data node")
    target_node: str = Field(..., description="Target data node")
    status: ReplicationStatus = Field(..., description="Operation status")
    progress: float = Field(0.0, ge=0.0, le=1.0, description="Completion progress (0-1)")
    files_total: NonNegativeInt = Field(0, description="Total files to replicate")
    files_completed: NonNegativeInt = Field(0, description="Files successfully replicated")
    files_failed: NonNegativeInt = Field(0, description="Failed file replications")
    bytes_transferred: NonNegativeInt = Field(0, description="Bytes transferred")
    started: Optional[datetime] = Field(None, description="Operation start time")
    completed: Optional[datetime] = Field(None, description="Operation completion time")
    error_message: Optional[str] = Field(None, description="Error message if failed")

    @model_validator(mode='after')
    def validate_file_counts(cls, values):
        """Validate file count consistency"""
        total = values.get('files_total', 0)
        completed = values.get('files_completed', 0)
        failed = values.get('files_failed', 0)

        if completed + failed > total:
            raise ValueError("Completed + failed files cannot exceed total")

        return values
