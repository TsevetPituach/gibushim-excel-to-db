from typing import Any, Optional
from pydantic import BaseModel, validator


class HalfInput(BaseModel):
    group_id: int
    chest_number: int
    candidate_name: Optional[str] = None
    assessor_name: Optional[str] = None
    grade: int
    comment: str

    @validator("comment", pre=True)
    def ensure_comment_str(cls, v: Any) -> str:
        if v is None:
            return ""
        return str(v)

