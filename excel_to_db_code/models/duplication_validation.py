from dataclasses import dataclass, field
from typing import Optional


@dataclass
class DuplicationValidation:
    stage: int
    soldier_id: int
    assessor_id: int
    group_id: Optional[int] = field(default=None, compare=False)
    chest_number: Optional[int] = field(default=None, compare=False)
    candidate_name: Optional[str] = field(default=None, compare=False)
    assessor_name: Optional[str] = field(default=None, compare=False)
    grade: Optional[int] = field(default=None, compare=False)
    comment: Optional[str] = field(default=None, compare=False)
    row_number: Optional[int] = field(default=None, compare=False)
    half_index: Optional[int] = field(default=None, compare=False)
