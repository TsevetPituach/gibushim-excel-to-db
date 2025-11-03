from dataclasses import dataclass
from typing import Optional


@dataclass
class EvaluationInsert:
    row_number: int
    half_index: int
    group_id: int
    chest_number: int
    candidate_name: Optional[str]
    assessor_name: Optional[str]
    grade: int
    comment: str
    stage: int
    assessor_id: int
    myun_id: int
    soldier_id: int
