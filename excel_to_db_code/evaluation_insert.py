from dataclasses import dataclass

@dataclass
class EvaluationInsert:
    grade: int
    comment: str
    stage: int
    assessor_id: int
    myun_id: int
    soldier_id: int