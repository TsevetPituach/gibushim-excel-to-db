from dataclasses import dataclass

@dataclass
class DuplicationValidation():
    stage: int
    soldier_id: int  # 1 or 2
    assessor_id: int