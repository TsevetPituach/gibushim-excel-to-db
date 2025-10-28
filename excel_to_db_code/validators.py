from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from pydantic import BaseModel, validator

from excel_to_db_code.evaluation_insert import EvaluationInsert


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

@dataclass
class InsertUniqueKeys(BaseModel):
    stage: int
    assessor_id: int
    soldier_id: int


def is_empty_value(v: Any) -> bool:
    return v is None or (isinstance(v, str) and v.strip() == "")


def is_half_empty(d: Dict[str, Any]) -> bool:
    """A half is considered empty if all six fields are empty/None."""
    keys = [
        "group_id",
        "chest_number",
        "candidate_name",
        "assessor_name",
        "grade",
        "comment",
    ]
    return all(is_empty_value(d.get(k)) for k in keys)


class ValidationError(BaseModel):
    row_number: int
    half: int  # 1 or 2
    field: str
    message: str


def validate_half(
    raw: Dict[str, Any],
    row_number: int,
    half: int,
) -> Tuple[Optional[HalfInput], List[ValidationError]]:
    errors: List[ValidationError] = []

    # Required fields: group_id, chest_number, grade, comment
    required_fields = ["group_id", "chest_number", "grade", "comment"]
    for field in required_fields:
        if is_empty_value(raw.get(field)):
            errors.append(
                ValidationError(
                    row_number=row_number, half=half, field=field, message="missing required field",
                )
            )

    # Type checks (ints)
    int_fields = ["group_id", "chest_number", "grade"]
    for field in int_fields:
        v = raw.get(field)
        if not is_empty_value(v):
            try:
                # Some Excel values may be floats that represent ints (e.g., 42.0)
                if isinstance(v, float) and v.is_integer():
                    raw[field] = int(v)
                else:
                    raw[field] = int(v)
            except Exception:
                errors.append(
                    ValidationError(
                        row_number=row_number,
                        half=half,
                        field=field,
                        message="must be an integer",
                    )
                )

    if errors:
        return None, errors

    try:
        model = HalfInput(**raw)
        return model, []
    except Exception as e:  # Pydantic validation
        errors.append(
            ValidationError(
                row_number=row_number, half=half, field="__model__", message=str(e)
            )
        )
        return None, errors
    

from typing import List, Tuple, Dict, Set

def _insert_key(ev: EvaluationInsert) -> Tuple[int, int, int]:
    return (ev.stage, ev.assessor_id, ev.soldier_id)

def find_duplicate_keys_in_excel(inserts: List[EvaluationInsert]) -> List[InsertUniqueKeys]:
    seen: Set[InsertUniqueKeys] = set()
    dups: Set[InsertUniqueKeys] = set()
    for ev in inserts:
        k = _insert_key(ev)
        if k in seen:
            dups.add(k)
        else:
            seen.add(k)
    return sorted(dups)


