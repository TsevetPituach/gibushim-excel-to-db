from typing import Any, Dict, List, Optional, Tuple, Set

from excel_to_db_code.models.duplication_validation import DuplicationValidation
from excel_to_db_code.models.excel_validation_error import ExcelValidationError

from .models import HalfInput
from .models.evaluation_insert import EvaluationInsert
from .db import DB


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


def validate_half(
    raw: Dict[str, Any],
    row_number: int,
    half: int,
) -> Tuple[Optional[HalfInput], List[ExcelValidationError]]:
    errors: List[ExcelValidationError] = []

    # Required fields: group_id, chest_number, grade, comment
    required_fields = ["group_id", "chest_number", "grade", "comment"]
    for field in required_fields:
        if is_empty_value(raw.get(field)):
            errors.append(
                ExcelValidationError(
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
                    ExcelValidationError(
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
            ExcelValidationError(
                row_number=row_number, half=half, field="__model__", message=str(e)
            )
        )
        return None, errors
    

def _insert_keys(ev: EvaluationInsert) -> DuplicationValidation:
    return DuplicationValidation(
        stage=ev.stage,
        assessor_id=ev.assessor_id,
        soldier_id=ev.soldier_id,
        group_id=ev.group_id,
        chest_number=ev.chest_number,
        candidate_name=ev.candidate_name,
        assessor_name=ev.assessor_name,
        grade=ev.grade,
        comment=ev.comment,
        row_number=ev.row_number,
        half_index=ev.half_index,
    )


def find_duplicate_keys_in_excel(inserts: List[EvaluationInsert]) -> List[DuplicationValidation]:
    """Return duplicates by (stage, assessor_id, soldier_id) present in Excel-derived inserts."""
    seen: Dict[Tuple[int, int, int], DuplicationValidation] = {}
    duplicates_map: Dict[Tuple[int, int, int], List[DuplicationValidation]] = {}
    duplications: List[DuplicationValidation] = []
    for ev in inserts:
        key = (ev.stage, ev.assessor_id, ev.soldier_id)
        current = _insert_keys(ev)
        if key in seen:
            entry_list = duplicates_map.setdefault(key, [seen[key]])
            entry_list.append(current)
        else:
            seen[key] = current
    for entries in duplicates_map.values():
        duplications.extend(entries)
    return duplications


def find_duplicate_keys_in_db(db: DB, inserts: List[EvaluationInsert]) -> List[DuplicationValidation]:
    """Check which (stage, assessor_id, soldier_id) from inserts already exist in the DB."""
    inserts_keys: List[DuplicationValidation] = [_insert_keys(ev) for ev in inserts]
    if not inserts_keys:
        return []
    existing = db.find_existing_evaluation_keys(inserts_keys)
    existing_key_set: Set[Tuple[int, int, int]] = {
        (dup.stage, dup.assessor_id, dup.soldier_id) for dup in existing
    }
    duplications: List[DuplicationValidation] = []
    for ev in inserts:
        key = (ev.stage, ev.assessor_id, ev.soldier_id)
        if key in existing_key_set:
            duplications.append(_insert_keys(ev))
    return duplications


