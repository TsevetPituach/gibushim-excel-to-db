from pathlib import Path
from typing import Dict, List, Optional, Set

from openpyxl import Workbook

from excel_to_db_code.models.duplication_validation import DuplicationValidation
from excel_to_db_code.models.evaluation_insert import EvaluationInsert
from excel_to_db_code.models.excel_validation_error import ExcelValidationError


def save_validation_errors(path: Path, errors: List[ExcelValidationError]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    wb = Workbook()
    ws = wb.active
    ws.title = "validation_errors"
    ws.append(["row_number", "half", "field", "message"])
    for e in errors:
        ws.append([e.row_number, e.half, e.field, e.message])
    wb.save(path)


def save_excel_duplications_errors(
    path: Path,
    duplicate_inserts: List[DuplicationValidation],
    all_inserts: List[EvaluationInsert],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    # Build quick lookups for the Excel rows/halves so we can reconstruct the original layout.
    row_to_halves: Dict[int, Dict[int, EvaluationInsert]] = {}
    for ev in all_inserts:
        row_halves = row_to_halves.setdefault(ev.row_number, {})
        row_halves[ev.half_index] = ev

    rows_with_issues: Dict[int, Set[int]] = {}
    for dup in duplicate_inserts:
        if dup.row_number is None:
            continue
        half_idx = dup.half_index if dup.half_index in (1, 2) else 0
        rows_with_issues.setdefault(dup.row_number, set()).add(half_idx)

    wb = Workbook()
    ws = wb.active
    ws.title = "duplicates"
    header = (
        ["group_id", "chest_number", "candidate_name", "assessor_name", "grade", "comment"]
        + [""] * 9
        + ["group_id", "chest_number", "candidate_name", "assessor_name", "grade", "comment"]
        + ["row_number", "duplicate_halves"]
    )
    ws.append(header)

    def half_values(ev: Optional[EvaluationInsert]) -> List[Optional[str]]:
        if ev is None:
            return ["", "", "", "", "", ""]
        return [
            ev.group_id,
            ev.chest_number,
            ev.candidate_name or "",
            ev.assessor_name or "",
            ev.grade,
            ev.comment or "",
        ]

    for row_number in sorted(rows_with_issues):
        halves = row_to_halves.get(row_number, {})
        dup_halves = sorted(h for h in rows_with_issues[row_number] if h in (1, 2))
        duplicate_flag = ",".join(str(h) for h in dup_halves)
        row: List[Optional[str]] = []
        row.extend(half_values(halves.get(1)))
        row.extend([""] * 9)
        row.extend(half_values(halves.get(2)))
        row.append(row_number)
        row.append(duplicate_flag)
        ws.append(row)

    wb.save(path)
