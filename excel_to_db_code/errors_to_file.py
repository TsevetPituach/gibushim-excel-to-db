import csv
from pathlib import Path
from typing import Dict, List, Optional, Set

from excel_to_db_code.models.duplication_validation import DuplicationValidation
from excel_to_db_code.models.evaluation_insert import EvaluationInsert
from excel_to_db_code.models.excel_validation_error import ExcelValidationError


def save_validation_errors(path: Path, errors: List[ExcelValidationError]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        for e in errors:
            w.writerow([f"row_number: {e.row_number}, half: {e.half}, field: {e.field}, message: {e.message}"])


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

    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        header = (
            ["group_id", "chest_number", "candidate_name", "assessor_name", "grade", "comment"]
            + [""] * 9
            + ["group_id", "chest_number", "candidate_name", "assessor_name", "grade", "comment"]
            + ["row_number", "duplicate_halves"]
        )
        w.writerow(header)

        def half_values(ev: Optional[EvaluationInsert]) -> List[str]:
            if ev is None:
                return ["", "", "", "", "", ""]
            return [
                str(ev.group_id),
                str(ev.chest_number),
                ev.candidate_name or "",
                ev.assessor_name or "",
                str(ev.grade),
                ev.comment or "",
            ]

        for row_number in sorted(rows_with_issues):
            halves = row_to_halves.get(row_number, {})
            dup_halves = sorted(h for h in rows_with_issues[row_number] if h in (1, 2))
            duplicate_flag = ",".join(str(h) for h in dup_halves)
            row: List[str] = []
            row.extend(half_values(halves.get(1)))
            row.extend([""] * 9)
            row.extend(half_values(halves.get(2)))
            row.append(str(row_number))
            row.append(duplicate_flag)
            w.writerow(row)
