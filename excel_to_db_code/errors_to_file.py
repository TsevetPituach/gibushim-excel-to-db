import csv
from pathlib import Path
from typing import List

from excel_to_db_code.db import DB
from excel_to_db_code.models.duplication_validation import DuplicationValidation
from excel_to_db_code.models.excel_validation_error import ExcelValidationError


def save_validation_errors(path: Path, errors: List[ExcelValidationError]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        for e in errors:
            w.writerow([f"row_number: {e.row_number}, half: {e.half}, field: {e.field}, message: {e.message}"])

def save_excel_duplications_errors(path: Path, db: DB, duplicate_inserts: List[DuplicationValidation], msg: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        for d in duplicate_inserts:
            chest = db.get_chest_number(d.soldier_id)
            chest_str = str(chest) if chest is not None else ""
            w.writerow([f"{msg}: stage: {d.stage}, assessor_id: {d.assessor_id}, soldier_id: {d.soldier_id}, chest_number: {chest_str}"])