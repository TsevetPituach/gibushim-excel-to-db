import argparse
from pathlib import Path
from typing import List, Optional, Tuple

from excel_to_db_code.enums.exit_code import ExitCode
from excel_to_db_code.errors_to_file import save_excel_duplications_errors, save_validation_errors
from excel_to_db_code.models.duplication_validation import DuplicationValidation
from excel_to_db_code.models.excel_validation_error import ExcelValidationError
from tqdm import tqdm

from .db import DB
from .logger import get_logger, setup_logging
from .excel_reader import iter_excel_halves
from .models import HalfInput
from .validators import find_duplicate_keys_in_db, validate_half, find_duplicate_keys_in_excel
from .models.evaluation_insert import EvaluationInsert


log = get_logger(__name__)

def main(argv: Optional[List[str]] = None) -> int:
    args = _parse_args(argv)
    setup_logging("INFO")
    log.info("Starting the script  ")

    excel_path = Path(args.excel)
    output_path = Path(args.output)
    errors_csv = output_path.parent / "validation_errors.xlsx"
    duplications_csv = output_path.parent / "duplications_errors.xlsx"

    db = DB(args.dsn)

    inserts, all_errors = _collect_inserts(db=db, excel_path=excel_path, stage=args.stage)
    validation_num = _validate_inserts(inserts, db, all_errors, errors_csv, duplications_csv)
    if validation_num != ExitCode.OK:
        return int(validation_num)

    sql = _build_insert_sql()

    try:
        inserted_rows, skipped_rows = _execute_inserts_and_log(
            db=db, inserts=inserts, output_path=output_path, sql=sql
        )
        db.commit()
        log.info(
            "Executed %d inserts (%d skipped due to conflict). Log: %s",
            inserted_rows,
            skipped_rows,
            output_path,
        )
    except Exception as e:
        db.rollback()
        log.exception("Execution failed; transaction rolled back: %s", e)
        return int(ExitCode.RUNTIME_ERROR)
    

def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="excel_to_db_code",
        description="Read Excel, validate, cross-ref with Postgres, and generate INSERTs.",
    )
    p.add_argument("--excel", required=True, help="Path to Excel file (e.g., example/input.xlsx)")
    p.add_argument("--stage", type=int, required=True, help="Stage number for evaluation")
    p.add_argument("--dsn", required=True, help="PostgreSQL DSN")
    p.add_argument(
        "--output",
        default=str(Path("logs") / "insert_log.xlsx"),
        help="Execution log output path (Excel workbook)",
    )
    return p.parse_args(argv)


def _validate_inserts(
    inserts: List[EvaluationInsert],
    db: DB,
    all_errors: List[ExcelValidationError],
    errors_csv: Path,
    duplications_csv: Path,
) -> ExitCode:
    if all_errors:
        save_validation_errors(errors_csv, all_errors)
        log.error("Validation errors found: %d (see %s)", len(all_errors), errors_csv)
        return ExitCode.VALIDATION_ERROR

    duplicate_inserts_in_excel: List[DuplicationValidation] = find_duplicate_keys_in_excel(inserts)
    if len(duplicate_inserts_in_excel) > 0:
        save_excel_duplications_errors(duplications_csv, duplicate_inserts_in_excel, inserts)
        log.error(f"Duplicates in Excel: {len(duplicate_inserts_in_excel)} (see {duplications_csv})")
        return ExitCode.DUPLICATES

    duplicate_inserts_in_excel_and_db: List[DuplicationValidation] = find_duplicate_keys_in_db(db, inserts)
    if len(duplicate_inserts_in_excel_and_db) > 0:
        save_excel_duplications_errors(duplications_csv, duplicate_inserts_in_excel_and_db, inserts)
        log.error(f"Duplicates in Excel and DB: {len(duplicate_inserts_in_excel_and_db)} (see {duplications_csv})")
        return ExitCode.DUPLICATES
    
    return ExitCode.OK


def _resolve_evaluation_ids(
    db: DB,
    half: HalfInput,
    stage: int,
    row_number: int,
    half_index: int,
) -> Tuple[Optional[Tuple[int, int, int]], List[ExcelValidationError]]:
    """Resolve (soldier_id, myun_id, assessor_id) via DB lookups.
    Returns tuple or (None, [errors]).
    """
    errors: List[ExcelValidationError] = []

    soldier_id = db.get_participant_id(half.chest_number)
    if soldier_id is None:
        errors.append(
            ExcelValidationError(
                row_number=row_number,
                half=half_index,
                field="chest_number",
                message="participant not found or not unique",
            )
        )

    # Choose role by Excel half: 1 -> מפקד קבוצה, 2 -> אחראי קבוצה
    role = (
        DB.AssessorRole.GROUP_COMMANDER if half_index == 1 else DB.AssessorRole.GROUP_RESPONSIBLE
    )
    # Support both real DB (with role param) and FakeDB in tests (without it)
    try:
        sql_result = db.get_assessoringroup(group_id=half.group_id, stage=stage, role=role)  # type: ignore[arg-type]
    except TypeError:
        sql_result = db.get_assessoringroup(group_id=half.group_id, stage=stage)  # type: ignore[call-arg]
    if sql_result is None:
        errors.append(
            ExcelValidationError(
                row_number=row_number,
                half=half_index,
                field="group_id/stage",
                message="assessoringroup not found or not unique",
            )
        )
        myun_id = assessor_id = None  # type: ignore
    else:
        myun_id, assessor_id = sql_result

    if errors:
        return None, errors

    assert soldier_id is not None and myun_id is not None and assessor_id is not None
    return (soldier_id, myun_id, assessor_id), []


def _build_insert_sql() -> str:
    return (
        "INSERT INTO api_assessorevaluation "
        "(grade, comment, stage, assessor_id, myun_id, soldier_id) "
        "VALUES (%s, %s, %s, %s, %s, %s) "
        "ON CONFLICT DO NOTHING RETURNING 1"
    )


def _collect_inserts(
    db: DB, excel_path: Path, stage: int
) -> Tuple[List[EvaluationInsert], List[ExcelValidationError]]:
    all_errors: List[ExcelValidationError] = []
    inserts: List[EvaluationInsert] = []

    halves = list(iter_excel_halves(str(excel_path)))
    for row_number, half_index, half_raw in tqdm(halves, desc="Rows"):
        half_model, errs = validate_half(half_raw, row_number=row_number, half=half_index)
        if errs:
            all_errors.extend(errs)
            continue

        assert half_model is not None
        resolved, xerrs = _resolve_evaluation_ids(
            db=db,
            half=half_model,
            stage=stage,
            row_number=row_number,
            half_index=half_index,
        )
        if xerrs:
            all_errors.extend(xerrs)
            continue

        assert resolved is not None
        soldier_id, myun_id, assessor_id = resolved
        inserts.append(
            EvaluationInsert(
                row_number=row_number,
                half_index=half_index,
                group_id=half_model.group_id,
                chest_number=half_model.chest_number,
                candidate_name=half_model.candidate_name,
                assessor_name=half_model.assessor_name,
                grade=half_model.grade,
                comment=half_model.comment,
                stage=stage,
                assessor_id=assessor_id,
                myun_id=myun_id,
                soldier_id=soldier_id,
            )
        )

    return inserts, all_errors


def _execute_inserts_and_log(
    db: DB, inserts: List[EvaluationInsert], output_path: Path, sql: str
) -> Tuple[int, int]:
    inserted_rows = 0
    skipped_rows = 0
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with db.cursor() as cur:
        from openpyxl import Workbook

        wb = Workbook()
        ws = wb.active
        ws.append(
            [
                "grade",
                "comment",
                "stage",
                "assessor_id",
                "myun_id",
                "soldier_id",
                "status",
            ]
        )
        for ev in tqdm(inserts, desc="Executing"):
            cur.execute(
                sql,
                (
                    ev.grade,
                    ev.comment,
                    ev.stage,
                    ev.assessor_id,
                    ev.myun_id,
                    ev.soldier_id,
                ),
            )
            if cur.rowcount and cur.rowcount > 0:
                status = "inserted"
                inserted_rows += 1
            else:
                status = "skipped"
                skipped_rows += 1
            ws.append(
                [
                    ev.grade,
                    ev.comment,
                    ev.stage,
                    ev.assessor_id,
                    ev.myun_id,
                    ev.soldier_id,
                    status,
                ]
            )

        wb.save(output_path)

    return inserted_rows, skipped_rows


if __name__ == "__main__":
    raise SystemExit(main())
