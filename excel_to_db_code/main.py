import argparse
import csv
from pathlib import Path
from typing import List, Optional, Tuple

from tqdm import tqdm

from .db import DB
from .logger import get_logger, setup_logging
from .excel_reader import iter_excel_halves
from .validators import HalfInput, ValidationError, validate_half, find_duplicate_keys_in_excel
from .evaluation_insert import EvaluationInsert


log = get_logger(__name__)

def main(argv: Optional[List[str]] = None) -> int:
    args = _parse_args(argv)
    setup_logging("INFO")
    log.info("Starting the script  ")

    excel_path = Path(args.excel)
    output_path = Path(args.output)
    errors_csv = output_path.parent / "validation_errors.csv"

    db = DB(args.dsn)

    inserts, all_errors = _collect_inserts(db=db, excel_path=excel_path, stage=args.stage)

    # Simple duplicate check by (stage, assessor_id, soldier_id) before DB work
    duplicate_inserts = find_duplicate_keys_in_excel(inserts)
    if duplicate_inserts:
        log.error("Duplicates in the Excel (stage, assessor_id, soldier_id):")
        for d in duplicate_inserts:
            try:
                stage_val, assessor_id_val, soldier_id_val = d  # type: ignore[misc]
            except Exception:
                stage_val = getattr(d, "stage", None)
                assessor_id_val = getattr(d, "assessor_id", None)
                soldier_id_val = getattr(d, "soldier_id", None)
            log.error(
                f"stage: {stage_val}, assessor_id: {assessor_id_val}, soldier_id: {soldier_id_val}"
            )
        # Fail fast to avoid unexpected DB state; replace with RuntimeError for always-on enforcement
        assert not duplicate_inserts, f"Duplicate keys: {duplicate_inserts}"


    if all_errors:
        _save_validation_errors(errors_csv, all_errors)
        log.warning("Validation errors found: %d (see %s)", len(all_errors), errors_csv)

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
        return 3
    

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
        default=str(Path("out") / "insert_api_assessorevaluation.sql"),
        help="Output .sql file path",
    )
    return p.parse_args(argv)


def _save_validation_errors(path: Path, errors: List[ValidationError]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["row_number", "half", "field", "message"])
        for e in errors:
            w.writerow([e.row_number, e.half, e.field, e.message])

def _resolve_evaluation_ids(
    db: DB,
    half: HalfInput,
    stage: int,
    row_number: int,
    half_index: int,
) -> Tuple[Optional[Tuple[int, int, int]], List[ValidationError]]:
    """Resolve (soldier_id, myun_id, assessor_id) via DB lookups.
    Returns tuple or (None, [errors]).
    """
    errors: List[ValidationError] = []

    soldier_id = db.get_participant_id(half.chest_number)
    if soldier_id is None:
        errors.append(
            ValidationError(
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
            ValidationError(
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
) -> Tuple[List[EvaluationInsert], List[ValidationError]]:
    all_errors: List[ValidationError] = []
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

    with db.cursor() as cur, output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
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
            writer.writerow(
                [
                    f"grade: {ev.grade}",
                    f"comment: {ev.comment}",
                    f"stage: {ev.stage}",
                    f"assessor_id: {ev.assessor_id}",
                    f"myun_id: {ev.myun_id}",
                    f"soldier_id: {ev.soldier_id}",
                    f"status: {status}",
                ]
            )

    return inserted_rows, skipped_rows


if __name__ == "__main__":
    raise SystemExit(main())
