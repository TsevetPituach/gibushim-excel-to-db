from __future__ import annotations

from contextlib import contextmanager
from typing import List, Optional, Sequence, Tuple
from enum import Enum

import psycopg2
from psycopg2.extensions import connection as PGConnection
from psycopg2.extras import execute_values, NamedTupleCursor
from .models.duplication_validation import DuplicationValidation

from .logger import get_logger

log = get_logger(__name__)


class DB:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self._conn: Optional[PGConnection] = None

    def connect(self) -> None:
        if self._conn is None:
            log.debug("Connecting to PostgreSQL")
            self._conn = psycopg2.connect(self.dsn)
            self._conn.autocommit = False

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    @contextmanager
    def cursor(self, cursor_factory=None):
        if self._conn is None:
            self.connect()
        assert self._conn is not None
        if cursor_factory is not None:
            cur = self._conn.cursor(cursor_factory=cursor_factory)
        else:
            cur = self._conn.cursor()
        try:
            yield cur
        finally:
            cur.close()

    def commit(self) -> None:
        if self._conn:
            self._conn.commit()

    def rollback(self) -> None:
        if self._conn:
            self._conn.rollback()

    # Cross-reference lookups
    def get_participant_id(self, chest_number: int) -> Optional[int]:
        sql = "SELECT id FROM api_participant WHERE chest_number = %s"
        with self.cursor() as cur:
            cur.execute(sql, (chest_number,))
            rows = cur.fetchall()
        if len(rows) == 1:
            return int(rows[0][0])
        return None

    def get_chest_number(self, soldier_id: int) -> Optional[int]:
        """Return chest_number for a given participant id (soldier_id)."""
        sql = "SELECT chest_number FROM api_participant WHERE id = %s"
        with self.cursor() as cur:
            cur.execute(sql, (soldier_id,))
            row = cur.fetchone()
        if row:
            return int(row[0])
        return None


    def find_existing_evaluation_keys(
        self, keys: Sequence[DuplicationValidation]
    ) -> List[DuplicationValidation]:
        """
        Given (stage, assessor_id, soldier_id) keys, return those that already exist
        in api_assessorevaluation.
        """
        if not keys:
            return []
        sql = (
            """
            SELECT e.stage, e.assessor_id, e.soldier_id
            FROM api_assessorevaluation e
            JOIN (VALUES %s) AS v(stage, assessor_id, soldier_id)
              ON (e.stage, e.assessor_id, e.soldier_id) = (v.stage, v.assessor_id, v.soldier_id)
            JOIN api_participant p ON p.id = e.soldier_id
            """
        )
        # Build args list of tuples for execute_values
        argslist = [(int(k.stage), int(k.assessor_id), int(k.soldier_id)) for k in keys]
        with self.cursor(cursor_factory=NamedTupleCursor) as cur:
            execute_values(cur, sql, argslist)
            rows = cur.fetchall()
        # rows come as named tuples: (stage, assessor_id, soldier_id)
        return [
            DuplicationValidation(
                stage=int(r.stage),
                assessor_id=int(r.assessor_id),
                soldier_id=int(r.soldier_id),
            )
            for r in rows
        ]


    class AssessorRole(str, Enum):
        GROUP_COMMANDER = "מפקד קבוצה"
        GROUP_RESPONSIBLE = "אחראי בטיחות"

    def get_assessoringroup(
            self,
            group_id: int,
            stage: int,
            role: AssessorRole = AssessorRole.GROUP_COMMANDER,
    ) -> Optional[Tuple[int, int]]:
        sql = (
            "SELECT myun_id, assessor_id FROM api_assessoringroup "
            "WHERE group_id = %s AND stage = %s AND role = %s"
        )
        with self.cursor() as cur:
            cur.execute(sql, (group_id, stage, role.value))
            rows = cur.fetchall()
        if len(rows) == 1:
            r = rows[0]
            return int(r[0]), int(r[1])
        return None

