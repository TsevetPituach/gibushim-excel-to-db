from __future__ import annotations

from contextlib import contextmanager
from typing import Iterable, List, Optional, Sequence, Tuple
from enum import Enum

import psycopg2
from psycopg2.extensions import connection as PGConnection

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
    def cursor(self):
        if self._conn is None:
            self.connect()
        assert self._conn is not None
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

