"""
Auto-add the local vendor/ directory to sys.path so third-party
libraries installed into the project folder are importable without
touching the system/site environment. This is executed automatically
by Python if it's on sys.path (project root is on sys.path when you
run from this folder or use -m excel_to_db_code.cli).
"""
from __future__ import annotations

import os
import sys

_ROOT = os.path.dirname(os.path.abspath(__file__))
_VENDOR = os.path.join(_ROOT, "vendor")

if os.path.isdir(_VENDOR) and _VENDOR not in sys.path:
    sys.path.insert(0, _VENDOR)

