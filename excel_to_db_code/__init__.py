__all__ = [
    "__version__",
]

__version__ = "0.1.0"

# Ensure vendored dependencies are importable when running without a venv.
# This complements top-level sitecustomize.py for environments where
# sitecustomize is not auto-imported (e.g., running `-m excel_to_db_code.cli`).
try:  # keep import side-effects minimal and safe
    import os
    import sys

    _ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    _VENDOR = os.path.join(_ROOT, "vendor")
    if os.path.isdir(_VENDOR) and _VENDOR not in sys.path:
        sys.path.insert(0, _VENDOR)
except Exception:
    # Never fail import due to vendor path tweaking
    pass

