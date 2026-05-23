"""공유 credentials 폴더 경로 (cursor/credentials/google-sa.json)."""
import os
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_CREDENTIALS = (PROJECT_ROOT.parent / "credentials" / "google-sa.json").resolve()


def resolve_credentials_path() -> str:
    load_dotenv(PROJECT_ROOT / ".env")
    raw = os.getenv("CREDENTIALS_FILE", "").strip()
    if raw:
        path = Path(raw)
        if not path.is_absolute():
            path = (PROJECT_ROOT / path).resolve()
        return str(path)
    return str(DEFAULT_CREDENTIALS)
