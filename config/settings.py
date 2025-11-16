import os
from pathlib import Path
from urllib.parse import parse_qs, urlparse


def _load_env_file() -> None:
    env_path = Path(__file__).resolve().parents[1] / ".env"
    if not env_path.exists():
        return
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


_load_env_file()


def _env(key: str, default: str | None = None) -> str | None:
    """
    Wrapper to centralize access to environment variables, keeping defaults in one place.
    """
    return os.getenv(key, default)


def _env_float(key: str, default: float) -> float:
    """
    Parses float environment variables with sane fallback and non-negative constraint.
    """
    value = _env(key)
    if value is None:
        return default
    try:
        parsed = float(value)
    except ValueError:
        return default
    return parsed if parsed >= 0 else 0.0


def _parse_db_url(url: str | None):
    if not url:
        return None
    parsed = urlparse(url)
    if parsed.scheme not in {"postgresql", "postgres"}:
        return None

    config = {
        "dbname": parsed.path.lstrip("/") or None,
        "user": parsed.username,
        "password": parsed.password,
        "host": parsed.hostname,
        "port": str(parsed.port) if parsed.port else None,
    }

    query_params = parse_qs(parsed.query)
    for key, values in query_params.items():
        if values:
            config[key] = values[0]

    return {k: v for k, v in config.items() if v is not None}


DB_CONFIG = {
    "dbname": _env("DB_NAME", "obamixteste"),
    "user": _env("DB_USER", "postgres"),
    "password": _env("DB_PASSWORD"),
    "host": _env("DB_HOST", "localhost"),
    "port": _env("DB_PORT", "5432"),
    "maintenance_db": _env("DB_MAINTENANCE", "postgres"),
    "auto_create_db": _env("DB_AUTO_CREATE", "true").lower() != "false",
}

NEON_DB_CONFIG = _parse_db_url(_env("NEON_DB_URL"))
if NEON_DB_CONFIG is not None:
    NEON_DB_CONFIG.setdefault("auto_create_db", False)

DATABASE_TARGETS = [("local", DB_CONFIG)]
if NEON_DB_CONFIG:
    DATABASE_TARGETS.append(("neon", NEON_DB_CONFIG))


START_URL = "https://app.obaobamix.com.br/login"
PRODUCTS_URL = "https://app.obaobamix.com.br/admin/products"
LOGIN_EMAIL = _env("OBA_EMAIL")
LOGIN_PASSWORD = _env("OBA_PASSWORD")
SCRAPER_ROW_DELAY = _env_float("SCRAPER_ROW_DELAY", 0.75)
SCRAPER_PAGE_DELAY = _env_float("SCRAPER_PAGE_DELAY", 1.5)
