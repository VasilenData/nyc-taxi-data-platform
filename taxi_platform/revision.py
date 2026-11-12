"""Portable transformation identity and explicit invalidation of incremental state."""
import hashlib

from . import config


def model_files() -> dict[str, str]:
    paths = [config.DBT_DIR / "dbt_project.yml"]
    for folder in ("models", "macros", "seeds", "tests"):
        paths.extend(p for p in (config.DBT_DIR / folder).rglob("*")
                     if p.is_file() and p.suffix in {".sql", ".yml", ".yaml", ".csv"})
    return {p.relative_to(config.DBT_DIR).as_posix():
            hashlib.sha256(p.read_bytes().replace(b"\r\n", b"\n")).hexdigest()
            for p in sorted(paths)}


def model_signature() -> str:
    digest = hashlib.sha256()
    for name, checksum in sorted(model_files().items()):
        digest.update(f"{name}\0{checksum}\n".encode())
    return digest.hexdigest()


def published_revision(con) -> dict:
    tables = {r[0] for r in con.execute("""select table_name from information_schema.tables
        where table_schema = 'main'""").fetchall()}
    if "publication" not in tables:
        return {"version": None, "model_sha256": None}
    columns = {r[0] for r in con.execute("describe main.publication").fetchall()}
    signature = "model_sha256" if "model_sha256" in columns else "null"
    rows = con.execute(f"select version, {signature} from main.publication").fetchall()
    if len(rows) != 1:
        raise ValueError("Expected exactly one current publication")
    return dict(zip(("version", "model_sha256"), rows[0]))


def requires_full_refresh(con) -> bool:
    # An unknown legacy signature is conservatively rebuilt once. A new empty
    # warehouse builds identically with --full-refresh, and records its identity.
    return published_revision(con)["model_sha256"] != model_signature()
