"""Verify ownership of the disposable local cluster before destructive tests."""
from pathlib import Path

import psycopg

expected = (Path(__file__).resolve().parents[1] / ".local/verification-postgres").resolve()
with psycopg.connect("postgresql://nos_test@127.0.0.1:15432/postgres", connect_timeout=3) as connection:
    actual = Path(connection.execute("SHOW data_directory").fetchone()[0]).resolve()
    if actual != expected:
        raise RuntimeError("Unexpected PostgreSQL cluster; verification tests must not run")
    print("Verified project-local PostgreSQL cluster")
