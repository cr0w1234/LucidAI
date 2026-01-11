#!/usr/bin/env python3
from __future__ import annotations
import argparse, datetime, hashlib, os, pathlib, shlex, subprocess, sys
from typing import Sequence
from wp_site_etl.core.config import settings  


TABLES: Sequence[str] = (
    "prod.sections",
    "prod.section_chunks",
    "prod.section_excerpt_embedding_3072",
    "prod.section_chunks_embedding_3072",
)


def run(cmd: str, timeout_s: int | None = None) -> None:
    subprocess.run(shlex.split(cmd), check=True, timeout=timeout_s)


def sha256(path: pathlib.Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def make_dump(db_dsn: str, dump_path: pathlib.Path) -> pathlib.Path:
    # Build pg_dump command using local PostgreSQL 17
    table_flags = " ".join(f"-t {t}" for t in TABLES)
    cmd = (
        f'/opt/homebrew/opt/postgresql@17/bin/pg_dump "{db_dsn}" '
        f'-n prod {table_flags} '
        f'--format=custom --no-owner --no-privileges '
        f'--file "{dump_path}"'
    )
    run(cmd)
    return dump_path


def scp_upload(
    local_file: pathlib.Path,
    aws_ssh_key: str,
    aws_ssh_host: str,
    aws_remote_dir: str,
) -> str:
    # Handle home directory properly
    if aws_remote_dir == "~":
        remote = f'{aws_ssh_host}:~/'
    else:
        remote = f'{aws_ssh_host}:{aws_remote_dir}/'
    
    cmd = f'scp -i "{aws_ssh_key}" "{local_file}" {remote}'
    run(cmd)
    
    # Return the full remote path
    if aws_remote_dir == "~":
        return f'{aws_ssh_host}:~/{local_file.name}'
    else:
        return f'{aws_ssh_host}:{aws_remote_dir}/{local_file.name}'


# def verify_remote_hash(
#     local_file: pathlib.Path, 
#     aws_ssh_key: str, 
#     aws_ssh_host: str, 
#     aws_remote_dir: str,
# ) -> None:
#     local_h = sha256(local_file)
#     remote_path = f"{aws_remote_dir}/{local_file.name}"
#     cmd = f'ssh -i "{aws_ssh_key}" {aws_ssh_host} sha256sum "{remote_path}"'
#     cp = subprocess.run(shlex.split(cmd), check=True, capture_output=True, text=True)
#     remote_h = cp.stdout.split()[0]

#     if remote_h != local_h:
#         raise SystemExit(f"hash mismatch: local={local_h} remote={remote_h}")


def remote_restore(
    remote_dump_path: str, 
    aws_ssh_key: str, 
    aws_ssh_host: str, 
    aws_remote_db: str, 
    pg_restore_bin: str,
) -> None:
    # 1) schema
    schema_cmd = (
        f'sudo -u postgres {pg_restore_bin} '
        f'-d {aws_remote_db} --schema-only --clean --if-exists '
        f'--no-owner --no-privileges --role=postgres "{remote_dump_path}"'
    )
    run(f'ssh -i "{aws_ssh_key}" {aws_ssh_host} {shlex.quote(schema_cmd)}')

    # 3) Restore using PG16 pg_restore
    restore_cmd = (
        f'sudo -u postgres {pg_restore_bin} '
        f'-d {aws_remote_db} --jobs=4 --no-owner --no-privileges '
        f'--role=csha_app "{remote_dump_path}"'
    )
    run(f'ssh -i "{aws_ssh_key}" {aws_ssh_host} {shlex.quote(restore_cmd)}')


def main():
    #TODO: Add incremental mode
    # ap = argparse.ArgumentParser()
    # ap.add_argument("--mode", default="incremental", choices=[m.value for m in Mode])
    # ap.add_argument("--no-restore", action="store_true", help="upload only; skip remote pg_restore")
    # args = ap.parse_args(argv)
    # mode = Mode(args.mode)

    aws_ssh_key = settings.AWS_SSH_KEY
    aws_ssh_host = settings.AWS_SSH_HOST
    aws_remote_dir = settings.AWS_REMOTE_DIR
    aws_remote_db = settings.AWS_REMOTE_DB
    
    pg_restore_bin = settings.PG_RESTORE_BIN
    db_dsn = settings.DSN

    dump_path = settings.DUMP_PATH

    dump_file = make_dump(db_dsn, dump_path)
    uploaded = scp_upload(dump_file, aws_ssh_key, aws_ssh_host, aws_remote_dir)
    # verify_remote_hash(dump_file, aws_ssh_key, aws_ssh_host, aws_remote_dir)

    remote_path = f"{aws_remote_dir}/{dump_file.name}"
    remote_restore(remote_path, aws_ssh_key, aws_ssh_host, aws_remote_db, pg_restore_bin)

    print({"dump": str(dump_file), "uploaded": uploaded})

if __name__ == "__main__":
    sys.exit(main())
