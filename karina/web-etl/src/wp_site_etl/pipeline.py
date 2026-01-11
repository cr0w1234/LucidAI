# pipeline.py
import sys, subprocess, shlex, time, os
from config import settings 

EXTRACT = shlex.split(os.getenv("EXTRACT_CMD", "bash extract/fetch_endpoint_content.sh"))
TRANSFORM = shlex.split(os.getenv("TRANSFORM_CMD", "python -m transform.wp_content_indexer"))
#Run vectorizer. Figure out how to do it elegantly with transform stage. 
LOAD = shlex.split(os.getenv("LOAD_CMD", "python -m load")) #TODO: Add load stage.

TO_EXTRACT   = int(os.getenv("EXTRACT_TIMEOUT_S", "900"))
TO_TRANSFORM = int(os.getenv("TRANSFORM_TIMEOUT_S","900"))
TO_LOAD      = int(os.getenv("LOAD_TIMEOUT_S","600"))
RETRIES      = int(os.getenv("ETL_MAX_RETRIES","2"))

def run(cmd, timeout_s, retries):
    attempt = 0
    while True:
        attempt += 1
        try:
            subprocess.run(cmd, timeout=timeout_s, check=True)
            return
        except (subprocess.TimeoutExpired, subprocess.CalledProcessError):
            if attempt > retries:
                raise
            time.sleep(2 ** attempt + 0.1 * attempt)

def main():
    run(EXTRACT, timeout_s=TO_EXTRACT, retries=RETRIES)
    run(TRANSFORM, timeout_s=TO_TRANSFORM, retries=RETRIES)
    run(LOAD, timeout_s=TO_LOAD, retries=RETRIES)

    #TODO: Add a incremental mode to only update and add new data to the database and add a full mode to re-run the entire pipeline. This mode can become an enum class.
    # mode = resolve_mode(sys.argv[1] if len(sys.argv) > 1 else None)
    # if mode != SETTINGS.mode:
        # raise SystemExit(f"Mode drift: {mode=} != {SETTINGS.mode=}")
    # run(EXTRACT, timeout_s=TO_EXTRACT, retries=RETRIES)
    # run(TRANSFORM + ["--mode", mode], timeout_s=TO_TRANSFORM, retries=RETRIES)
    # run(LOAD + ["--mode", mode], timeout_s=TO_LOAD, retries=RETRIES)

if __name__ == "__main__":
    sys.exit(main())
