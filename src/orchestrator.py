"""Step Functions style orchestrator.

Each pipeline is a YAML state machine with an ordered stage list.
Stages run in order: validate, transform, catalog, notify.
A stage that raises is retried per its retry count, then the source file
is quarantined and an alert is logged. This mirrors Step Functions
Retry and Catch blocks that route to a dead letter queue plus an SNS
alert on repeated failure.
"""
import sys
from datetime import datetime, timezone
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).parent))

from catalog import register_table
from transforms import (
    count_curated_rows,
    mirror_to_sqlite,
    read_records,
    scd1_merge,
    transform_clickstream,
    transform_product_dim,
    validate_records,
    write_partitioned_csv,
)

ROOT = Path(__file__).resolve().parent.parent
ALERTS_LOG = ROOT / "alerts.log"
QUARANTINE = ROOT / "quarantine"


def log_alert(message):
    ts = datetime.now(timezone.utc).isoformat()
    with open(ALERTS_LOG, "a") as fh:
        fh.write(f"{ts} {message}\n")


def stage_validate(ctx):
    cfg = ctx["config"]
    records = read_records(cfg["source"], ctx["source_path"])
    problems = validate_records(records, cfg["source"]["required_fields"])
    if problems:
        raise ValueError(
            f"{len(problems)} validation problems; first: {problems[0]}")
    if not records:
        raise ValueError("no records found in source file")
    ctx["records"] = records
    return len(records)


def stage_transform(ctx):
    cfg = ctx["config"]
    curated = cfg["curated"]
    columns = [c["name"] for c in curated["columns"]]
    kind = cfg["transform"]["kind"]
    if kind == "partitioned_events":
        rows = transform_clickstream(ctx["records"], cfg["transform"])
        npart = write_partitioned_csv(
            rows, curated["base_path"], curated["partition_by"], columns)
        log_detail = f"{npart} partitions"
    elif kind == "scd1_dimension":
        rows = transform_product_dim(ctx["records"], curated)
        total = scd1_merge(rows, curated["base_path"], columns,
                           curated["merge_key"])
        log_detail = f"{total} dimension rows after merge"
    else:
        raise ValueError(f"unknown transform kind {kind}")
    ctx["rows"] = rows
    return f"{len(rows)} rows in, {log_detail}"


def stage_catalog(ctx):
    cfg = ctx["config"]
    curated = cfg["curated"]
    columns = curated["columns"]
    register_table(
        curated["table"],
        columns,
        curated.get("partition_by", []),
        curated["base_path"],
        curated["format"],
        count_curated_rows(curated["base_path"]),
        merge_key=curated.get("merge_key"),
    )
    mirror_to_sqlite(curated["table"], columns, ctx["rows"],
                     curated.get("merge_key"))
    return curated["table"]


def stage_notify(ctx):
    cfg = ctx["config"]
    msg = (f"INFO pipeline={cfg['pipeline']} "
           f"file={Path(ctx['source_path']).name} "
           f"records={len(ctx['rows'])} "
           f"table={cfg['curated']['table']} status=complete")
    log_alert(msg)
    return "notified"


STAGES = {
    "validate": stage_validate,
    "transform": stage_transform,
    "catalog": stage_catalog,
    "notify": stage_notify,
}


def run_stage(stage_name, fn, retries, ctx):
    attempts = retries + 1
    last_error = None
    for attempt in range(1, attempts + 1):
        try:
            return fn(ctx)
        except Exception as exc:
            last_error = exc
            log_alert(f"WARN pipeline={ctx['pipeline']} stage={stage_name} "
                      f"attempt={attempt} error={exc}")
    raise RuntimeError(
        f"stage {stage_name} failed after {attempts} attempts: {last_error}")


def run_pipeline(config_path, source_path):
    """Run one file through one pipeline state machine.

    Returns True on success. On repeated stage failure the source file is
    moved to quarantine and an alert is logged, then returns False.
    """
    cfg = yaml.safe_load(Path(config_path).read_text())
    ctx = {"pipeline": cfg["pipeline"], "config": cfg,
           "source_path": str(source_path)}
    try:
        for stage in cfg["stages"]:
            result = run_stage(stage["name"], STAGES[stage["name"]],
                               stage.get("retries", 0), ctx)
            print(f"  [{stage['name']}] {result}")
    except Exception as exc:
        qdir = QUARANTINE / cfg["pipeline"]
        qdir.mkdir(parents=True, exist_ok=True)
        dest = qdir / Path(source_path).name
        Path(source_path).replace(dest)
        log_alert(f"ALERT pipeline={cfg['pipeline']} "
                  f"file={Path(source_path).name} "
                  f"quarantined after retries error={exc}")
        print(f"  FAILED: {exc} -> quarantined")
        return False
    return True


if __name__ == "__main__":
    import sys as _sys
    if len(_sys.argv) != 3:
        print("usage: python src/orchestrator.py <config.yaml> <source file>")
        raise SystemExit(2)
    ok = run_pipeline(_sys.argv[1], _sys.argv[2])
    raise SystemExit(0 if ok else 1)
