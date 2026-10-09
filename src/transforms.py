"""Record level transforms shared by the pipeline stages.

Covers reading raw records, validation, the two transform kinds used by
the sample pipelines, partitioned CSV writes with _SUCCESS markers, and
the SQLite mirror that stands in for Athena during local runs.
"""
import csv
import json
import sqlite3
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SQLITE_PATH = ROOT / "curated.db"

TYPE_MAP = {
    "string": "TEXT",
    "timestamp": "TEXT",
    "date": "TEXT",
    "decimal": "REAL",
    "integer": "INTEGER",
    "boolean": "INTEGER",
}


def read_records(source_cfg, path):
    fmt = source_cfg["format"]
    if fmt == "json_lines":
        with open(path) as fh:
            return [json.loads(line) for line in fh if line.strip()]
    if fmt == "csv":
        with open(path, newline="") as fh:
            return list(csv.DictReader(fh))
    raise ValueError(f"unknown source format {fmt}")


def validate_records(records, required_fields):
    problems = []
    for i, rec in enumerate(records):
        for field in required_fields:
            if field not in rec or rec[field] in (None, ""):
                problems.append(f"row {i}: missing {field}")
    return problems


def transform_clickstream(records, transform_cfg):
    date_field = transform_cfg.get("derive_event_date_from", "event_time")
    rows = []
    for rec in records:
        rows.append({
            "event_id": rec["event_id"],
            "event_time": rec["event_time"],
            "event_date": rec[date_field][:10],
            "user_id": rec["user_id"],
            "event_type": rec["event_type"],
            "region": rec["region"],
            "page_url": rec.get("page_url", ""),
            "session_id": rec.get("session_id", ""),
        })
    return rows


def transform_product_dim(records, curated_cfg):
    columns = [c["name"] for c in curated_cfg["columns"]]
    return [{c: rec.get(c, "") for c in columns} for rec in records]


def _next_part_name(part_dir):
    idx = 0
    for p in part_dir.glob("part-*.csv"):
        try:
            idx = max(idx, int(p.stem.split("-")[1]) + 1)
        except (IndexError, ValueError):
            pass
    return f"part-{idx:05d}.csv"


def write_partitioned_csv(rows, base_path, partition_by, columns):
    base = Path(base_path)
    partitions = {}
    for row in rows:
        key = tuple(row[p] for p in partition_by)
        partitions.setdefault(key, []).append(row)
    for key, part_rows in partitions.items():
        part_dir = base.joinpath(
            *[f"{p}={v}" for p, v in zip(partition_by, key)])
        part_dir.mkdir(parents=True, exist_ok=True)
        with open(part_dir / _next_part_name(part_dir), "w", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=columns)
            writer.writeheader()
            writer.writerows(part_rows)
        (part_dir / "_SUCCESS").write_text("ok")
    (base / "_SUCCESS").write_text("ok")
    return len(partitions)


def scd1_merge(rows, base_path, columns, merge_key):
    """Slowly changing dimension type 1: incoming rows overwrite existing
    rows on the merge key, so the table always reflects the latest state."""
    base = Path(base_path)
    base.mkdir(parents=True, exist_ok=True)
    data_file = base / "part-00000.csv"
    merged = {}
    if data_file.exists():
        with open(data_file, newline="") as fh:
            for rec in csv.DictReader(fh):
                merged[rec[merge_key]] = rec
    for row in rows:
        merged[row[merge_key]] = {c: row.get(c, "") for c in columns}
    ordered = [merged[k] for k in sorted(merged)]
    with open(data_file, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=columns)
        writer.writeheader()
        writer.writerows(ordered)
    (base / "_SUCCESS").write_text("ok")
    return len(ordered)


def count_curated_rows(base_path):
    """Count data rows across all part files under a curated table path,
    so the catalog always reflects what is actually on disk."""
    total = 0
    for part in Path(base_path).rglob("part-*.csv"):
        with open(part, newline="") as fh:
            total += sum(1 for _ in fh) - 1
    return max(total, 0)


def mirror_to_sqlite(table, columns, rows, merge_key=None):
    """Write curated rows into the local SQLite mirror.

    Table and column names match catalog.json exactly, so every query in
    athena_queries.sql runs as is here and in Athena.
    """
    con = sqlite3.connect(SQLITE_PATH)
    try:
        coldefs = ", ".join(
            f"{c['name']} {TYPE_MAP[c['type']]}" for c in columns)
        if merge_key:
            coldefs += f", PRIMARY KEY ({merge_key})"
        con.execute(f"CREATE TABLE IF NOT EXISTS {table} ({coldefs})")
        names = [c["name"] for c in columns]
        placeholders = ", ".join("?" for _ in names)
        if merge_key:
            sql = (f"INSERT OR REPLACE INTO {table} "
                   f"({', '.join(names)}) VALUES ({placeholders})")
        else:
            sql = (f"INSERT INTO {table} "
                   f"({', '.join(names)}) VALUES ({placeholders})")
        con.executemany(sql, [[r.get(n) for n in names] for r in rows])
        con.commit()
    finally:
        con.close()
