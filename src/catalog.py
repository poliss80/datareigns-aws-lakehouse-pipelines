"""Glue style data catalog backed by a local JSON file.

In production this maps to the AWS Glue Data Catalog. Each table entry
records columns, types, partition keys, location, and row counts, the
same metadata a crawler or an explicit table definition would hold.
"""
import json
from pathlib import Path

CATALOG_PATH = Path(__file__).resolve().parent.parent / "catalog.json"


def load_catalog():
    if CATALOG_PATH.exists():
        return json.loads(CATALOG_PATH.read_text())
    return {"tables": {}}


def save_catalog(catalog):
    CATALOG_PATH.write_text(json.dumps(catalog, indent=2))


def register_table(table, columns, partition_keys, location, fmt,
                   record_count, merge_key=None):
    catalog = load_catalog()
    entry = {
        "columns": columns,
        "partition_keys": partition_keys,
        "location": location,
        "format": fmt,
        "record_count": record_count,
    }
    if merge_key:
        entry["merge_key"] = merge_key
        entry["scd_type"] = 1
    catalog["tables"][table] = entry
    save_catalog(catalog)
    return entry
