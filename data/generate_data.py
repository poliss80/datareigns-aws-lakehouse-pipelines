#!/usr/bin/env python3
"""Create realistic raw sample files in the landing bucket.

Writes JSON lines clickstream event files and product catalog CSV files,
including one product update file that overlaps on product id so the SCD1
merge has something to do. Fixed seed, so output is reproducible.
"""
import csv
import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

random.seed(20261008)
ROOT = Path(__file__).resolve().parent.parent
LANDING = ROOT / "s3_landing"

REGIONS = ["West", "East", "Midwest", "South"]
PAGES = ["/home", "/products", "/pricing", "/checkout", "/search", "/deals"]
EVENT_TYPES = ["page_view", "page_view", "page_view", "search",
               "add_to_cart", "add_to_cart", "purchase"]

CATEGORIES = {
    "Electronics": ["Aurora Laptop 14", "Nimbus Headphones", "Pulse Smartwatch",
                    "Drift Keyboard", "Volt Charger 65W"],
    "Outdoors": ["Trailblaze Backpack", "Everpour Water Bottle",
                 "Summit Tent 2P", "Ember Camp Stove"],
    "Home": ["Lumen Desk Lamp", "Cloudrest Pillow", "Terra Plant Pot Set",
             "Halcyon Throw Blanket"],
    "Apparel": ["Velocity Running Shoes", "Atlas Denim Jacket",
                "Meridian Rain Shell"],
}


def gen_clickstream(path, n_events, start):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as fh:
        for i in range(n_events):
            ts = start + timedelta(seconds=random.randint(0, 86399))
            fh.write(json.dumps({
                "event_id": f"evt-{start:%Y%m%d}-{i:05d}",
                "event_time": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "user_id": f"u{random.randint(1000, 9999)}",
                "event_type": random.choice(EVENT_TYPES),
                "region": random.choice(REGIONS),
                "page_url": random.choice(PAGES),
                "session_id": f"s{random.randint(10000, 99999)}",
            }) + "\n")
    print(f"wrote {n_events} events -> {path}")


def product_rows():
    rows = []
    pid = 1001
    for category, names in CATEGORIES.items():
        for name in names:
            rows.append({
                "product_id": f"P{pid}",
                "product_name": name,
                "category": category,
                "price": round(random.uniform(29, 899), 2),
                "updated_at": "2026-10-01T09:00:00Z",
            })
            pid += 1
    return rows


def gen_products(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="") as fh:
        writer = csv.DictWriter(
            fh, fieldnames=["product_id", "product_name", "category",
                            "price", "updated_at"])
        writer.writeheader()
        writer.writerows(rows)
    print(f"wrote {len(rows)} products -> {path}")


def main():
    base = datetime(2026, 10, 8, tzinfo=timezone.utc)
    gen_clickstream(LANDING / "raw/clickstream/clickstream_20261008_a.json",
                    420, base)
    gen_clickstream(LANDING / "raw/clickstream/clickstream_20261008_b.json",
                    380, base + timedelta(hours=6))

    rows = product_rows()
    gen_products(LANDING / "raw/products/products_20261008.csv", rows)

    updates = []
    for r in random.sample(rows, 10):
        r = dict(r)
        r["price"] = round(float(r["price"]) * random.uniform(0.85, 1.15), 2)
        r["updated_at"] = "2026-10-08T09:00:00Z"
        updates.append(r)
    new_names = [("Aero Drone Mini", "Electronics"),
                 ("Harbor Hammock", "Outdoors"),
                 ("Solstice Candle Trio", "Home"),
                 ("Court Classic Sneakers", "Apparel")]
    for i, (name, category) in enumerate(new_names):
        updates.append({
            "product_id": f"P{3001 + i}",
            "product_name": name,
            "category": category,
            "price": round(random.uniform(39, 249), 2),
            "updated_at": "2026-10-08T09:00:00Z",
        })
    gen_products(LANDING / "raw/products/products_20261008_update.csv",
                 updates)


if __name__ == "__main__":
    main()
