"""Simulates S3 event notifications delivered to Lambda.

Polls the landing bucket for new files, matches each file to a pipeline
by prefix and file pattern, then hands it to the orchestrator. Processed
files move to _processed so each object fires exactly once. Files that
fail every retry are quarantined by the orchestrator with an alert.
"""
import shutil
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(Path(__file__).parent))

from orchestrator import run_pipeline


def main():
    configs = sorted((ROOT / "configs").glob("*.yaml"))
    if not configs:
        print("no pipeline configs found")
        return
    for cfg_path in configs:
        cfg = yaml.safe_load(cfg_path.read_text())
        trig = cfg["trigger"]
        scan_dir = ROOT / trig["landing_dir"] / trig["prefix"]
        files = sorted(scan_dir.glob(trig["file_pattern"]))
        files = [p for p in files if p.is_file()]
        if not files:
            continue
        for path in files:
            print(f"event: {path.name} -> pipeline {cfg['pipeline']}")
            ok = run_pipeline(cfg_path, path)
            if ok:
                dest_dir = (ROOT / trig["landing_dir"] / "_processed"
                            / cfg["pipeline"])
                dest_dir.mkdir(parents=True, exist_ok=True)
                shutil.move(str(path), dest_dir / path.name)
                print(f"  archived to _processed/{cfg['pipeline']}/")
            # on failure the orchestrator already quarantined the file


if __name__ == "__main__":
    main()
