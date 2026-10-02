#!/usr/bin/env python3
"""Download and normalize CMS Provider Data datasets for the "Hospitals" theme.

Features:
- Pulls CMS metastore dataset items.
- Filters datasets where theme includes "Hospitals".
- Downloads and processes CSV files in parallel.
- Converts CSV headers to snake_case.
- Tracks run metadata to support incremental updates (modified-since-last-run behavior).
"""

from __future__ import annotations

import argparse
import concurrent.futures
import csv
import datetime as dt
import json
import re
import unicodedata
from urllib.parse import urlparse
from dataclasses import dataclass
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any

import requests


METASTORE_URL = "https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items"
USER_AGENT = "cms-hospitals-sync/1.0"


@dataclass
class DistributionTask:
    dataset_id: str
    dataset_title: str
    dataset_modified: str
    download_url: str
    distribution_name: str


class SyncError(Exception):
    """Raised for recoverable sync errors."""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync CMS Hospitals datasets")
    parser.add_argument(
        "--output-dir",
        default="output",
        help="Directory where normalized CSV files are written (default: output)",
    )
    parser.add_argument(
        "--state-file",
        default="state/sync_state.json",
        help="Path to state metadata JSON file (default: state/sync_state.json)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=8,
        help="Number of parallel workers for download/process tasks (default: 8)",
    )
    parser.add_argument(
        "--metastore-url",
        default=METASTORE_URL,
        help="Override CMS metastore API URL",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Discover and report pending files without downloading or writing output CSVs",
    )
    return parser.parse_args()


def http_get_json(url: str) -> Any:
    try:
        response = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=60)
        response.raise_for_status()
        payload = response.text
    except requests.RequestException as exc:
        raise SyncError(f"Failed to GET JSON from {url}: {exc}") from exc

    try:
        return json.loads(payload)
    except json.JSONDecodeError as exc:
        raise SyncError(f"Invalid JSON from {url}: {exc}") from exc


def load_metastore_items(metastore_url: str) -> list[dict[str, Any]]:
    data = http_get_json(metastore_url)
    if not isinstance(data, list):
        raise SyncError("Metastore payload is not a list")
    return data


def load_state(state_file: Path) -> dict[str, Any]:
    default_state = {
        "last_run_started_utc": None,
        "last_run_completed_utc": None,
        "datasets": {},
        "history": [],
    }

    if not state_file.exists():
        return default_state

    contents = state_file.read_text(encoding="utf-8")
    if not contents.strip():
        return default_state

    try:
        return json.loads(contents)
    except json.JSONDecodeError as exc:
        raise SyncError(f"Invalid state JSON in {state_file}: {exc}") from exc


def save_state(state_file: Path, state: dict[str, Any]) -> None:
    state_file.parent.mkdir(parents=True, exist_ok=True)
    state_file.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")


def is_hospitals_dataset(item: dict[str, Any]) -> bool:
    theme = item.get("theme", [])
    if isinstance(theme, str):
        theme = [theme]
    return any(str(entry).strip().lower() == "hospitals" for entry in theme)


def build_distribution_tasks(
    items: list[dict[str, Any]],
    previous_datasets_state: dict[str, Any],
) -> tuple[list[DistributionTask], int]:
    hospitals = [item for item in items if is_hospitals_dataset(item)]
    tasks: list[DistributionTask] = []

    for item in hospitals:
        dataset_id = str(item.get("identifier", "")).strip()
        if not dataset_id:
            continue

        dataset_modified = str(item.get("modified", "")).strip()
        dataset_title = str(item.get("title", dataset_id)).strip()

        prev = previous_datasets_state.get(dataset_id, {})
        prev_modified = str(prev.get("modified", "")).strip()
        needs_update = dataset_modified != prev_modified

        if not needs_update:
            continue

        raw_distribution = item.get("distribution", [])
        distributions = raw_distribution if isinstance(raw_distribution, list) else [raw_distribution]

        for dist in distributions:
            if not isinstance(dist, dict):
                continue
            media_type = str(dist.get("mediaType", "")).lower()
            if "csv" not in media_type:
                continue
            download_url = str(dist.get("downloadURL", "")).strip()
            if not download_url:
                continue

            parsed = urlparse(download_url)
            distribution_name = Path(parsed.path).name or "distribution"
            tasks.append(
                DistributionTask(
                    dataset_id=dataset_id,
                    dataset_title=dataset_title,
                    dataset_modified=dataset_modified,
                    download_url=download_url,
                    distribution_name=distribution_name,
                )
            )

    return tasks, len(hospitals)


def snake_case_column(name: str) -> str:
    text = unicodedata.normalize("NFKD", name)
    text = text.encode("ascii", "ignore").decode("ascii")
    text = text.replace("'", "")
    text = text.replace("\"", "")
    text = re.sub(r"[^a-zA-Z0-9]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    text = text.lower()
    return text or "column"


def make_unique_headers(headers: list[str]) -> list[str]:
    counts: dict[str, int] = {}
    unique: list[str] = []

    for header in headers:
        count = counts.get(header, 0)
        if count == 0:
            unique_name = header
        else:
            unique_name = f"{header}_{count + 1}"
        counts[header] = count + 1
        unique.append(unique_name)

    return unique


def safe_name(value: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9._-]+", "_", value.strip())
    cleaned = re.sub(r"_+", "_", cleaned).strip("_")
    return cleaned or "unknown"


def output_file_path(output_dir: Path, task: DistributionTask) -> Path:
    date_tag = task.dataset_modified.replace("-", "") if task.dataset_modified else "unknown"
    distribution_stem = safe_name(Path(task.distribution_name).stem)
    filename = f"{distribution_stem}__{date_tag}.csv"
    return output_dir / filename


def migrate_legacy_output_filenames(state: dict[str, Any], output_dir: Path) -> bool:
    changed = False
    datasets = state.get("datasets", {})

    for dataset_id, dataset_state in datasets.items():
        modified = str(dataset_state.get("modified", "")).strip()
        date_tag = modified.replace("-", "") if modified else "unknown"

        files = dataset_state.get("files", [])
        for file_entry in files:
            old_output = str(file_entry.get("output_file", "")).strip()
            download_url = str(file_entry.get("download_url", "")).strip()
            if not old_output or not download_url:
                continue

            distribution_name = Path(urlparse(download_url).path).name or "distribution.csv"
            distribution_stem = safe_name(Path(distribution_name).stem)
            new_relative = str(Path("output") / f"{distribution_stem}_{date_tag}.csv")

            if old_output == new_relative:
                continue

            old_path = Path(old_output)
            if not old_path.is_absolute():
                old_path = output_dir.parent / old_path

            new_path = output_dir / f"{distribution_stem}__{date_tag}.csv"

            if old_path.exists() and old_path != new_path and not new_path.exists():
                new_path.parent.mkdir(parents=True, exist_ok=True)
                old_path.replace(new_path)

            if new_path.exists() or not old_path.exists():
                file_entry["output_file"] = new_relative
                changed = True

    return changed


def download_and_normalize_csv(task: DistributionTask, output_dir: Path) -> dict[str, Any]:
    try:
        response = requests.get(task.download_url, headers={"User-Agent": USER_AGENT}, timeout=120)
        response.raise_for_status()
        payload = response.content
    except requests.RequestException as exc:
        raise SyncError(f"Download failed for {task.dataset_id} ({task.download_url}): {exc}") from exc

    output_path = output_file_path(output_dir, task)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    rows_written = 0

    with NamedTemporaryFile("w", encoding="utf-8", newline="", delete=False) as temp_out:
        temp_path = Path(temp_out.name)
        text = payload.decode("utf-8-sig", errors="replace")
        reader = csv.DictReader(text.splitlines())
        if reader.fieldnames is None:
            raise SyncError(f"CSV has no header row: {task.download_url}")

        normalized_headers = make_unique_headers([snake_case_column(col) for col in reader.fieldnames])

        writer = csv.DictWriter(temp_out, fieldnames=normalized_headers)
        writer.writeheader()

        for row in reader:
            normalized_row = {
                normalized_headers[idx]: row.get(original_header, "")
                for idx, original_header in enumerate(reader.fieldnames)
            }
            writer.writerow(normalized_row)
            rows_written += 1

    temp_path.replace(output_path)

    return {
        "dataset_id": task.dataset_id,
        "dataset_title": task.dataset_title,
        "dataset_modified": task.dataset_modified,
        "download_url": task.download_url,
        "output_file": str(output_path),
        "rows_written": rows_written,
    }


def update_dataset_state(
    previous: dict[str, Any],
    results: list[dict[str, Any]],
) -> dict[str, Any]:
    updated = dict(previous)

    grouped: dict[str, list[dict[str, Any]]] = {}
    for result in results:
        grouped.setdefault(result["dataset_id"], []).append(result)

    now_utc = dt.datetime.now(dt.timezone.utc).isoformat()
    for dataset_id, entries in grouped.items():
        modified = entries[0]["dataset_modified"]
        updated[dataset_id] = {
            "modified": modified,
            "last_processed_utc": now_utc,
            "files": [
                {
                    "download_url": entry["download_url"],
                    "output_file": entry["output_file"],
                    "rows_written": entry["rows_written"],
                }
                for entry in entries
            ],
        }

    return updated


def run_sync_once(args: argparse.Namespace) -> int:
    output_dir = Path(args.output_dir)
    state_file = Path(args.state_file)

    state = load_state(state_file)
    if migrate_legacy_output_filenames(state, output_dir):
        save_state(state_file, state)

    run_start = dt.datetime.now(dt.timezone.utc).isoformat()
    state["last_run_started_utc"] = run_start

    items = load_metastore_items(args.metastore_url)

    previous_datasets_state = state.get("datasets", {})
    tasks, hospitals_count = build_distribution_tasks(items, previous_datasets_state)

    results: list[dict[str, Any]] = []
    failures: list[str] = []

    if tasks and not args.dry_run:
        with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, args.workers)) as executor:
            future_map = {
                executor.submit(download_and_normalize_csv, task, output_dir): task
                for task in tasks
            }
            for future in concurrent.futures.as_completed(future_map):
                task = future_map[future]
                try:
                    results.append(future.result())
                except Exception as exc:  # noqa: BLE001 - keep sync robust
                    failures.append(f"{task.dataset_id}: {exc}")

    run_end = dt.datetime.now(dt.timezone.utc).isoformat()
    state["last_run_completed_utc"] = run_end

    if results:
        state["datasets"] = update_dataset_state(previous_datasets_state, results)

    history_entry = {
        "started_utc": run_start,
        "completed_utc": run_end,
        "hospitals_datasets_seen": hospitals_count,
        "files_targeted": len(tasks),
        "files_processed": len(results),
        "dry_run": bool(args.dry_run),
        "failed": failures,
    }
    state.setdefault("history", []).append(history_entry)
    state["history"] = state["history"][-50:]

    save_state(state_file, state)

    print("=" * 80)
    print("CMS Hospitals sync complete")
    print(f"Hospitals datasets discovered : {hospitals_count}")
    print(f"CSV files needing refresh    : {len(tasks)}")
    print(f"CSV files processed          : {len(results)}")
    print(f"Dry run                      : {args.dry_run}")
    print(f"Failures                     : {len(failures)}")
    print(f"Output directory             : {output_dir.resolve()}")
    print(f"State file                   : {state_file.resolve()}")
    print(f"Run started                  : {run_start}")
    print(f"Run completed                : {run_end}")
    print("=" * 80)
    if failures:
        print("-- Failures --")
        for failure in failures:
            print(f"  - {failure}")

    return 1 if failures else 0


def main() -> int:
    args = parse_args()
    return run_sync_once(args)


if __name__ == "__main__":
    raise SystemExit(main())
