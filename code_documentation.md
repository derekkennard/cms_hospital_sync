# CMS Hospitals Dataset Sync

## What this script does

`cms_hospitals_sync.py` downloads and processes all CMS Provider Data catalog datasets where `theme == "Hospitals"`.

For each CSV distribution in those datasets, it:
- Downloads the file
- Renames column headers to `snake_case`
- Writes normalized CSV output to the configured output directory

It also stores run metadata in a JSON state file so future runs only process datasets whose `modified` value changed since the prior successful processing.

## Requirement mapping

- Download all data sets related to theme "Hospitals"
	- Filter uses `theme` field from CMS metastore items.
- Convert mixed/special-case column headers to snake_case
	- Uses Unicode normalization and regex cleanup.
- Download/process in parallel
	- Uses `ThreadPoolExecutor` worker pool.
- Designed to run every day
	- Run this script once per day using OS scheduler (cron on Linux, Task Scheduler on Windows).
- Only download modified-since-previous-run files
	- Compares each dataset's `modified` value against persisted state.
- Track run metadata
	- Persists timestamps, file outputs, failures, and short run history.
- Python on standard Windows/Linux/macOS
	- Uses Python standard library plus `requests` for HTTPS downloads.

## Files

- `cms_hospitals_sync.py` – main sync script
- `state/sync_state.json` – created at runtime, contains incremental sync metadata
- `output/` – normalized CSV outputs

## Usage

One-time run against live CMS API:

`pip install -r requirements.txt`

`python cms_hospitals_sync.py`

Dry run (discover what would be processed, no downloads):

`python cms_hospitals_sync.py --dry-run`

Schedule daily runs externally (recommended for production):

- Linux: cron
- Windows: Task Scheduler

