# CMS Hospitals Dataset Sync

## Overview

`cms_hospitals_sync.py` synchronizes CMS Provider Data datasets where `theme` includes `Hospitals`.

For every CSV distribution in changed datasets, the script:
- downloads the file,
- normalizes header names to `snake_case`,
- writes transformed output to the configured output directory,
- records per-file and per-run metadata in a JSON state file.

The sync is incremental: it compares each dataset's CMS `modified` timestamp to the stored state and only processes datasets whose timestamp changed.

## Current behavior (as implemented)

### Data source and filtering

- Default metastore endpoint:
	- `https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items`
- Dataset filter:
	- Includes items where `theme` contains `Hospitals` (case-insensitive, supports string or list values).

### Incremental logic

- State is loaded from `state/sync_state.json` (or `--state-file` override).
- For each hospitals dataset, the script compares:
	- current `item.modified` from CMS
	- previously stored `datasets[dataset_id].modified`
- If unchanged, dataset distributions are skipped.
- If changed, CSV distributions are queued for processing.

### Parallel processing

- CSV downloads and transforms run in parallel using `ThreadPoolExecutor`.
- Worker count is configurable with `--workers` (default: `8`).

### Header normalization

Headers are transformed by:
- Unicode normalization (`NFKD`) and ASCII folding,
- removal of quotes/apostrophes,
- replacement of non-alphanumeric sequences with `_`,
- collapsing repeated underscores,
- lowercasing.

Duplicate normalized names are made unique with numeric suffixes:
- example: `score`, `score` -> `score`, `score_2`.

### Output naming

- Output filename format is based on distribution file stem + dataset modified date tag.
- Output path defaults under `output/`.
- The script also includes a legacy filename migration step that attempts to rename prior outputs and update state references.

### Dry-run mode

- `--dry-run` discovers and reports pending work without downloading files or writing CSV output.
- Run metadata is still recorded (including `dry_run: true` and targeted file counts).

### Failure handling and summary

- Individual file failures are captured and reported; successful files still complete.
- Exit code:
	- `0` when no file failures occurred
	- `1` when one or more file failures occurred
- Console summary includes discovered datasets, targeted/processed files, run times, failures, output directory, and state file location.

### Runtime progress logging

The script now emits timestamped progress logs during execution so long runs are observable in real time.

You will see messages for:
- state load and run start,
- metastore fetch start/completion,
- discovered hospitals dataset count and queued file count,
- per-file worker activity (`START`, `DOWNLOADED`, `DONE`),
- aggregate progress updates as futures complete (`PROGRESS x/y`),
- dry-run target previews,
- state save completion.

This logging is printed to standard output and is enabled by default.

## CLI options

- `--output-dir` (default: `output`)
- `--state-file` (default: `state/sync_state.json`)
- `--workers` (default: `8`)
- `--metastore-url` (override CMS endpoint)
- `--dry-run` (discovery only, no download/write)

## State file structure

State JSON contains:
- `last_run_started_utc`
- `last_run_completed_utc`
- `datasets` (keyed by CMS dataset identifier)
	- `modified`
	- `last_processed_utc`
	- `files` list:
		- `download_url`
		- `output_file`
		- `rows_written`
- `history` (last 50 runs retained)
	- `started_utc`, `completed_utc`
	- `hospitals_datasets_seen`
	- `files_targeted`, `files_processed`
	- `dry_run`
	- `failed` (list of failure messages)

## Requirement mapping

- Download all data sets related to theme `Hospitals`
	- Done via metastore theme filter.
- Convert mixed/special-case headers to `snake_case`
	- Done via normalization + regex cleanup.
- Download/process in parallel
	- Done via thread pool workers.
- Designed for daily execution
	- Script is idempotent/incremental and intended for scheduler-driven runs.
- Only process modified datasets since previous run
	- Done via persisted `modified` comparison.
- Track run metadata and change history
	- Done via state timestamps, dataset file records, and bounded run history.
- Python + standard environment support
	- Uses stdlib plus `requests`.

## Files

- `cms_hospitals_sync.py` - main sync program
- `requirements.txt` - dependency list (`requests`)
- `state/sync_state.json` - persisted incremental state and run history
- `output/` - normalized CSV outputs

## Run instructions

1. Install dependencies: `pip install -r requirements.txt`
2. Run sync: `python cms_hospitals_sync.py`
3. Optional discovery mode: `python cms_hospitals_sync.py --dry-run`
4. Schedule daily execution externally (for example, cron or Task Scheduler).

