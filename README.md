# Backblaze Hard Drive Reliability Pipeline

A schema-aware, incremental data pipeline that ingests ~2 years of Backblaze's
daily hard-drive SMART stats, cleans and normalizes them under a fixed memory
budget, and produces drive-reliability analytics (AFR by model, manufacturer
rollups, monthly failure trends, and data-completeness metrics).

## Dataset

- **Source:** Backblaze Hard Drive Stats (`https://www.backblaze.com/cloud-storage/resources/hard-drive-test-data`)
- **Window used:** 2024-01-01 to 2025-12-31 (2 full, consecutive calendar
  years) — chosen as the most recent complete 2-year window available at the
  time of this run, rather than the earliest available window, since the
  assessment did not specify a fixed range and recent data is more relevant
  to current drive reliability.
- **Volume:** 731 daily CSV files, ~89 GB raw, ~375,000 distinct drives
  tracked, 93 distinct drive models.

## Architecture

data/raw/ <- raw daily CSVs (downloaded, never committed)
|
v
src/ingest.py <- bronze layer: schema normalization, dtype
| consistency, date-partitioned Parquet
v
data/processed/date=YYYY-MM-DD/data.parquet
|
v
src/transform.py <- silver layer: data-quality policies applied
| (missing SMART flags, drive registry,
| capacity normalization)
v
data/clean/date=YYYY-MM-DD/data.parquet
|
v
src/aggregate.py <- incremental per-model, per-month summary
| tables (drive-days, failures, missing-SMART
| counts) -- the only structure Part 2 reads
v
data/model_monthly_summary.json
|
v
src/analytics.py <- Part 2 outputs, computed entirely from the
small summary table above (no raw data or
Parquet touched)




Each stage (`ingest`, `transform`, `aggregate`) has its own manifest file
(`data/processed_manifest.json`, `data/clean_manifest.json`,
`data/aggregate_manifest.json`) recording which dates have already been
processed, so **re-running any stage only processes new dates** — verified
directly (see "Incremental loading" below).

## Design decisions & trade-offs

**Why Parquet, not CSV, for intermediate storage.** Columnar, compressed,
and carries an embedded schema — required for the schema-registry approach
below, and much faster for the column-subset reads `aggregate.py` performs.

**Chunking at the file level, not the sub-file level.** The 2 GB memory
budget is about not loading the *entire multi-year dataset* into memory at
once, not about a single day's file. Daily files here average ~125 MB
(89 GB / 731 files) — trivially small against a 2 GB budget. Reading one
full day at a time, rather than chunking within it, means pandas infers
each column's dtype exactly once per file. This matters in practice: an
earlier version of this pipeline chunked *within* each file
(`chunksize=100_000`), and hit real schema-mismatch failures because pandas
promotes a column from `int64` to `float64` the moment a chunk contains a
null — so two chunks of the *same file* could disagree on a column's dtype,
which breaks `pyarrow.ParquetWriter`'s fixed-schema requirement. File-level
processing eliminates this failure mode entirely, and only one day's data
(~125 MB) is ever resident in memory during ingestion.

**Schema drift handled via a registry, not a hardcoded column list.** Every
file's actual header is treated as ground truth. A `schema_registry.json`
tracks the first-seen/last-seen date for every column ever encountered
(schema-on-read). SMART columns are identified by a naming pattern
(`smart_\d+_(raw|normalized)`), not enumeration — across the 2-year window,
**197 distinct columns** were recorded, confirming real schema drift (not
just a theoretical concern the assessment was testing for).

**Incremental loading via per-date manifests + date-based partitioning.**
Every processed date becomes its own partition (`date=YYYY-MM-DD/`), and a
JSON manifest records what's already done. Re-running any stage diffs
against the manifest and only processes new dates — already-processed
partitions are never rewritten or rescanned. Verified directly: re-running
`ingest.py`, `transform.py`, and `aggregate.py` against the full,
already-processed dataset each reported **0 newly processed / all dates
skipped**.

**Aggregation happens once, incrementally, and Part 2 never re-scans raw
data.** `aggregate.py` rolls each day's cleaned partition into running
per-model, per-month totals (drive-days, failures, missing-SMART counts).
`analytics.py` only reads this small JSON summary — none of the five Part 2
outputs re-read Parquet or CSV.

**Data quality policies (explicit, not silent):**
- *Missing SMART values:* rows are **not dropped** — a null SMART reading
  still represents a valid drive-day for AFR purposes. Instead, each row
  gets a `missing_smart_pct` flag (fraction of SMART columns null for that
  row), which directly feeds the Part 2e completeness metric.
- *Drives that appear/disappear:* a drive registry
  (`data/drive_registry.json`) tracks first-seen/last-seen date per serial
  number. A drive disappearing from the dataset is **not** treated as a
  failure — only rows with `failure=1` count. Disappearance without a
  failure flag is right-censored data; see Limitations below.
- *Inconsistent capacity for the same model:* `capacity_bytes` is
  normalized per model to that model's most common (mode) reported value.
  Deviating rows are **flagged, not silently overwritten** — both the
  original (`capacity_bytes`) and normalized (`capacity_bytes_clean`)
  values are preserved for audit.

**Minimum drive-days threshold for reliability rankings.** The Top 10
most/least reliable models (Part 2b) exclude any model with fewer than
10,000 total drive-days, so a model with 3 drives and 1 failure can't
dominate the ranking with a distorted AFR. This threshold is a judgment
call, not derived from the data — noted here explicitly for that reason.

**Manufacturer identification via prefix pattern, not a per-model
mapping.** `analytics.py` matches model name prefixes (`ST` → Seagate,
`WDC`/`WD` → Western Digital, `HGST`/`HUS`/`HDS` → HGST, `TOSHIBA` →
Toshiba, `HP` → HP) against Backblaze's known naming conventions, rather
than hardcoding all 93 model names individually.

## Complexity analysis

- **Per-file ingestion:** O(rows_in_file) time, O(file_size) space — since
  each day's file is read whole rather than chunked (see rationale above).
  Space is bounded by the single largest daily file (~125 MB average, low
  hundreds of MB worst case), independent of total file count or total
  historical volume.
- **Incremental re-run (any stage):** O(new_rows) time — the manifest diff
  means already-processed dates are never re-read. A pipeline run over Δ
  new days costs O(Δ × avg_rows_per_day), not O(total_history).
- **Aggregation:** O(rows_in_partition) per date processed, O(distinct
  models × distinct months) space for the resident summary table — a few
  hundred KB regardless of how many raw rows have been processed historically.
- **Analytics (Part 2 a-e):** all five outputs are O(models × months) time
  and space — reading and summing the small pre-aggregated JSON — completely
  independent of the ~223M underlying drive-day row count.
- **Full pipeline over N days, run daily:** O(N × avg_rows_per_day) total
  work across all runs, but any single run only pays for the days it
  hasn't seen: O(Δdays × avg_rows_per_day) time, O(single_day_file_size +
  aggregation_state_size) space.

## Results (from this run)

- **Overall AFR:** ~1.47% (8,991 failures / 222,773,948 drive-days) —
  consistent with Backblaze's own published fleet-wide figures, used here
  as a sanity check on pipeline correctness.
- **Least reliable models** include several Toshiba MQ01ABF variants and
  Seagate ST12000NM0007 — consistent with patterns Backblaze has
  publicly reported for these models.
- **Manufacturer AFR:** HGST 2.96%, Seagate 1.73%, Toshiba 1.26%, Western
  Digital 0.65% (full breakdown output by `analytics.py`).
- **Data completeness:** on average, ~76% of the 197 total tracked SMART
  columns are null per row. This is expected, not a data-quality failure —
  different manufacturers report different SMART attribute subsets (a
  Toshiba drive doesn't populate the same fields as a Seagate drive), so
  most of the 197-column union is inapplicable to any single row by
  design. The per-row `missing_smart_pct` flag is a more meaningful
  completeness signal than raw null counts for this reason.

## Known limitations

- **Right-censoring on disappeared drives.** A drive that stops appearing
  in the dataset without `failure=1` may have failed, been retired, or
  been decommissioned for unrelated reasons — the raw data doesn't
  distinguish these. AFR calculated this way is standard practice (matches
  Backblaze's own methodology) but technically undercounts failures that
  occur without an explicit `failure=1` record on the drive's last active day.
- **Manufacturer mapping is prefix-based** and may misclassify unusual or
  OEM-rebadged model names not matching the five known prefixes (these
  fall into "Unknown" rather than being guessed incorrectly).
- **Capacity normalization uses the first-seen mode as the reference**,
  computed incrementally as models are first encountered — a model whose
  *true* typical capacity only becomes clear after more data arrives could
  have an early, less-representative reference value locked in.

## How to run

```bash
# 1. Set up environment
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 2. Download raw data (idempotent -- skips files already downloaded)
chmod +x download_data.sh
./download_data.sh

# 3. Run the pipeline stages in order (each is independently incremental)
python3 src/ingest.py
python3 src/transform.py
python3 src/aggregate.py
python3 src/analytics.py

# 4. Run tests
pytest tests/ -v
```

Re-running any of steps 3's scripts after the first successful run will
process 0 new dates (verified in this repo's commit history) — this is the
incremental-loading behavior in action.