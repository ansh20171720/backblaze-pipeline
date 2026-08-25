import json
import re
from collections import defaultdict

MODEL_MONTHLY_PATH = "data/model_monthly_summary.json"
CLEAN_MANIFEST_PATH = "data/clean_manifest.json"

MIN_DRIVE_DAYS_FOR_RANKING = 10_000  # avoid tiny-sample models dominating rankings

# Manufacturer prefixes, derived from Backblaze's published model naming
# conventions -- a small mapping, not a per-model hardcoded list.
MANUFACTURER_PATTERNS = [
    (re.compile(r"^ST", re.I), "Seagate"),
    (re.compile(r"^WDC|^WD", re.I), "Western Digital"),
    (re.compile(r"^HGST|^HUS|^HDS", re.I), "HGST"),
    (re.compile(r"^TOSHIBA", re.I), "Toshiba"),
    (re.compile(r"^HP", re.I), "HP"),
]


def load_summary():
    with open(MODEL_MONTHLY_PATH) as f:
        return json.load(f)


def guess_manufacturer(model: str) -> str:
    for pattern, name in MANUFACTURER_PATTERNS:
        if pattern.match(model):
            return name
    return "Unknown"


def model_totals(summary: dict) -> dict:
    """Collapse each model's per-month buckets into a single totals dict."""
    totals = {}
    for model, months in summary.items():
        drive_days = sum(m["drive_days"] for m in months.values())
        failures = sum(m["failures"] for m in months.values())
        missing = sum(m["rows_with_missing_smart"] for m in months.values())
        totals[model] = {
            "drive_days": drive_days,
            "failures": failures,
            "rows_with_missing_smart": missing,
        }
    return totals


def compute_afr(drive_days: int, failures: int) -> float:
    if drive_days == 0:
        return 0.0
    return (failures / drive_days) * 365 * 100  # as a percentage


# ---- a. AFR by model ----
def afr_by_model(summary: dict) -> dict:
    totals = model_totals(summary)
    return {
        model: round(compute_afr(t["drive_days"], t["failures"]), 4)
        for model, t in totals.items()
    }


# ---- b. Top 10 most / least reliable models ----
def top_reliable_models(summary: dict, n: int = 10):
    totals = model_totals(summary)
    eligible = {
        model: t for model, t in totals.items()
        if t["drive_days"] >= MIN_DRIVE_DAYS_FOR_RANKING
    }
    afr = {
        model: compute_afr(t["drive_days"], t["failures"])
        for model, t in eligible.items()
    }
    most_reliable = sorted(afr.items(), key=lambda x: x[1])[:n]
    least_reliable = sorted(afr.items(), key=lambda x: x[1], reverse=True)[:n]
    return most_reliable, least_reliable


# ---- c. Total drive-days and failures per manufacturer ----
def manufacturer_rollup(summary: dict) -> dict:
    totals = model_totals(summary)
    rollup = defaultdict(lambda: {"drive_days": 0, "failures": 0})
    for model, t in totals.items():
        mfr = guess_manufacturer(model)
        rollup[mfr]["drive_days"] += t["drive_days"]
        rollup[mfr]["failures"] += t["failures"]
    for mfr, t in rollup.items():
        t["afr_pct"] = round(compute_afr(t["drive_days"], t["failures"]), 4)
    return dict(rollup)


# ---- d. Monthly failure trend ----
def monthly_failure_trend(summary: dict) -> dict:
    monthly = defaultdict(lambda: {"drive_days": 0, "failures": 0})
    for model, months in summary.items():
        for month, bucket in months.items():
            monthly[month]["drive_days"] += bucket["drive_days"]
            monthly[month]["failures"] += bucket["failures"]
    for month, t in monthly.items():
        t["afr_pct"] = round(compute_afr(t["drive_days"], t["failures"]), 4)
    return dict(sorted(monthly.items()))


# ---- e. Data completeness: avg % of SMART fields missing per row ----
def missing_smart_percentage(clean_manifest_path=CLEAN_MANIFEST_PATH) -> float:
    """Weighted average fraction of SMART columns null per row, across all
    dates. This is more informative than '% of rows with at least one
    missing field', since different manufacturers report different SMART
    attribute subsets -- meaning 'any missing' trivially converges to
    ~100% of rows and doesn't actually indicate severity. Reuses the
    avg_missing_smart_pct already computed per-date in transform.py, so
    this reads the small clean_manifest.json rather than touching any
    Parquet or raw data."""
    with open(clean_manifest_path) as f:
        manifest = json.load(f)["cleaned_dates"]

    total_weighted = 0.0
    total_rows = 0
    for date, info in manifest.items():
        total_weighted += info["avg_missing_smart_pct"] * info["rows"]
        total_rows += info["rows"]

    if total_rows == 0:
        return 0.0
    return round((total_weighted / total_rows) * 100, 4)


def run_analytics():
    summary = load_summary()

    print("=== a. AFR by model (sample of 5) ===")
    afr = afr_by_model(summary)
    for model, rate in list(afr.items())[:5]:
        print(f"  {model}: {rate}%")

    print(f"\n=== b. Top 10 most / least reliable (min {MIN_DRIVE_DAYS_FOR_RANKING} drive-days) ===")
    most, least = top_reliable_models(summary)
    print("Most reliable:")
    for model, rate in most:
        print(f"  {model}: {round(rate, 4)}%")
    print("Least reliable:")
    for model, rate in least:
        print(f"  {model}: {round(rate, 4)}%")

    print("\n=== c. Manufacturer rollup ===")
    for mfr, t in manufacturer_rollup(summary).items():
        print(f"  {mfr}: drive_days={t['drive_days']}, failures={t['failures']}, AFR={t['afr_pct']}%")

    print("\n=== d. Monthly failure trend (first 5 months) ===")
    trend = monthly_failure_trend(summary)
    for month, t in list(trend.items())[:5]:
        print(f"  {month}: drive_days={t['drive_days']}, failures={t['failures']}, AFR={t['afr_pct']}%")

    print(f"\n=== e. Data completeness ===")
    print(f"  Avg % of SMART fields missing per row (weighted across all dates): {missing_smart_percentage()}%")


if __name__ == "__main__":
    run_analytics()