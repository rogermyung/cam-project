from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from cam.store import load


def detect_spike(
    series: list[int],
    *,
    baseline_months: int = 12,
    threshold_pct: float = 50.0,
) -> dict:
    """Detect a late spike relative to the prior baseline window."""

    latest = series[-1]
    prior = series[:-1]
    count = min(baseline_months, len(prior))
    if count == 0:
        baseline = 0.0
    else:
        baseline = sum(prior[-count:]) / count

    if baseline == 0.0:
        ratio = None
        spike = len(series) > 1 and latest > 0
        baseline_out = 0.0
    else:
        ratio_unrounded = latest / baseline
        ratio = ratio_unrounded
        spike = ratio_unrounded > 1.0 + (threshold_pct / 100.0)
        baseline_out = round(baseline, 1)

    ratio_out = None if ratio is None else round(ratio, 2)
    return {
        "latest": latest,
        "baseline": baseline_out,
        "ratio": ratio_out,
        "spike": spike,
    }


def _parse_month(month: str) -> tuple[int, int]:
    year_s, mon_s = month.split("-", 1)
    return int(year_s), int(mon_s)


def _month_range(start: str, end: str) -> list[str]:
    start_y, start_m = _parse_month(start)
    end_y, end_m = _parse_month(end)
    months: list[str] = []
    y, m = start_y, start_m
    while (y, m) <= (end_y, end_m):
        months.append(f"{y:04d}-{m:02d}")
        if m == 12:
            y += 1
            m = 1
        else:
            m += 1
    return months


def build(data_dir: Path, site_dir: Path, *, now: datetime | None = None) -> dict:
    monthly_path = data_dir / "cfpb" / "monthly.csv"
    raw_rows = load(monthly_path)
    if not raw_rows:
        raise ValueError("No CFPB monthly.csv rows found")

    months_in_rows = [r["month"] for r in raw_rows]
    window_start = min(months_in_rows)
    window_end = max(months_in_rows)
    window_months = _month_range(window_start, window_end)

    status_path = data_dir / "cfpb" / "status.json"
    if status_path.exists():
        status = json.loads(status_path.read_text(encoding="utf-8"))
    else:
        status = {}

    by_company_months: dict[str, dict[str, int]] = {}
    by_company_products: dict[str, dict[str, int]] = {}
    for row in raw_rows:
        company = row["company"]
        month = row["month"]
        product = row["product"]
        complaints = int(row["complaints"] or 0)

        by_company_months.setdefault(company, {})
        by_company_products.setdefault(company, {})
        by_company_months[company][month] = by_company_months[company].get(month, 0) + complaints
        by_company_products[company][product] = (
            by_company_products[company].get(product, 0) + complaints
        )

    companies_out: list[dict] = []
    total_complaints = 0
    spikes = 0
    for company in by_company_months.keys():
        month_map = by_company_months[company]
        series = [month_map.get(m, 0) for m in window_months]
        total = sum(series)
        detect = detect_spike(series)

        products_map = by_company_products.get(company, {})
        products_sorted = sorted(products_map.items(), key=lambda x: (-x[1], x[0]))
        top_product = products_sorted[0][0] if products_sorted else ""
        products_out = [{"product": p, "complaints": n} for p, n in products_sorted]

        months_out = [{"month": m, "complaints": month_map.get(m, 0)} for m in window_months]

        if detect["spike"]:
            spikes += 1
        total_complaints += total

        obj = {
            "company": company,
            "total": total,
            "latest": detect["latest"],
            "baseline": detect["baseline"],
            "ratio": detect["ratio"],
            "spike": detect["spike"],
            "top_product": top_product,
            "months": months_out,
            "products": products_out,
        }
        companies_out.append(obj)

    companies_out.sort(key=lambda c: (-c["total"], c["company"]))
    window_start_out = window_start
    window_end_out = window_end

    if now is None:
        now = datetime.now(UTC)
    meta = {
        "generated_at": now.isoformat(),
        "window_start": window_start_out,
        "window_end": window_end_out,
        "latest_month": window_end_out,
        "companies": len(companies_out),
        "total_complaints": total_complaints,
        "spikes": spikes,
        "status": status,
    }

    site_dir.mkdir(parents=True, exist_ok=True)
    (site_dir / "cfpb_companies.json").write_text(
        json.dumps(companies_out, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    (site_dir / "cfpb_meta.json").write_text(
        json.dumps(meta, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return meta
