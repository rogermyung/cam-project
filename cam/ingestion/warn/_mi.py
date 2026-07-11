from __future__ import annotations

import json
import re
from datetime import date, datetime
from typing import Any

from bs4 import BeautifulSoup

from cam.ingestion.warn import WarnRecord


def _parse_first_mmddyyyy(value: str | None) -> date | None:
    if value is None:
        return None
    text = value.strip()
    if not text:
        return None

    m = re.search(r"(\d{1,2}/\d{1,2}/\d{4})", text)
    if not m:
        return None

    try:
        return datetime.strptime(m.group(1), "%m/%d/%Y").date()
    except ValueError:
        return None


def _parse_positive_int(value: str | None) -> int | None:
    if value is None:
        return None
    text = value.strip()
    if not text:
        return None
    text = text.replace(",", "")
    m = re.search(r"\d+", text)
    if not m:
        return None
    try:
        n = int(m.group(0))
    except ValueError:
        return None
    return n if n > 0 else None


def parse_mi(content: bytes) -> list[WarnRecord]:
    """Parse Michigan WARN notice JSON envelope into normalized WarnRecords."""

    payload: dict[str, Any] = json.loads(content)
    results = payload.get("Results", [])
    records: list[WarnRecord] = []

    for item in results:
        html = item.get("Html") or ""
        soup = BeautifulSoup(html, "html.parser")

        h3 = soup.find("h3")
        company = h3.get_text(" ", strip=True) if h3 else ""
        company = (company or "").strip()
        if not company:
            continue

        raw: dict[str, Any] = {}
        city_best_effort = ""
        city_seen = False

        for li in soup.find_all("li"):
            strong = li.find("strong")
            if strong is None:
                continue

            label_raw = strong.get_text(" ", strip=True)
            label = label_raw.rstrip(":").strip()
            if not label:
                continue

            # Value extraction: decode entities by using get_text. For a
            # "Site addresses" notice with a nested <ul>, get_text recurses, so
            # value_text already holds all site addresses concatenated — keep it
            # rather than blanking city (multi-site events still get location
            # context in the Event description).
            strong.extract()  # remove the label part so remaining text is the value
            value_text = li.get_text(" ", strip=True)
            value_text = (value_text or "").strip()

            # Keep the extracted label/value pairs as raw source.
            raw[label] = value_text

            if label in ("Site address", "Site addresses"):
                city_seen = True
                city_best_effort = value_text

        records.append(
            WarnRecord(
                state_code="MI",
                company=company,
                notice_date=_parse_first_mmddyyyy(raw.get("Layoff date")),
                employees_affected=_parse_positive_int(raw.get("Number of jobs impacted")),
                city=(city_best_effort if city_seen else ""),
                county=str(raw.get("County", "") or "").strip(),
                layoff_type=str(raw.get("Type of company action", "") or "").strip(),
                raw=raw,
            )
        )

    return records
