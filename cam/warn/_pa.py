from __future__ import annotations

import re
from datetime import date

from bs4 import BeautifulSoup

from cam.warn import WarnRecord

_DATE_RE = re.compile(r"(\d{1,2})/(\d{1,2})/(\d{4}|\d{2})")


def _parse_date_from_match(match: re.Match[str]) -> date | None:
    month = int(match.group(1))
    day = int(match.group(2))
    year_raw = match.group(3)
    year = int(year_raw)
    if len(year_raw) == 2:
        year += 2000
    try:
        return date(year, month, day)
    except ValueError:
        return None


def _extract_first_valid_date(text: str) -> date | None:
    for m in _DATE_RE.finditer(text):
        parsed = _parse_date_from_match(m)
        if parsed is not None:
            return parsed
    return None


def _extract_first_valid_date_from_lines(lines: list[str]) -> date | None:
    for line in lines:
        found = _extract_first_valid_date(line)
        if found is not None:
            return found
    return None


def parse_pa(content: bytes) -> list[WarnRecord]:
    """Parse Pennsylvania WARN accordion listing into normalized WarnRecords."""

    html = content.decode("utf-8", errors="replace")
    soup = BeautifulSoup(html, "html.parser")

    all_items = soup.find_all("div", class_="cmp-accordion__item")
    if not all_items:
        return []

    records: list[WarnRecord] = []
    for item in all_items:
        # Leaf accordion items represent individual employer notices.
        if item.find("div", class_="cmp-accordion__item") is not None:
            continue

        title_el = item.find("span", class_="cmp-accordion__title")
        company = (title_el.get_text(strip=True) if title_el else "").strip()
        if not company:
            continue

        panel_el = item.find("div", class_="cmp-accordion__panel")
        if panel_el is None:
            continue

        panel_text = panel_el.get_text("\n", strip=True)
        panel_text = panel_text.replace("\xa0", " ").replace("\u200b", "")
        lines = [ln.strip() for ln in panel_text.splitlines()]
        lines = [ln for ln in lines if ln]

        raw_text = "\n".join(lines)

        county = ""
        employees_affected: int | None = None
        layoff_type = ""
        notice_date: date | None = None

        # Extract labelled fields from the first matching line (document order).
        for line in lines:
            line_upper = line.upper()

            if not county and (
                line_upper.startswith("COUNTY:") or line_upper.startswith("COUNTIES:")
            ):
                county = line.split(":", 1)[1].strip()
                continue

            if employees_affected is None and (
                line_upper.startswith("# AFFECTED:") or line_upper.startswith("AFFECTED:")
            ):
                rest = line.split(":", 1)[1]
                m = re.search(r"(\d[\d,]*)", rest)
                if m:
                    n = int(m.group(1).replace(",", ""))
                    if n > 0:
                        employees_affected = n
                continue

            if not layoff_type and line_upper.startswith("CLOSURE OR LAYOFF:"):
                layoff_type = line.split(":", 1)[1].strip()
                continue

            if notice_date is None and (
                line_upper.startswith("EFFECTIVE DATE:")
                or line_upper.startswith("EFFECTIVE DATES:")
            ):
                rest = line.split(":", 1)[1]
                notice_date = _extract_first_valid_date(rest)
                continue

        if notice_date is None:
            notice_date = _extract_first_valid_date_from_lines(lines)

        city = lines[0] if lines else ""
        records.append(
            WarnRecord(
                state_code="PA",
                company=company,
                notice_date=notice_date,
                employees_affected=employees_affected,
                city=city,
                county=county,
                layoff_type=layoff_type,
                raw={"title": company, "text": raw_text},
            )
        )

    return records


__all__ = ["parse_pa"]
