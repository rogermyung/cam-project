"""
State-by-state WARN Act data source configuration.

Each StateConfig specifies the URL, format ("csv", "html", "pdf"), and
column-name mappings used when parsing that state's WARN notice list.

Design note — why these URLs are NOT in ``cam.config.Settings``
----------------------------------------------------------------
``cam.config.Settings`` holds environment-specific overrides (database
credentials, API keys, numeric thresholds) that may differ between dev,
staging, and production deployments.  The state URLs here are static
reference data: they are identical across all environments and must be
updated only after human review of state labour department websites.
Placing them in Settings would require dozens of environment variables
with no operational benefit and would make accidental overrides easier.

State entries are maintained manually — state labour department URLs
change frequently and require human verification before updating.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class StateConfig:
    """Configuration for a single state's WARN data source."""

    state_code: str
    url: str
    format: str  # "csv" | "html" | "pdf"
    # Mapping from canonical field names to column headers in the source data.
    # Canonical keys: "company", "date", "employees", "city", "county", "layoff_type"
    columns: dict[str, str] = field(default_factory=dict)
    # CSS / XPath selector to identify the table in HTML sources (ignored for CSV/PDF)
    html_table_id: str | None = None
    # Date format string for strptime; defaults to common US format MM/DD/YYYY
    date_fmt: str = "%m/%d/%Y"


# ---------------------------------------------------------------------------
# Priority states (CA, TX, NY, FL, IL, OH, PA, MI)
# ---------------------------------------------------------------------------

STATE_CONFIGS: dict[str, StateConfig] = {
    # CA switched from CSV to a multi-sheet XLSX workbook in early 2026
    # (warn_report1.xlsx); the old warn_report.csv URL now 404s.  Parsed by
    # cam.ingestion.warn._xlsx.parse_ca_xlsx, which reads the "Detailed WARN
    # Report" sheet directly — the ``columns`` map below is unused for xlsx and
    # kept only as documentation of the source fields.
    "CA": StateConfig(
        state_code="CA",
        url="https://edd.ca.gov/siteassets/files/jobs_and_training/warn/warn_report1.xlsx",
        format="xlsx",
        columns={
            "company": "Company",
            "date": "Notice Date",
            "employees": "No. Of Employees",
            "city": "Address",
            "county": "County/Parish",
            "layoff_type": "Layoff/Closure",
        },
        date_fmt="%m/%d/%Y",
    ),
    # TX moved to https://www.twc.texas.gov/data-reports/warn-notice
    "TX": StateConfig(
        state_code="TX",
        url="https://www.twc.texas.gov/data-reports/warn-notice",
        format="html",
        columns={
            "company": "Company Name",
            "date": "Layoff Date",
            "employees": "Number of Employees",
            "city": "City",
            "county": "County",
            "layoff_type": "Type of Layoff",
        },
        date_fmt="%m/%d/%Y",
    ),
    # NY moved to legacy-warn-notices
    "NY": StateConfig(
        state_code="NY",
        url="https://dol.ny.gov/legacy-warn-notices",
        format="html",
        columns={
            "company": "Employer",
            "date": "Effective Date",
            "employees": "Employees Affected",
            "city": "Region",
            "county": "County",
            "layoff_type": "Reason",
        },
        date_fmt="%m/%d/%Y",
    ),
    # FL: year-specific CSV; update the year as new files are published.
    "FL": StateConfig(
        state_code="FL",
        url="https://floridajobs.org/docs/default-source/communications/2026-warn-notices.csv",
        format="csv",
        columns={
            "company": "Company",
            "date": "Date",
            "employees": "Employees",
            "city": "City",
            "county": "County",
            "layoff_type": "Type",
        },
        date_fmt="%m/%d/%Y",
    ),
    # IL: year-specific PDF; update the year as new files are published.
    "IL": StateConfig(
        state_code="IL",
        url="https://www.illinoisworknet.com/WARN/Documents/2026-WARN-Notices.pdf",
        format="pdf",
        columns={
            "company": "Employer Name",
            "date": "Date of Notice",
            "employees": "# of Employees",
            "city": "City",
            "county": "County",
            "layoff_type": "Type",
        },
        date_fmt="%m/%d/%Y",
    ),
    # OH: year-specific page; update the year as new pages are published.
    "OH": StateConfig(
        state_code="OH",
        url="https://jfs.ohio.gov/warn/2026.stm",
        format="html",
        columns={
            "company": "Company Name",
            "date": "Effective Date",
            "employees": "Employees",
            "city": "City",
            "county": "County",
            "layoff_type": "Layoff Type",
        },
        date_fmt="%m/%d/%Y",
    ),
    # PA moved to pa.gov in 2026
    "PA": StateConfig(
        state_code="PA",
        url="https://www.pa.gov/agencies/dli/programs-services/workforce-development-home/warn-requirements.html",
        format="html",
        columns={
            "company": "Company",
            "date": "Effective Date",
            "employees": "Affected Workers",
            "city": "Municipality",
            "county": "County",
            "layoff_type": "Type",
        },
        date_fmt="%m/%d/%Y",
    ),
    # MI retired the old .../wd/warn HTML page (now 404) in 2026.  The current
    # notices come from the Sitecore search API behind the JS listing page at
    # .../wd/data-public-notices/warn-notices, which returns a JSON envelope of
    # HTML fragments.  Parsed by cam.ingestion.warn._mi.parse_mi.  The endpoint
    # is guarded by an Akamai WAF that 403s non-browser clients — _fetch sends a
    # Mozilla User-Agent to satisfy it.  p=500 returns the full result set.
    "MI": StateConfig(
        state_code="MI",
        url=(
            "https://www.michigan.gov/leo/sxa/search/results/"
            "?s={8E97AB1D-D2D4-47F8-8CC4-3F1039C8854F}"
            "&itemid={BE81F7C2-36A8-4FDE-853C-B05B6E090055}"
            "&sig=&autoFireSearch=true"
            "&v={1FFFCC21-5151-4A2B-ABFC-F7FE4E5C9783}"
            "&p=500&o=Created%20Date%20sort%2CDescending"
        ),
        format="mi_json",
        columns={
            "company": "Employer Name",
            "date": "Layoff date",
            "employees": "Number of jobs impacted",
            "city": "Site address",
            "county": "County",
            "layoff_type": "Type of company action",
        },
        date_fmt="%m/%d/%Y",
    ),
}
