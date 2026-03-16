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
    # CA switched from CSV to XLSX in early 2026 (warn_report1.xlsx).
    # The old CSV URL (warn_report.csv) now returns 404.  Until XLSX ingestion
    # is implemented this source will return 0 records gracefully.
    # TODO: implement XLSX parsing and update URL to warn_report1.xlsx
    "CA": StateConfig(
        state_code="CA",
        url="https://edd.ca.gov/siteassets/files/jobs_and_training/warn/warn_report.csv",
        format="csv",
        columns={
            "company": "Company",
            "date": "Notice Date",
            "employees": "No. Of Employees Affected",
            "city": "City",
            "county": "County",
            "layoff_type": "Event Type",
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
    # MI: 403 from Akamai WAF as of March 2026 — ingest will log an error and
    # return 0 records until the state provides an alternative URL or format.
    "MI": StateConfig(
        state_code="MI",
        url="https://www.michigan.gov/leo/bureaus-agencies/wd/warn",
        format="html",
        columns={
            "company": "Employer Name",
            "date": "Warn Date",
            "employees": "Employees Affected",
            "city": "City",
            "county": "County",
            "layoff_type": "Notice Type",
        },
        date_fmt="%m/%d/%Y",
    ),
}
