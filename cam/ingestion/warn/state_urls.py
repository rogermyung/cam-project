"""
State-by-state WARN Act data source configuration.

Each StateConfig specifies the URL, format ("csv", "html", "pdf"), and
column-name mappings used when parsing that state's WARN notice list.

This file is intentionally maintained manually — state labour department
URLs change frequently and require human verification.
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
    "TX": StateConfig(
        state_code="TX",
        url=(
            "https://www.twc.texas.gov/businesses/"
            "worker-adjustment-and-retraining-notification-warn-notices"
        ),
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
    "NY": StateConfig(
        state_code="NY",
        url="https://dol.ny.gov/warn-notices",
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
    "FL": StateConfig(
        state_code="FL",
        url="https://floridajobs.org/docs/default-source/communications/2023-warn-notices.csv",
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
    "IL": StateConfig(
        state_code="IL",
        url="https://www.illinoisworknet.com/WARN/Documents/2023-WARN-Notices.pdf",
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
    "OH": StateConfig(
        state_code="OH",
        url="https://jfs.ohio.gov/warn/2023.stm",
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
    "PA": StateConfig(
        state_code="PA",
        url="https://www.dli.pa.gov/Individuals/Workforce-Development/warn/Pages/default.aspx",
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
