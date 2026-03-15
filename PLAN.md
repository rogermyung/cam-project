# Corporate Accountability Monitor — Project Plan

## Purpose

This document is the authoritative implementation plan for the Corporate Accountability Monitor (CAM) system. It is structured for use by Claude Code or any AI coding agent. Each module is defined with clear inputs, outputs, acceptance criteria, and test requirements. Work proceeds module by module; do not begin a module until its dependencies are marked complete.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                     DATA INGESTION LAYER                     │
│  EDGAR API │ OSHA API │ EPA API │ CFPB API │ PACER │ State  │
└─────────────────────┬───────────────────────────────────────┘
                      │
┌─────────────────────▼───────────────────────────────────────┐
│                   ENTITY RESOLUTION LAYER                    │
│         Normalize company names → canonical entity IDs       │
└─────────────────────┬───────────────────────────────────────┘
                      │
┌─────────────────────▼───────────────────────────────────────┐
│                     ANALYSIS LAYER                           │
│  Violation Aggregation │ NLP Signals │ Merger Screening      │
└─────────────────────┬───────────────────────────────────────┘
                      │
┌─────────────────────▼───────────────────────────────────────┐
│                      ALERT LAYER                             │
│         Threshold scoring → structured alert output          │
└─────────────────────┬───────────────────────────────────────┘
                      │
┌─────────────────────▼───────────────────────────────────────┐
│                      OUTPUT LAYER                            │
│        Dashboard │ API │ Digest emails │ Export formats       │
└─────────────────────────────────────────────────────────────┘
```

**Language:** Python 3.11+  
**Storage:** PostgreSQL (structured data) + S3-compatible object store (raw documents)  
**Queue:** Redis-backed task queue (Celery)  
**Testing:** pytest, with fixtures for all external API calls (no live API calls in tests)  
**Config:** All credentials and thresholds in environment variables; never hardcoded  

---

## Module Index

| ID | Module | Depends On | Status |
|----|--------|------------|--------|
| M0 | Project scaffolding | — | TODO |
| M1 | Entity Resolution | M0 | TODO |
| M2 | EDGAR Ingestion | M0, M1 | TODO |
| M3 | OSHA Ingestion | M0, M1 | TODO |
| M4 | EPA Ingestion | M0, M1 | TODO |
| M5 | CFPB Ingestion | M0, M1 | TODO |
| M6 | Cross-Agency Aggregation | M2, M3, M4, M5 | TODO |
| M7 | 10-K Risk Language NLP | M2 | TODO |
| M8 | Earnings Call NLP | M2 | TODO |
| M9 | Proxy Statement Parser | M2 | TODO |
| M10 | HSR Merger Screener | M0, M1 | TODO |
| M11 | WARN Act Ingestion | M0, M1 | TODO |
| M12 | PE/Bankruptcy Correlator | M11, M0 | TODO |
| M13 | Alert Scoring Engine | M6, M7, M8, M9, M10, M12 | TODO |
| M14 | Output Layer | M13 | TODO |

---

## M0 — Project Scaffolding

### Goal
Establish repo structure, shared utilities, database schema, and CI pipeline that all other modules depend on.

### Directory Structure
```
cam/
├── ingestion/          # One subpackage per data source
├── entity/             # Entity resolution
├── analysis/           # NLP and aggregation modules
├── alerts/             # Scoring and alert logic
├── output/             # Dashboard, API, exports
├── db/
│   ├── models.py       # SQLAlchemy models
│   └── migrations/     # Alembic migrations
├── tests/
│   ├── fixtures/       # Canned API responses for offline testing
│   └── unit/
├── config.py           # Pydantic settings from env vars
└── tasks.py            # Celery task definitions
```

### Database Schema (initial tables)
```sql
-- Canonical company entities
CREATE TABLE entities (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    canonical_name TEXT NOT NULL,
    ticker TEXT,
    lei TEXT,           -- Legal Entity Identifier (global standard)
    ein TEXT,           -- Employer Identification Number
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);

-- Maps raw name strings to canonical entity IDs
CREATE TABLE entity_aliases (
    id SERIAL PRIMARY KEY,
    entity_id UUID REFERENCES entities(id),
    raw_name TEXT NOT NULL,
    source TEXT NOT NULL,   -- e.g. 'osha', 'sec', 'manual'
    confidence FLOAT,
    UNIQUE(raw_name, source)
);

-- All violations/events from all sources land here
CREATE TABLE events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_id UUID REFERENCES entities(id),
    source TEXT NOT NULL,           -- 'osha', 'epa', 'cfpb', 'nlrb', etc.
    event_type TEXT NOT NULL,       -- 'violation', 'complaint', 'fine', 'lawsuit'
    event_date DATE,
    penalty_usd NUMERIC,
    description TEXT,
    raw_url TEXT,
    raw_json JSONB,
    ingested_at TIMESTAMPTZ DEFAULT now()
);

-- NLP-derived signals from documents
CREATE TABLE signals (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_id UUID REFERENCES entities(id),
    source TEXT NOT NULL,       -- 'edgar_10k', 'earnings_call', 'proxy'
    signal_type TEXT NOT NULL,  -- 'risk_language_expansion', 'captive_strategy', 'say_on_pay_fail'
    signal_date DATE,
    score FLOAT,                -- 0.0 to 1.0
    evidence TEXT,              -- Extracted text supporting the signal
    document_url TEXT,
    created_at TIMESTAMPTZ DEFAULT now()
);

-- Composite alert scores per entity per time period
CREATE TABLE alert_scores (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_id UUID REFERENCES entities(id),
    score_date DATE NOT NULL,
    composite_score FLOAT NOT NULL,
    component_scores JSONB,     -- breakdown by signal type
    alert_level TEXT,           -- 'watch', 'elevated', 'critical'
    created_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE(entity_id, score_date)
);
```

### Acceptance Criteria
- [ ] `pytest` runs with zero failures on an empty fixture set
- [ ] `alembic upgrade head` creates all tables without errors
- [ ] `config.py` raises `ValidationError` if required env vars are missing
- [ ] CI pipeline (GitHub Actions or equivalent) runs tests on every push
- [ ] `docker-compose up` starts postgres, redis, and a worker process

### Test Requirements
- Test that config fails fast on missing env vars
- Test that all migrations are reversible (`alembic downgrade -1`)

---

## M1 — Entity Resolution

### Goal
Given a raw company name string from any data source, return a canonical `entity_id`. Handle subsidiaries, DBAs, ticker symbols, and name changes. This is the hardest module technically; do not underestimate it.

### Approach
1. **Exact match** against `entity_aliases` table (fastest path)
2. **Fuzzy match** using token-based similarity (rapidfuzz) against all known aliases
3. **External lookup** via OpenCorporates API or SEC EDGAR company search if fuzzy score < threshold
4. **Manual review queue** for matches below minimum confidence

### Key Functions
```python
# cam/entity/resolver.py

def resolve(raw_name: str, source: str, hint: dict = None) -> ResolveResult:
    """
    Returns:
        ResolveResult(
            entity_id: UUID | None,
            canonical_name: str | None,
            confidence: float,      # 0.0 to 1.0
            method: str,            # 'exact', 'fuzzy', 'api', 'unresolved'
            needs_review: bool
        )
    hint: optional dict with keys like 'ticker', 'state', 'ein' to improve matching
    """

def bulk_resolve(records: list[dict], source: str) -> list[ResolveResult]:
    """Batch version. Uses DB bulk lookups before falling back to per-record resolution."""

def add_alias(entity_id: UUID, raw_name: str, source: str, confidence: float) -> None:
    """Persist a new alias after manual review or high-confidence automatic match."""
```

### Thresholds (configurable via env vars)
```
ENTITY_EXACT_THRESHOLD=1.0      # Exact string match
ENTITY_FUZZY_THRESHOLD=0.85     # Accept fuzzy match automatically
ENTITY_REVIEW_THRESHOLD=0.65    # Queue for manual review
# Below ENTITY_REVIEW_THRESHOLD → unresolved, logged as warning
```

### Test Requirements
- Unit tests with 50+ real company name variations (subsidiaries, punctuation differences, abbreviations)
- Test that "CVS Health Corporation", "CVS Pharmacy Inc", "CVS Caremark" all resolve to the same entity when seeded correctly
- Test that low-confidence matches are queued for review, not silently dropped
- Test bulk_resolve performance: 1000 records should complete in < 5 seconds using DB batch lookups
- All external API calls must be mocked in tests

### Acceptance Criteria
- [ ] Resolution accuracy > 90% on a labeled test set of 200 known company name pairs
- [ ] No external HTTP calls in unit tests
- [ ] Manual review queue is queryable and actionable via CLI

---

## M2 — EDGAR Ingestion

### Goal
Ingest SEC filings for all public companies. Priority filing types: 10-K (annual report), DEF 14A (proxy statement), 8-K (material events), S-4/424B (offering documents for debt issuances).

### Data Source
SEC EDGAR Full-Text Search API: `https://efts.sec.gov/LATEST/search-index?q=...`  
EDGAR Filing API: `https://data.sec.gov/submissions/{CIK}.json`  
EDGAR XBRL API: `https://data.sec.gov/api/xbrl/companyfacts/{CIK}.json`  

Rate limit: 10 requests/second. Always include `User-Agent` header with project contact email.

### Key Functions
```python
# cam/ingestion/edgar.py

def fetch_company_filings(cik: str, filing_types: list[str], 
                           since_date: date) -> list[FilingMetadata]:
    """Fetch filing metadata for a given CIK. Does not download full documents."""

def download_filing(filing_metadata: FilingMetadata) -> FilingDocument:
    """Download the full filing text and store to object store. Returns path."""

def get_cik_for_ticker(ticker: str) -> str | None:
    """Resolve ticker symbol to CIK using EDGAR company search."""

def ingest_all_10k(since_date: date, entity_ids: list[UUID] = None) -> IngestResult:
    """Scheduled task: ingest all 10-K filings since a given date."""
```

### Storage
- Filing metadata in `events` table (source='sec_edgar', event_type='filing')
- Full document text stored to object store at `edgar/{cik}/{accession_number}/full.txt`
- XBRL financial data stored as JSONB in `events.raw_json`

### Test Requirements
- Mock all HTTP calls using `responses` or `httpx` mock
- Include fixture files for at least 3 real companies' filing metadata responses
- Test that rate limiting is respected (no burst of >10 req/s)
- Test that filing text is correctly parsed from both HTML and plain text formats
- Test that already-ingested filings are not re-downloaded (idempotency)

### Acceptance Criteria
- [ ] Can ingest 500 companies' annual 10-K filings in a single overnight run
- [ ] Handles EDGAR HTTP 429 (rate limit) gracefully with exponential backoff
- [ ] Duplicate detection prevents re-ingesting already-stored filings

---

## M3 — OSHA Ingestion

### Goal
Ingest OSHA inspection records, violation citations, and penalty data. OSHA data is the most directly work-harm-relevant signal in the system.

### Data Source
OSHA Enforcement Data: `https://www.osha.gov/foia/enforcement-data`  
Available as bulk CSV download updated quarterly. Also queryable via:  
`https://data.dol.gov/get/inspections` (Department of Labor API)

### Schema Mapping
```python
# Map OSHA fields to events table
{
    "source": "osha",
    "event_type": "violation",  # or 'inspection' for no-violation inspections
    "event_date": osha_record["open_date"],
    "penalty_usd": osha_record["initial_penalty"],
    "description": f"{osha_record['violation_type']}: {osha_record['citation_text']}",
    "raw_json": osha_record  # full record preserved
}
```

### Key Functions
```python
# cam/ingestion/osha.py

def download_bulk_data(year: int) -> Path:
    """Download full OSHA enforcement CSV for a given year."""

def ingest_from_csv(csv_path: Path, since_date: date = None) -> IngestResult:
    """Parse CSV, resolve entities, insert events. Idempotent."""

def fetch_recent_inspections(days_back: int = 30) -> list[dict]:
    """Poll DOL API for inspections in last N days. For near-real-time updates."""
```

### Test Requirements
- Include a 100-row fixture CSV with realistic OSHA data including edge cases (missing fields, unusual company names, multi-establishment employers)
- Test entity resolution for OSHA's establishment name format (often "COMPANY NAME - ESTABLISHMENT CITY")
- Test that penalty amounts are correctly parsed (OSHA uses string format with no currency symbol in some exports)
- Test idempotency: running ingest twice on the same CSV produces identical DB state

### Acceptance Criteria
- [ ] Full historical OSHA dataset (2010–present, ~5M records) ingests in < 4 hours
- [ ] Entity resolution rate > 70% for records matching known entities in DB
- [ ] Unresolved establishments are logged with enough context for manual review

---

## M4 — EPA Ingestion

### Goal
Ingest EPA Toxic Release Inventory (TRI), Clean Air Act enforcement actions, and RCRA (hazardous waste) violation records.

### Data Sources
- TRI: `https://www.epa.gov/toxics-release-inventory-tri-program/tri-data-and-tools` (annual bulk download)
- ECHO (Enforcement and Compliance History Online): `https://echo.epa.gov/tools/data-downloads`
- ECHO API: `https://echo.epa.gov/api/swagger/ui`

### Priority Fields
TRI is particularly valuable for NLP comparison: it shows what a facility *reports* releasing. When TRI self-reports diverge significantly from enforcement actions at the same facility, it is a signal of under-reporting.

### Key Functions
```python
# cam/ingestion/epa.py

def ingest_tri(year: int) -> IngestResult:
    """Ingest TRI annual release data. Store as event_type='tri_release'."""

def ingest_echo_violations(since_date: date) -> IngestResult:
    """Ingest EPA enforcement actions from ECHO."""

def compute_tri_enforcement_divergence(entity_id: UUID, 
                                        year: int) -> float | None:
    """
    Compare self-reported TRI releases to enforcement actions for same entity/year.
    Returns divergence score (higher = more concerning discrepancy).
    Returns None if insufficient data.
    """
```

### Test Requirements
- Fixture data for at least 2 facilities with both TRI reports and enforcement actions
- Test divergence computation with known good/bad examples
- Test that facility-level data is correctly rolled up to parent entity

### Acceptance Criteria
- [ ] TRI data for 2015–present ingested
- [ ] ECHO violation records updated at least weekly via scheduled task
- [ ] Divergence scores computed for all entities with both TRI and ECHO data

---

## M5 — CFPB Ingestion

### Goal
Ingest CFPB consumer complaint database and enforcement action records. Consumer complaint velocity is a leading indicator of consumer harm before regulatory action.

### Data Sources
- Consumer Complaint Database: `https://www.consumerfinance.gov/data-research/consumer-complaints/` (bulk CSV and API)
- Enforcement Actions: `https://www.consumerfinance.gov/enforcement/actions/`

### Key Design Decision
Raw complaint counts are not meaningful without normalization. Store raw counts but always compute complaint rate per unit of business activity (e.g., per $1B in assets for banks, per million accounts). This requires joining CFPB data with EDGAR financial data — M5 therefore soft-depends on M2.

### Key Functions
```python
# cam/ingestion/cfpb.py

def ingest_complaints(since_date: date) -> IngestResult:
    """Ingest new complaints from CFPB API."""

def compute_complaint_rate(entity_id: UUID, 
                            period_months: int = 12) -> ComplaintRate | None:
    """
    Returns complaints per $1B assets for the trailing N months.
    Returns None if financial data not available for normalization.
    """

def detect_complaint_spike(entity_id: UUID, 
                            lookback_months: int = 6,
                            threshold_pct: float = 50.0) -> bool:
    """
    Returns True if complaint rate in most recent 3 months is threshold_pct 
    higher than prior 3 months. Designed to catch emerging problems early.
    """
```

### Test Requirements
- Test normalization logic with known asset figures
- Test spike detection with synthetic time series data
- Test that complaint categories are preserved in raw_json for downstream NLP

### Acceptance Criteria
- [ ] Complaint database updated daily via scheduled task
- [ ] Complaint rates computed for all entities with EDGAR financial data
- [ ] Spike detection fires correctly on test time series

---

## M6 — Cross-Agency Aggregation

### Goal
The core value module. Join signals from M2–M5 into a per-entity, per-time-period composite view. Identify companies accumulating simultaneous signals across agencies — the pattern most predictive of congressional investigation.

### Logic
```python
# cam/analysis/aggregation.py

@dataclass
class AgencySignalSummary:
    entity_id: UUID
    period_start: date
    period_end: date
    osha_violation_count: int
    osha_penalty_total: float
    osha_vs_industry_benchmark: float   # ratio: entity rate / industry average
    epa_violation_count: int
    epa_penalty_total: float
    cfpb_complaint_rate: float
    cfpb_spike_detected: bool
    nlrb_complaint_count: int           # from future M module; 0 until implemented
    agency_overlap_count: int           # how many agencies have active signals
    composite_risk_score: float         # 0.0 to 1.0, see scoring spec below

def compute_agency_summary(entity_id: UUID, 
                            period_end: date,
                            lookback_days: int = 365) -> AgencySignalSummary:
    """Compute the composite summary for one entity over the lookback window."""

def compute_industry_benchmarks(naics_code: str, 
                                 period_end: date) -> dict:
    """
    Returns industry averages for violation rates, penalty totals, etc.
    Used to contextualize individual company scores.
    NAICS codes from OSHA records; store in entities table.
    """
```

### Composite Score Specification
The composite risk score is a weighted sum of normalized sub-scores. Weights are configurable via env vars.

```
WEIGHT_OSHA_RATE=0.25           # OSHA violation rate vs industry benchmark
WEIGHT_EPA_RATE=0.20            # EPA violation rate vs industry benchmark
WEIGHT_CFPB_SPIKE=0.20          # Binary: CFPB complaint spike detected
WEIGHT_AGENCY_OVERLAP=0.35      # Number of agencies with concurrent signals (non-linear)
```

Agency overlap uses a non-linear bonus: a company with signals in 3+ agencies scores disproportionately higher than one with a single agency signal. This reflects the empirical pattern that multi-agency signal overlap is the strongest predictor of congressional investigation.

```python
def agency_overlap_bonus(n_agencies: int) -> float:
    # 1 agency: 0.0 bonus
    # 2 agencies: 0.3 bonus
    # 3+ agencies: 0.7 bonus
    return {0: 0.0, 1: 0.0, 2: 0.3}.get(n_agencies, 0.7)
```

### Test Requirements
- Unit tests for composite score with known inputs and expected outputs
- Test that industry benchmarks correctly segment by NAICS code
- Test the agency overlap bonus function at each threshold
- Integration test: seed events from M3/M4/M5 fixtures and verify summary output
- Test that missing data (e.g., entity has no EPA records) defaults gracefully to 0.0 sub-scores

### Acceptance Criteria
- [ ] Summaries computable for all entities with at least one event in any source
- [ ] Industry benchmarks cover all 2-digit NAICS codes present in OSHA data
- [ ] Score is reproducible: same inputs always produce same output

---

## M7 — 10-K Risk Language NLP

### Goal
Detect year-over-year expansion in risk factor language related to labor, environment, and consumer harm. When a company's own lawyers expand their risk disclosures, it is a leading indicator that something has changed internally.

### Method
1. For each entity, retrieve current and prior year 10-K from object store
2. Extract the "Risk Factors" section (Item 1A)
3. Compute sentence-level similarity between years using a sentence transformer model
4. Identify sentences/paragraphs that are new or substantially expanded
5. Classify new content by topic using a zero-shot classifier

### Topic Categories
```python
RISK_TOPICS = [
    "labor_relations",          # union activity, worker complaints, wage disputes
    "regulatory_investigation", # government probe, subpoena, inquiry
    "supply_chain_labor",       # supplier labor practices, forced labor
    "environmental_liability",  # contamination, cleanup, EPA action
    "consumer_harm",            # product liability, consumer complaints, fraud
    "antitrust_competition",    # market concentration, DOJ/FTC investigation
]
```

### Key Functions
```python
# cam/analysis/risk_nlp.py

def extract_risk_section(filing_text: str) -> str:
    """
    Extract Item 1A (Risk Factors) from 10-K text.
    Handles both HTML and plain text EDGAR formats.
    Falls back to heuristic section detection if standard headers absent.
    """

def compute_risk_expansion(
    current_text: str,
    prior_text: str,
    topics: list[str] = RISK_TOPICS
) -> RiskExpansionResult:
    """
    Returns:
        RiskExpansionResult(
            expansion_score: float,         # 0.0 to 1.0
            new_sentences: list[str],
            topic_scores: dict[str, float], # per-topic expansion score
            evidence: list[dict]            # new/expanded text with topic labels
        )
    """

def classify_risk_topics(text: str, 
                          topics: list[str]) -> dict[str, float]:
    """Zero-shot topic classification using a pre-trained model."""
```

### Model
Use `sentence-transformers/all-MiniLM-L6-v2` for sentence embeddings (small, fast, adequate quality).  
Use `facebook/bart-large-mnli` for zero-shot classification.  
Both available via HuggingFace `transformers`. Pin model versions in requirements.

### Test Requirements
- Include 3 pairs of real 10-K risk sections (same company, consecutive years) as fixtures
- One pair should show significant expansion (company with known emerging problem)
- One pair should show minimal change (stable company)
- Test section extraction on both HTML and plain text formats
- Test topic classification with known examples for each topic category
- All model calls must be mockable for CI (use fixture embeddings, not live model)

### Acceptance Criteria
- [ ] Section extraction succeeds on > 95% of 10-K filings in test set
- [ ] Topic classification F1 > 0.70 on labeled test examples (100 sentences per topic)
- [ ] Processing time per filing pair < 30 seconds on CPU

---

## M8 — Earnings Call NLP

### Goal
Detect language in earnings call transcripts and investor presentations that signals value extraction strategies — language companies use with investors but not with regulators or the public. The "captive strategy" language CVS used on investor calls is the canonical example.

### Data Source
Earnings call transcripts from SEC filings (some companies file them as 8-K exhibits).  
For companies that don't file transcripts: Seeking Alpha and Motley Fool publish free transcripts — scrape with respectful rate limiting and robot.txt compliance.

### Extraction Patterns
These are the linguistic patterns associated with consumer/worker harm that appear in investor-facing documents:

```python
EXTRACTION_PATTERNS = {
    "captive_strategy": [
        "captive", "locked in", "sticky", "fully engaged member",
        "cross-sell", "self-referral", "in-network steering",
        "captive network", "preferred network"
    ],
    "labor_cost_extraction": [
        "labor efficiency", "headcount optimization", "workforce rationalization",
        "labor productivity", "right-sizing", "restructuring charges",
        "variable labor model", "contractor conversion"
    ],
    "margin_extraction": [
        "spread compression", "rebate retention", "spread income",
        "take rate", "monetization", "capture rate"
    ],
    "regulatory_arbitrage": [
        "regulatory environment", "regulatory flexibility",
        "light-touch regulation", "favorable regulatory",
        "offshore", "restructuring for regulatory"
    ]
}
```

### Key Functions
```python
# cam/analysis/earnings_nlp.py

def score_transcript(transcript_text: str) -> TranscriptScore:
    """
    Returns:
        TranscriptScore(
            overall_score: float,
            pattern_hits: dict[str, list[dict]],  # pattern → [{text, context, score}]
            divergence_score: float | None,        # vs same company's 10-K language
        )
    """

def compute_divergence(
    transcript_text: str,
    regulatory_text: str     # 10-K or proxy text for same period
) -> float:
    """
    Measure semantic divergence between investor-facing and regulatory-facing language
    on the same topics. Higher score = more divergence = higher concern.
    Uses cosine distance between topic-specific embeddings.
    """
```

### Test Requirements
- Fixture transcripts: CVS Health 2023 earnings call (captive strategy language), plus 2 neutral transcripts
- Test that captive strategy patterns fire on CVS fixture
- Test that divergence score is higher for CVS than neutral companies
- Test graceful handling of transcripts with non-standard formatting

### Acceptance Criteria
- [ ] Pattern detection precision > 0.80 (low false positives prioritized over recall)
- [ ] Divergence scoring produces interpretable, consistent results
- [ ] Transcripts older than 2 years are archived to cold storage

---

## M9 — Proxy Statement Parser

### Goal
Extract structured data from DEF 14A proxy statements: shareholder proposals and vote outcomes, say-on-pay vote results, executive compensation structure, and related-party transactions.

### Key Data Points
```python
@dataclass
class ProxyData:
    entity_id: UUID
    filing_date: date
    say_on_pay_pct: float | None        # % votes FOR executive compensation
    shareholder_proposals: list[ProposalData]
    executive_comp_total: float | None  # total CEO compensation USD
    median_worker_pay: float | None     # CEO pay ratio denominator
    ceo_pay_ratio: float | None

@dataclass  
class ProposalData:
    topic: str                  # classified topic
    proponent: str              # who filed it
    vote_for_pct: float
    vote_against_pct: float
    passed: bool
    management_recommendation: str  # 'FOR' or 'AGAINST'
    management_opposed: bool        # proponent and management on opposite sides
```

### Key Functions
```python
# cam/analysis/proxy_parser.py

def parse_proxy(filing_text: str, filing_date: date) -> ProxyData:
    """
    Parse DEF 14A filing. Use regex + structural parsing for tabular vote data.
    Use NLP for topic classification of proposal text.
    """

def classify_proposal_topic(proposal_text: str) -> str:
    """
    Classify shareholder proposal into topic categories.
    Topics: 'worker_welfare', 'environmental', 'executive_pay', 
            'supply_chain', 'diversity', 'political_spending', 'other'
    """

def flag_escalating_minority(
    entity_id: UUID, 
    topic: str,
    years: int = 3
) -> bool:
    """
    Returns True if a proposal on this topic has received increasing vote support
    over the past N years without passing. This is the key 'shift left' signal:
    growing institutional investor concern before it becomes public crisis.
    """
```

### Test Requirements
- Fixture proxy statements for 3 companies: one with failed say-on-pay, one with escalating minority votes on worker welfare, one clean
- Test vote percentage extraction from common table formats (proxy tables are inconsistent)
- Test escalating minority detection on synthetic 3-year series
- Test that management-opposed proposals are correctly flagged

### Acceptance Criteria
- [ ] Vote percentage extraction accuracy > 90% on 50-proxy test set
- [ ] Say-on-pay votes below 70% generate a signal record automatically
- [ ] Proposals with 3-year escalating support generate a signal record

---

## M10 — HSR Merger Screener

### Goal
When a significant merger is announced, automatically score it for vertical integration risk. Flag transactions where the acquirer controls a bottleneck input that the target's competitors depend on.

### Data Source
FTC/DOJ merger press releases: 
- `https://www.ftc.gov/news-events/news/press-releases` (filter for merger review)
- `https://www.justice.gov/atr/press-releases` (filter for HSR)
Monitor these via RSS feeds and/or weekly scraping.

### Vertical Integration Risk Signals
```python
VERTICAL_RISK_FACTORS = {
    "controls_bottleneck_input": 2.0,      # Acquirer already provides essential service to target's competitors
    "payer_plus_provider": 1.5,            # Combines who pays with who provides (insurance + healthcare)
    "platform_plus_seller": 1.5,           # Marketplace operator acquiring marketplace participant
    "price_setter_plus_competitor": 2.0,   # Entity that sets prices also competes at that price level
    "high_hhi_either_market": 1.0,         # HHI > 2500 in either pre-merger market
    "prior_vertical_merger_same_firm": 1.0 # Acquirer has made prior vertical acquisitions in same space
}
```

### Key Functions
```python
# cam/analysis/merger_screener.py

def score_merger(
    acquirer_entity_id: UUID,
    target_description: str,
    deal_description: str
) -> MergerRiskScore:
    """
    Returns:
        MergerRiskScore(
            score: float,                       # 0.0 to 1.0
            risk_factors_present: list[str],
            market_overlap_description: str,
            comparable_past_cases: list[str],   # Links to similar reviewed mergers
            recommended_review_focus: str       # Plain-language flag for reviewers
        )
    """
```

### Test Requirements
- Test cases: CVS/Aetna (should score high), a horizontal deal in an unconcentrated market (should score low), a conglomerate deal (should score moderate)
- Test that the acquirer's prior merger history is incorporated
- Test that score is explainable (every component traceable to evidence)

### Acceptance Criteria
- [ ] Scores CVS/Aetna-equivalent deals in top quartile (score > 0.7)
- [ ] False positive rate < 20% on a test set of 30 labeled historical mergers
- [ ] Output is human-readable and suitable for regulatory memo

---

## M11 — WARN Act Ingestion

### Goal
Ingest WARN Act filings (mass layoff/plant closure notices) from state labor departments. Cross-reference with known PE ownership to measure whether PE-owned firms generate WARN events at elevated rates.

### Data Sources
WARN Act filings are maintained by individual states, not a federal database. Priority states by volume: CA, TX, NY, FL, IL, OH, PA, MI.  
Most states publish CSV or PDF lists on their labor department websites.  
State URLs documented in `cam/ingestion/warn/state_urls.py` (to be maintained manually as pages change).

### Known Challenge
Some states publish PDFs, not CSVs. Use `pdfplumber` for PDF extraction. Flag states with PDF-only data for manual review if extraction confidence is low.

### Key Functions
```python
# cam/ingestion/warn.py

def ingest_state(state_code: str, since_date: date = None) -> IngestResult:
    """Ingest WARN filings for a single state."""

def ingest_all_states(since_date: date = None) -> list[IngestResult]:
    """Ingest all configured states. Runs in parallel with thread pool."""

def get_pe_owned_entities() -> list[UUID]:
    """
    Return entity IDs flagged as PE-owned.
    Source: manual curation + Private Equity Stakeholder Project data.
    This list requires ongoing maintenance.
    """
```

### Test Requirements
- Fixture data for at least 3 states in different formats (CSV, HTML table, PDF)
- Test PDF extraction on a real CA WARN PDF
- Test that establishments are correctly linked to parent entities
- Test parallel ingestion doesn't create duplicate records

### Acceptance Criteria
- [ ] 8 priority states ingested with automated refresh
- [ ] Entity resolution rate > 60% for WARN filings (these use establishment names, harder to resolve)
- [ ] PE-owned entity list maintained and queryable

---

## M12 — PE/Bankruptcy Correlator

### Goal
Measure whether PE-owned companies in the database generate WARN Act filings and bankruptcy events at statistically elevated rates compared to non-PE-owned peers in the same industry. Provide evidentiary foundation for regulatory action.

### Data Sources
- WARN Act data from M11
- Bankruptcy filings from PACER (federal court system)
  - PACER bulk data available via CourtListener/RECAP: `https://www.courtlistener.com/api/`
  - Free for non-commercial research use
- PE ownership data: manual curation + Private Equity Stakeholder Project published lists

### Key Functions
```python
# cam/analysis/pe_correlator.py

def compute_pe_warn_rate(
    naics_2digit: str,
    lookback_years: int = 5
) -> PEComparison:
    """
    Returns:
        PEComparison(
            pe_warn_rate: float,            # WARN events per company per year, PE-owned
            non_pe_warn_rate: float,        # Same for non-PE
            rate_ratio: float,              # pe / non_pe
            sample_sizes: dict,
            p_value: float | None,          # Statistical significance if sample large enough
            industry: str
        )
    """

def compute_pe_bankruptcy_rate(naics_2digit: str, 
                                lookback_years: int = 5) -> PEComparison:
    """Same structure as above but for bankruptcy events."""

def flag_pe_entity_for_monitoring(entity_id: UUID) -> None:
    """Mark an entity as PE-owned and initiate enhanced monitoring."""
```

### Test Requirements
- Synthetic dataset with known PE/non-PE split and known outcome rates for statistical tests
- Test that rate ratio computation is correct with edge cases (zero non-PE events)
- Test p-value computation is correct (use scipy.stats)
- Test that industries with insufficient sample size return None for p-value rather than spurious results

### Acceptance Criteria
- [ ] Rate ratios computed for all 2-digit NAICS codes with > 10 PE entities
- [ ] Statistical significance reported where sample size permits
- [ ] Output formatted as citable summary table for regulatory or congressional use

---

## M13 — Alert Scoring Engine

### Goal
Combine all signals into per-entity alert scores on a rolling basis. Write scores to `alert_scores` table. Generate structured alert records when thresholds are crossed.

### Score Composition
```python
# cam/alerts/scorer.py

ALERT_THRESHOLDS = {
    "watch":    0.40,   # Worth monitoring; no action required
    "elevated": 0.65,   # Assign to analyst for review
    "critical": 0.80    # Escalate; consider regulatory referral
}

COMPONENT_WEIGHTS = {
    "cross_agency_composite":   0.35,   # From M6
    "risk_language_expansion":  0.20,   # From M7
    "earnings_divergence":      0.15,   # From M8
    "proxy_escalation":         0.15,   # From M9
    "merger_vertical_risk":     0.10,   # From M10
    "pe_warn_flag":             0.05    # From M12
}

def compute_entity_score(entity_id: UUID, 
                          score_date: date) -> AlertScore:
    """Compute composite score for one entity as of score_date."""

def run_daily_scoring() -> list[AlertScore]:
    """Scheduled task: score all active entities. Write to alert_scores table."""

def generate_alert(entity_id: UUID, 
                   score: AlertScore,
                   prior_score: AlertScore | None) -> Alert | None:
    """
    Generate an alert if:
    - Score crosses a threshold for the first time, OR
    - Score level increases (watch → elevated, elevated → critical)
    Returns None if no threshold crossed.
    """
```

### Alert Record Schema
```python
@dataclass
class Alert:
    entity_id: UUID
    canonical_name: str
    alert_level: str            # 'watch', 'elevated', 'critical'
    score: float
    score_date: date
    prior_score: float | None
    threshold_crossed: str      # which threshold triggered this alert
    component_breakdown: dict   # per-component scores
    top_evidence: list[str]     # 3-5 most significant evidence strings
    suggested_action: str       # plain-language action recommendation
    relevant_regulatory_body: list[str]  # which agencies should be notified
```

### Test Requirements
- Test score composition with all components present
- Test score composition with missing components (graceful degradation)
- Test alert generation logic: only fires on threshold crossing, not on every score update
- Test that alert records contain enough context to be actionable without querying DB

### Acceptance Criteria
- [ ] Scores computed daily for all entities with at least one signal in any source
- [ ] Alert generation produces zero duplicate alerts for same entity/threshold in same week
- [ ] All alerts are human-readable without additional context

---

## M14 — Output Layer

### Goal
Export scored data as static JSON files and a minimal HTML dashboard, served from S3/CDN, GitHub Pages, or any static host. The audience is regulatory staff, congressional committee researchers, and investigative journalists. No live database connection is required at serve time — data is exported once per day after the scoring run completes.

### Components

#### Static Data Export

`export_static_site(output_dir: str | Path, *, db: Session) -> dict[str, int]`

Reads from `alert_scores`, `entities`, and `signals` tables and writes a self-contained directory of JSON files:

```
{output_dir}/
├── meta.json                    # export timestamp, entity count, alert count
├── alerts.json                  # all alerts sorted by severity then date desc
├── entities.json                # all entities with current score summaries
└── entities/
    └── {entity_id}.json         # per-entity: score history, component breakdown, evidence
```

File schemas:

| File | Contents |
|------|----------|
| `meta.json` | `{exported_at, entity_count, alert_count, version}` |
| `alerts.json` | Array of alert objects sorted critical → elevated → watch, then date desc |
| `entities.json` | Array of `{id, canonical_name, composite_score, alert_level, score_date}` |
| `entities/{id}.json` | Full record: score timeline, per-component breakdown, top evidence, naics_code |

Re-running export is **idempotent** — files are written atomically (write to temp, then rename) so partial exports are never served.

#### Static HTML Dashboard

Minimal vanilla-JS HTML pages that load the exported JSON via `fetch()`. No build step, no bundler, no framework. Works from `file://` URI or any CDN (S3 + CloudFront, GitHub Pages).

Priority views:

1. **index.html** — Alert feed sorted by severity (critical first), links to entity detail
2. **entity.html?id={uuid}** — Score timeline, component breakdown, source evidence with links
3. **industries.html** — All entities grouped by 2-digit NAICS, sorted by composite score

#### Weekly Digest

`export_digest(since_date: date, *, db: Session) -> str`

Plaintext email digest (SMTP via `cam/config.py`) summarizing:
- New critical/elevated alerts since `since_date`
- Sectors with rising aggregate scores
- Mergers scored above 0.6 in the look-back period

### Key Functions

```python
def export_static_site(
    output_dir: str | Path,
    *,
    db: Session,
) -> dict[str, int]:
    """Export all scored data to a directory of static JSON files.

    Returns a summary dict: {entities, alerts, files_written}.
    """

def export_digest(
    since_date: date,
    *,
    db: Session,
) -> str:
    """Return plaintext weekly digest body (does not send; caller handles SMTP)."""
```

### Test Requirements
- `export_static_site` writes all required files to a `tmp_path`; each file is valid JSON
- `entities/{id}.json` is self-contained (all required fields present without further DB queries)
- `alerts.json` is sorted correctly: critical before elevated before watch, then date descending
- `meta.json` contains correct `entity_count` and `alert_count`
- Digest text includes entities at elevated/critical; excludes below-watch entities
- Idempotency: calling `export_static_site` twice with same data overwrites cleanly (no duplicates, no leftover files from prior run)
- Performance: export completes in < 60 seconds for 10,000 entities (benchmark test)

### Acceptance Criteria
- [ ] `export_static_site` writes all four file types with valid JSON in < 60 s for 10 K entities
- [ ] `entities/{id}.json` is fully self-contained (no live DB query at serve time)
- [ ] Static HTML pages load and render correctly from `file://` URI (no server required)
- [ ] Weekly digest email sends successfully via SMTP and contains evidence for each entity listed
- [ ] Re-running export is idempotent (same data → same files; no partial writes visible)

---

## Testing Standards (All Modules)

### No Live API Calls in Tests
All external HTTP calls must be intercepted using `responses` library or `httpx` mock. Fixture files live in `tests/fixtures/{source}/`.

### Fixture File Format
```
tests/fixtures/
├── edgar/
│   ├── cvs_health_10k_2023.txt
│   ├── cvs_health_proxy_2023.txt
│   └── edgar_submissions_cvs.json
├── osha/
│   ├── violations_sample_100rows.csv
└── cfpb/
    └── complaints_sample_50rows.json
```

### Coverage Requirements
- Minimum 80% line coverage for all modules
- 100% coverage for scoring and alert generation logic (M13)
- Run `pytest --cov=cam --cov-report=term-missing` in CI

### Performance Tests
Each module must include at least one performance test validating it can handle production-scale data within time bounds documented in its Acceptance Criteria.

---

## Configuration Reference

All configuration via environment variables. Copy `.env.example` to `.env` for local development.

```bash
# Database
DATABASE_URL=postgresql://user:pass@localhost:5432/cam

# Object Store
S3_BUCKET=cam-documents
S3_ENDPOINT=http://localhost:9000   # For local MinIO

# Redis
REDIS_URL=redis://localhost:6379/0

# SEC EDGAR (required - your contact email for User-Agent header)
EDGAR_USER_AGENT=your-project@your-org.org

# CFPB (no auth required for public API)
# EPA ECHO (no auth required)
# DOL (no auth required for public data)

# NLP Models
NLP_MODEL_DIR=./models              # Local model cache
NLP_DEVICE=cpu                      # or 'cuda' if GPU available

# Alert Thresholds (override defaults)
ALERT_THRESHOLD_WATCH=0.40
ALERT_THRESHOLD_ELEVATED=0.65
ALERT_THRESHOLD_CRITICAL=0.80

# Entity Resolution
ENTITY_FUZZY_THRESHOLD=0.85
ENTITY_REVIEW_THRESHOLD=0.65

# Output
API_AUTH_TOKEN=change-me-in-production
DIGEST_EMAIL_TO=alerts@your-org.org
SMTP_HOST=localhost
SMTP_PORT=587
```

---

## Implementation Sequence

Work in this order. Each phase should be fully tested before proceeding.

**Phase 1 — Foundation (M0 + M1)**  
Scaffolding and entity resolution. Nothing else works without entity resolution. Expect this to take longer than it looks.

**Phase 2 — Data Ingestion (M2 + M3 + M4 + M5 in parallel)**  
All four ingestion modules can be developed in parallel by different contributors after M1 is done.

**Phase 3 — First Aggregation (M6)**  
Cross-agency aggregation. First point where the system produces meaningful composite output. Validate against known cases (e.g., companies that were later investigated should score elevated).

**Phase 4 — NLP Signals (M7 + M8 + M9 in parallel)**  
NLP modules can run in parallel. Each depends only on M2 (EDGAR data).

**Phase 5 — Specialized Modules (M10 + M11 + M12 in parallel)**  
Merger screener, WARN ingestion, PE correlator.

**Phase 6 — Alert Engine (M13)**  
Depends on all analysis modules being at least partially functional.

**Phase 7 — Output (M14)**  
Dashboard and API. Can begin scaffolding in parallel with Phase 5, but requires M13 for real data.

---

## Known Limitations and Future Work

**Private companies**: The system has limited visibility into PE-owned and other private companies. WARN Act and bankruptcy data partially address this but the gap is structural. Future work: expand UCC filing monitoring and pension fund FOIA request tracking.

**Supply chain tier 2+**: The system monitors direct employers and their regulatory records, not upstream suppliers. Future work: integrate Panjiva/ImportGenius customs data and map to entity graph.

**International**: All data sources are US-centric. Future work: integrate EU CSRD disclosures when mandated reporting matures, and expand Violation Tracker global data.

**Entity resolution drift**: As companies merge, acquire subsidiaries, and rename, the entity resolution table requires ongoing maintenance. Assign a maintainer role.

**Model drift**: NLP models and keyword patterns require periodic re-evaluation as corporate language evolves. Schedule annual review of EXTRACTION_PATTERNS and RISK_TOPICS.

**Political context**: Regulatory bodies' propensity to act on alerts varies with administration. The system surfaces signals; it cannot control whether they are acted upon. Documented here so future maintainers understand the system's limits.
