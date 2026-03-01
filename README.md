# Corporate Accountability Monitor

## What This Is

The Corporate Accountability Monitor (CAM) is an open-source system for detecting early warning signals of corporate behavior that harms workers, consumers, or the environment — ideally before those harms become visible through scandal, congressional investigation, or regulatory enforcement.

Most public scrutiny of corporate misbehavior is reactive. A company gets investigated by Congress, fined by the EPA, or profiled in an investigative piece, and the response is: why didn't we know sooner? The answer, usually, is that the signals were there — in regulatory databases, SEC filings, shareholder meeting records, earnings call transcripts — but nobody was watching them systematically or connecting them across sources.

This project exists to do that watching systematically.

---

## The Problem We're Solving

Current business accountability works something like this: a worker gets hurt, a consumer gets defrauded, or a river gets polluted. If the harm is severe enough and visible enough, it attracts a journalist, a regulator, or a legislator. Months or years later, there might be a fine, a hearing, or a headline.

The problem is not primarily a lack of information. In the United States, a surprising amount of corporate behavior leaves public traces — in OSHA inspection records, EPA enforcement databases, SEC filings, CFPB complaint data, court records, and shareholder meeting documents. The problem is that this information is scattered across dozens of agencies, maintained in incompatible formats, and almost never synthesized in ways that reveal patterns across companies or over time.

Three specific gaps stand out:

**Fragmentation**: A company might be accumulating OSHA violations, EPA enforcement actions, and CFPB consumer complaints simultaneously, but no single institution sees the composite picture. Each agency sees its slice. Congressional investigations often surface the multi-agency pattern too late.

**Reactive detection**: Most monitoring is episodic — triggered by a specific event rather than by systematic tracking of behavioral indicators over time. The earliest, most actionable signals often appear in filings and documents that exist but aren't being read.

**Opacity of private ownership**: Much of the most extractive corporate behavior — particularly leveraged buyouts by private equity firms that load debt onto acquired companies — occurs in ownership structures with minimal disclosure requirements, making it systematically harder to detect.

CAM addresses the first two gaps directly and partially addresses the third through creative use of existing public records.

---

## How It Works

The system connects five categories of public data, normalizes them to a common company identifier, and produces composite risk scores designed to surface the patterns that precede regulatory action and congressional investigation.

**Regulatory violations** from OSHA (workplace safety), EPA (environmental enforcement), CFPB (consumer financial protection), and NLRB (labor relations) are aggregated per company and benchmarked against industry peers. A company whose injury rate is three times its sector average is a different kind of signal than one with a single violation.

**SEC filings** are analyzed in two ways. Annual reports (10-K filings) are compared year-over-year to detect expanding risk factor language — when a company's own lawyers write more about labor disputes, regulatory investigations, or environmental liability, it typically means something has changed internally. Proxy statements reveal shareholder proposals and vote outcomes, which are some of the best leading indicators available: institutional investors often surface governance concerns months before media coverage.

**Earnings calls and investor documents** are scanned for language that companies use with Wall Street but not with regulators — terms like "captive strategy," "locked-in customers," "workforce rationalization," and "margin capture" that signal value extraction. When this language diverges significantly from what the same company tells regulators in its SEC filings, it is a meaningful signal.

**Merger announcements** are scored for vertical integration risk — specifically, whether an acquisition would give the buyer control over a bottleneck input that competitors depend on. The CVS/Aetna merger (insurer + pharmacy benefit manager + pharmacy) is the canonical example of a transaction whose consumer harm implications were visible at announcement but not flagged adequately in regulatory review.

**WARN Act filings and bankruptcy records** are correlated with private equity ownership to measure whether PE-owned companies generate mass layoff notices and bankruptcy events at elevated rates compared to non-PE peers in the same industry.

The outputs are composite risk scores per company, updated on a rolling basis, with structured alerts when scores cross thresholds. Scores are decomposed into their contributing signals so that the evidence behind any alert is immediately visible and citable.

---

## Design Decisions

Several choices made in building this system reflect lessons learned from studying how existing accountability mechanisms work and where they fail.

**Normalizing to industry benchmarks rather than absolute counts.** A company with ten OSHA violations means something different in a high-hazard manufacturing sector than in an office services firm. Every violation count and penalty total is expressed relative to the industry average (using NAICS codes from OSHA records). This makes the system useful for comparison; raw counts are not.

**Weighting multi-agency signal overlap non-linearly.** The single strongest empirical predictor of congressional investigation is simultaneous signals across multiple regulatory agencies. A company with active signals at OSHA, EPA, and CFPB at the same time is more concerning than the sum of its individual signals would suggest. The composite scoring explicitly bonuses for this overlap rather than treating each agency's signal as independent.

**Prioritizing low false positives over high recall.** The system's output goes to regulatory staff and researchers who have limited time. A system that produces too many spurious alerts quickly gets ignored. We've calibrated thresholds toward precision: it's better to surface ten genuine concerns than fifty speculative ones. The "watch" tier exists precisely to capture signals that don't meet elevated thresholds but warrant a second look.

**Keeping NLP evidence visible and citable.** Every NLP-derived signal — risk language expansion, earnings call pattern hits, proxy proposal classification — preserves the specific text that triggered it. Scores without evidence are not actionable. A regulatory memo or congressional hearing preparation document needs to cite specific language, specific filings, specific dates.

**Building on existing authority rather than requiring new regulation.** All data sources in this system are already public or required by existing law. The system is designed to help institutions that already have investigative authority act faster and with better-organized information — not to create new regulatory mechanisms. This is both a practical choice (the system can be built and used now) and a principled one (the bottleneck in corporate accountability is rarely the absence of authority; it's the absence of systematically organized signal).

**Separating entity resolution as its own module.** The hardest technical problem in aggregating corporate data across sources is that the same company appears under dozens of different names — subsidiaries, DBAs, name changes, ticker symbols, EIN variations. Getting this wrong means signals about the same company don't get connected. The entity resolution module is built first and treated as critical infrastructure. It is designed to be conservative: ambiguous matches go to a human review queue rather than being silently merged or dropped.

**Acknowledging the private company gap explicitly.** The system works best for public companies in regulated industries. Private equity-owned companies are deliberately opaque, and the system captures only partial signals for them (WARN Act filings, bankruptcy records, UCC liens). This limitation is documented in the plan rather than papered over, because future maintainers need to understand it and because the political and regulatory work required to close it is genuinely distinct from the technical work of building this system.

---

## What This Is Not

**Not a replacement for regulatory judgment.** The system surfaces signals; it does not make enforcement decisions. A high score means "this warrants a closer look," not "this company is guilty of something." Every alert includes evidence, but evidence requires interpretation.

**Not a financial product.** Scores are not designed to predict stock price movements or serve as investment signals. The harms being detected — to workers, consumers, and communities — often don't show up in near-term financial performance, which is precisely why markets under-discipline them.

**Not a surveillance system for employees.** The data sources are all company-level regulatory and financial disclosures, not individual worker data. The system helps surface corporate patterns; it is not designed to identify individual workers or whistleblowers.

**Not a substitute for investigative journalism.** Systematic monitoring is complementary to investigative reporting, not a replacement for it. The system can identify patterns and prioritize where investigative attention is warranted; it cannot do the qualitative work of understanding context, interviewing sources, or verifying claims.

---

## Who This Is For

**Regulatory staff** at agencies like the FTC, DOJ Antitrust Division, OSHA, EPA, and CFPB who want better-organized early warning signals without building bespoke monitoring infrastructure for each data source.

**Congressional committee staff** at committees like Senate HELP, Senate Banking, House Energy and Commerce, and the Permanent Subcommittee on Investigations, who need evidentiary foundations for oversight activities.

**Investigative journalists** covering corporate accountability who want to identify companies worth investigating and have a documented, citable evidence base.

**Academic researchers** studying corporate governance, labor markets, environmental compliance, and antitrust who need a structured dataset linking violations, governance signals, and financial disclosures.

**Advocacy organizations** like Good Jobs First, the Economic Policy Institute, and the Private Equity Stakeholder Project who currently do similar work manually and would benefit from automation.

---

## Getting Started

See `PLAN.md` for the complete implementation specification. Work proceeds in phases:

1. Project scaffolding and entity resolution (foundation)
2. Data ingestion from OSHA, EPA, CFPB, and EDGAR (parallel development)
3. Cross-agency aggregation (first meaningful output)
4. NLP signal extraction from 10-K filings, earnings calls, and proxy statements
5. Specialized modules for merger screening, WARN Act tracking, and PE correlation
6. Alert scoring and output layer

Each module in `PLAN.md` defines its inputs, outputs, acceptance criteria, and test requirements. No live API calls in tests. All thresholds and weights configurable via environment variables.

---

## Limitations and Honest Caveats

The most extractive corporate behavior tends to occur in ownership structures with the least disclosure obligation. Private equity in particular is deliberately opaque, and this system can only partially address that. Closing the gap between where harm concentrates and where disclosure requirements exist is ultimately a regulatory and legislative challenge, not a technical one.

The system can also be gamed: a sufficiently motivated company can write risk factors that obscure emerging problems, scrub incriminating language from investor calls before filing transcripts, and structure subsidiary relationships to reduce the signal strength of regulatory violations. The system is designed to be harder to game than the current status quo, not impossible to game.

Finally, the system surfaces signals for institutions that already have investigative authority. Whether those institutions act depends on political conditions — administrative priorities, resource levels, lobbying pressure — that the system cannot control. It can improve the velocity and quality of information available to those institutions; it cannot substitute for the political will to act on that information.

---

## Contributing

Contributions welcome. Priority areas:

- State WARN Act scrapers (more states, better PDF extraction)
- Entity resolution improvements (better subsidiary mapping, international entities)
- NLP pattern libraries (domain experts in healthcare, finance, and labor improve precision)
- Visualization and dashboard work
- Documentation of data source behavior and gotchas

Please read `PLAN.md` before contributing to understand the module architecture and testing standards.

---

## Data Sources

All data sources are US government public records or SEC-mandated disclosures.

| Source | Content | Update Frequency |
|--------|---------|------------------|
| SEC EDGAR | 10-K, DEF 14A, 8-K, S-4 filings | Continuous |
| OSHA Enforcement | Inspection records, violations, penalties | Quarterly bulk + weekly delta |
| EPA ECHO | Environmental enforcement actions | Weekly |
| EPA TRI | Toxic release self-reports | Annual |
| CFPB Complaints | Consumer financial complaints | Daily |
| DOL WARN Act | Mass layoff notices (state-level) | Varies by state |
| PACER/CourtListener | Federal bankruptcy filings | Continuous |
| FTC/DOJ Press Releases | Merger review announcements | As published |

---

## License

Apache 2.0. Use freely, including for commercial purposes. Attribution appreciated. Do not use to harm workers, consumers, or communities — that would rather miss the point.
