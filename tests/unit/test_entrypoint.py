"""Unit tests for the CLI entrypoint."""

from __future__ import annotations

import cam.entrypoint


class TestIngestGuardrail:
    """Tests for the guardrail that fails when all sources ingest 0 records."""

    def test_all_sources_zero_returns_nonzero(self, monkeypatch):
        """When every source ingests 0 records, exit code must be non-zero."""

        def fake_ingest_source(source, since, args):
            return 0

        monkeypatch.setattr("cam.entrypoint._ingest_source", fake_ingest_source)
        exit_code = cam.entrypoint.main(["ingest", "--source", "all", "--since", "2025-01-01"])
        assert exit_code != 0

    def test_one_source_with_records_returns_zero(self, monkeypatch):
        """When at least one source ingests records, exit code must be 0."""

        def fake_ingest_source(source, since, args):
            return 5 if source == "osha" else 0

        monkeypatch.setattr("cam.entrypoint._ingest_source", fake_ingest_source)
        exit_code = cam.entrypoint.main(["ingest", "--source", "all", "--since", "2025-01-01"])
        assert exit_code == 0

    def test_exception_in_source_returns_nonzero(self, monkeypatch):
        """When a source raises an exception, exit code must be non-zero."""

        def fake_ingest_source(source, since, args):
            if source == "epa":
                raise ValueError("API down")
            return 0

        monkeypatch.setattr("cam.entrypoint._ingest_source", fake_ingest_source)
        exit_code = cam.entrypoint.main(["ingest", "--source", "all", "--since", "2025-01-01"])
        assert exit_code != 0

    def test_single_source_zero_returns_nonzero(self, monkeypatch):
        """When a single source (--source cfpb) ingests 0, exit code must be non-zero."""

        def fake_ingest_source(source, since, args):
            return 0

        monkeypatch.setattr("cam.entrypoint._ingest_source", fake_ingest_source)
        exit_code = cam.entrypoint.main(["ingest", "--source", "cfpb", "--since", "2025-01-01"])
        assert exit_code != 0

    def test_multiple_sources_with_records_returns_zero(self, monkeypatch):
        """When multiple sources collectively ingest records, exit code must be 0."""

        call_counts = {}

        def fake_ingest_source(source, since, args):
            call_counts[source] = call_counts.get(source, 0) + 1
            return {"osha": 3, "epa": 2, "cfpb": 0, "warn": 0, "edgar": 0}.get(source, 0)

        monkeypatch.setattr("cam.entrypoint._ingest_source", fake_ingest_source)
        exit_code = cam.entrypoint.main(
            ["ingest", "--source", "osha", "epa", "--since", "2025-01-01"]
        )
        assert exit_code == 0
        assert call_counts.get("osha", 0) == 1
        assert call_counts.get("epa", 0) == 1
