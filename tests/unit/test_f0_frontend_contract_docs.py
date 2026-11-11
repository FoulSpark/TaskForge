"""Automated contract and link verification for Phase 10 Gate F0 deliverables."""

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
PHASE10_DIR = REPO_ROOT / "docs" / "phases" / "phase_10"

F0_DOCS = [
    PHASE10_DIR / "PHASE_10_FRONTEND_SPEC.md",
    PHASE10_DIR / "FRONTEND_API_CONTRACT.md",
    PHASE10_DIR / "FRONTEND_SECURITY_MODEL.md",
    PHASE10_DIR / "FRONTEND_DESIGN_SYSTEM.md",
    PHASE10_DIR / "F0_ACCEPTANCE_CRITERIA.md",
    PHASE10_DIR / "README.md",
]
DIAGRAM_PATH = PHASE10_DIR / "diagrams" / "f0_frontend_architecture.mmd"


def _github_heading_slug(heading_text: str) -> str:
    slug = heading_text.strip().lower()
    slug = re.sub(r"[^\w\s-]", "", slug)
    slug = re.sub(r"\s", "-", slug)
    return slug


def _extract_heading_slugs(md_text: str) -> set[str]:
    slugs: set[str] = set()
    in_fence = False
    for line in md_text.splitlines():
        stripped = line.strip()
        if stripped.startswith("```"):
            in_fence = not in_fence
            continue
        if in_fence:
            continue
        match = re.match(r"^#{1,6}\s+(.+)$", stripped)
        if match:
            slugs.add(_github_heading_slug(match.group(1)))
    return slugs


def test_f0_deliverable_files_exist():
    """Verify all Gate F0 documents and Mermaid architecture source exist."""
    for doc_path in F0_DOCS:
        assert doc_path.is_file(), f"Missing Gate F0 deliverable: {doc_path}"
    assert DIAGRAM_PATH.is_file(), f"Missing Gate F0 Mermaid diagram: {DIAGRAM_PATH}"
    diag = DIAGRAM_PATH.read_text(encoding="utf-8").strip()
    assert diag.startswith("flowchart ")


def test_f0_resolves_all_five_contract_gaps():
    """Verify explicit locked decisions exist for all five pre-F0 audit gaps."""
    spec = (PHASE10_DIR / "PHASE_10_FRONTEND_SPEC.md").read_text(encoding="utf-8")
    for gap_marker in ("**G1**", "**G2**", "**G3**", "**G4**", "**G5**"):
        assert gap_marker in spec, f"Missing explicit decision for gap {gap_marker}"

    assert "/api/v1/auth/me" in spec
    assert "/api/v1/monitoring/summary" in spec
    assert "localStorage" in spec
    assert "lease_token" in spec
    assert "F0" in spec and "F8" in spec


def test_f0_monitoring_contract_prevents_lifecycle_interval_conflation():
    """Ensure FRONTEND_API_CONTRACT.md separates outbox_dwell, queue_dwell, and submission_to_start."""
    contract = (PHASE10_DIR / "FRONTEND_API_CONTRACT.md").read_text(encoding="utf-8")
    for field_name in (
        "outbox_dwell:",
        "queue_dwell:",
        "submission_to_start:",
        "handler_duration:",
        "worker_lease_tenure:",
        "end_to_end:",
    ):
        assert field_name in contract, f"Missing lifecycle metric field '{field_name}' in API contract"

    assert "queue_wait:" not in contract, (
        "Conflated 'queue_wait:' field must not appear in MonitoringSummaryResponse.latencies_ms"
    )
    assert "outbox.published_at - task.created_at" in contract
    assert "attempt.started_at - outbox.published_at" in contract
    assert "attempt.started_at - task.created_at" in contract
    assert "population:" in contract


def test_f0_diagram_matches_embedded_spec_block():
    """Verify embedded Mermaid diagram in PHASE_10_FRONTEND_SPEC.md matches f0_frontend_architecture.mmd."""
    spec = (PHASE10_DIR / "PHASE_10_FRONTEND_SPEC.md").read_text(encoding="utf-8")
    blocks = re.findall(r"```mermaid\n(.*?)\n```", spec, flags=re.DOTALL)
    assert len(blocks) == 1, f"Expected 1 Mermaid block in PHASE_10_FRONTEND_SPEC.md, found {len(blocks)}"

    def norm(raw: str) -> str:
        return "\n".join(
            line.rstrip()
            for line in raw.splitlines()
            if line.strip() and not line.strip().startswith("%%")
        )

    assert norm(blocks[0]) == norm(DIAGRAM_PATH.read_text(encoding="utf-8"))


def test_f0_links_and_cited_backend_paths_resolve():
    """Verify all internal Markdown links, anchors, and cited src/ paths resolve on disk."""
    path_pattern = re.compile(r"\b(src/[A-Za-z0-9_./-]+\.py)\b")

    for doc_path in F0_DOCS:
        content = doc_path.read_text(encoding="utf-8")
        heading_slugs = _extract_heading_slugs(content)
        prose_only = re.sub(r"```.*?```", "", content, flags=re.DOTALL)
        prose_only = re.sub(r"`[^`]+`", "", prose_only)

        for _label, target in re.findall(r"\[([^\]]+)\]\(([^)]+)\)", prose_only):
            if target.startswith(("http://", "https://", "mailto:")):
                continue
            if target.startswith("#"):
                assert target[1:] in heading_slugs, (
                    f"Broken anchor '{target}' in {doc_path.relative_to(REPO_ROOT)}"
                )
                continue
            rel_target, _, anchor = target.partition("#")
            resolved = (doc_path.parent / rel_target).resolve()
            assert resolved.exists(), (
                f"Broken relative link '{target}' in {doc_path.relative_to(REPO_ROOT)}"
            )
            if anchor and resolved.is_file() and resolved.suffix.lower() == ".md":
                target_slugs = _extract_heading_slugs(resolved.read_text(encoding="utf-8"))
                assert anchor in target_slugs, (
                    f"Broken cross-doc anchor '{target}' in {doc_path.relative_to(REPO_ROOT)}"
                )

        for cited_py in sorted(set(path_pattern.findall(content))):
            assert (REPO_ROOT / cited_py).is_file(), (
                f"Cited backend file '{cited_py}' missing (referenced in {doc_path.relative_to(REPO_ROOT)})"
            )

