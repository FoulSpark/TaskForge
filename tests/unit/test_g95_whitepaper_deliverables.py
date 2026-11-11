"""Automated structural and contract verification for Gate G9.5 Stage 3 deliverables."""

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
WHITEPAPER_DIR = REPO_ROOT / "docs" / "whitepaper"
WHITEPAPER_PATH = WHITEPAPER_DIR / "ARCHITECTURE.md"
DIAGRAMS_DIR = WHITEPAPER_DIR / "diagrams"

DIAGRAM_FILES = [
    "d1_system_architecture.mmd",
    "d2_task_submission_execution.mmd",
    "d3_lease_fencing_sequence.mmd",
    "d4_outbox_broker_recovery.mmd",
]


def test_whitepaper_and_diagram_files_exist():
    """Verify ARCHITECTURE.md and all four Mermaid diagram source files exist."""
    assert WHITEPAPER_PATH.is_file(), f"Missing whitepaper at {WHITEPAPER_PATH}"
    for fname in DIAGRAM_FILES:
        fpath = DIAGRAMS_DIR / fname
        assert fpath.is_file(), f"Missing Mermaid source file at {fpath}"
        content = fpath.read_text(encoding="utf-8").strip()
        assert content.startswith(("flowchart", "sequenceDiagram")), (
            f"{fname} must begin with a valid Mermaid diagram declaration"
        )


def test_whitepaper_contains_all_13_required_sections():
    """Verify ARCHITECTURE.md contains all 13 sections prescribed in Stage 3 authorization."""
    text = WHITEPAPER_PATH.read_text(encoding="utf-8")
    required_headings = [
        "## 1. Executive Summary",
        "## 2. System Requirements",
        "## 3. Architecture Overview",
        "## 4. Task Lifecycle & State Machine",
        "## 5. Data Architecture",
        "## 6. Queue Architecture",
        "## 7. Reliability & Fault Recovery",
        "## 8. Concurrency Control & Lease Fencing",
        "## 9. Observability & Health Probes",
        "## 10. Security & Access Controls",
        "## 11. Performance Evaluation",
        "## 12. Trade-offs, Delivery Semantics & Limitations",
        "## 13. Deployment & Operations Runbook",
    ]
    for heading in required_headings:
        assert heading in text, f"Missing required whitepaper section heading: {heading}"


def test_whitepaper_preserves_canonical_delivery_guarantee():
    """Verify the verbatim non-negotiable delivery guarantee and idempotency distinction."""
    text = WHITEPAPER_PATH.read_text(encoding="utf-8")
    normalized = " ".join(text.split())

    canonical_guarantee = (
        "TaskForge provides durable task submission through PostgreSQL transactions "
        "and a transactional outbox. Queue delivery is at-least-once. Worker ownership "
        "is protected by leases and fencing tokens. Task completion is persisted "
        "transactionally, and duplicate delivery is handled through idempotent execution "
        "and authoritative task state."
    )
    assert canonical_guarantee in normalized, (
        "Canonical architectural delivery guarantee wording missing from ARCHITECTURE.md"
    )
    assert "Duplicate Message Suppression vs. External Side-Effect Idempotency" in text
    assert "explicitly does NOT claim" in text


def test_whitepaper_provenance_and_r1_terminology():
    """Verify clean provenance separation (1d4a76c vs 2a14600) and R1 Stage 2 metric naming."""
    text = WHITEPAPER_PATH.read_text(encoding="utf-8")
    assert "1d4a76cd047a61548e92e57cfb1f032ba7914068" in text
    assert "2a14600" in text
    assert "a89858b73f8488e05cbcc2393a61f3080ffc7a38" in text
    assert "fc557f14" in text
    assert "Worker Execution / Lease Tenure" in text
    assert "ZPOPMAX" in text


def test_whitepaper_links_and_embeds_all_mermaid_diagrams():
    """Verify ARCHITECTURE.md references all four .mmd files and embeds matching Mermaid blocks."""
    text = WHITEPAPER_PATH.read_text(encoding="utf-8")
    embedded_blocks = re.findall(r"```mermaid\n(.*?)\n```", text, flags=re.DOTALL)
    assert len(embedded_blocks) == 4, (
        f"Expected 4 embedded Mermaid blocks in ARCHITECTURE.md, found {len(embedded_blocks)}"
    )

    def normalize_mmd(raw: str) -> str:
        return "\n".join(
            line.rstrip()
            for line in raw.splitlines()
            if line.strip() and not line.strip().startswith("%%")
        )

    emb_clean_set = {normalize_mmd(block) for block in embedded_blocks}
    for fname in DIAGRAM_FILES:
        assert f"diagrams/{fname}" in text, f"Missing link to diagrams/{fname} in ARCHITECTURE.md"
        src_clean = normalize_mmd((DIAGRAMS_DIR / fname).read_text(encoding="utf-8"))
        assert src_clean in emb_clean_set, (
            f"Embedded Mermaid block matching {fname} not found in ARCHITECTURE.md"
        )


def _github_heading_slug(heading_text: str) -> str:
    """Compute GitHub-flavored Markdown anchor slug from heading text."""
    slug = heading_text.strip().lower()
    slug = re.sub(r"[^\w\s-]", "", slug)
    slug = re.sub(r"\s", "-", slug)
    return slug


def _extract_heading_slugs(md_text: str) -> set[str]:
    """Extract all Markdown heading anchor slugs outside fenced code blocks."""
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


def test_documentation_links_and_referenced_paths_resolve():
    """Verify all internal Markdown links, anchors, and cited repo paths resolve on disk."""
    doc_files = [
        WHITEPAPER_PATH,
        REPO_ROOT / "docs" / "phases" / "phase_09" / "29_g95_stage3_whitepaper_and_diagrams_report.md",
        REPO_ROOT / "docs" / "phases" / "phase_09" / "30_g95_stage3_reconciliation_addendum.md",
        REPO_ROOT / "docs" / "phases" / "phase_09" / "31_phase9_final_synthesis_and_closure.md",
        REPO_ROOT / "docs" / "phases" / "phase_09" / "README.md",
    ]
    path_pattern = re.compile(
        r"\b((?:src|scripts|benchmarks|infrastructure|docs|tests)/[A-Za-z0-9_./-]+|docker-compose\.yml)\b"
    )

    for doc_path in doc_files:
        assert doc_path.is_file(), f"Documentation file missing: {doc_path}"
        content = doc_path.read_text(encoding="utf-8")
        heading_slugs = _extract_heading_slugs(content)

        # 1. Validate Markdown links [label](target) outside fenced/inline code
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
                f"Broken relative link '{target}' (resolved '{resolved}') in {doc_path.relative_to(REPO_ROOT)}"
            )
            if anchor and resolved.is_file() and resolved.suffix.lower() == ".md":
                target_slugs = _extract_heading_slugs(resolved.read_text(encoding="utf-8"))
                assert anchor in target_slugs, (
                    f"Broken cross-doc anchor '{target}' in {doc_path.relative_to(REPO_ROOT)}"
                )

        # 2. Validate cited repository file/directory paths in whitepaper & Stage 3 report
        if doc_path != REPO_ROOT / "docs" / "phases" / "phase_09" / "README.md":
            for cited_path in sorted(set(path_pattern.findall(content))):
                resolved_path = REPO_ROOT / cited_path
                assert resolved_path.exists(), (
                    f"Referenced repo path '{cited_path}' does not exist on disk (cited in {doc_path.relative_to(REPO_ROOT)})"
                )
