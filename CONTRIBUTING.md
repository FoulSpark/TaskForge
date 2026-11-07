# Contributing to TaskForge

Thank you for your interest in contributing to TaskForge!

## Development Principles

Before contributing, please review our core engineering tenets:
1. **Definition of Done:** A feature is only complete when implementation is verified with unit and integration tests, failure modes are tested, and documentation is updated.
2. **Measurement Over Claims:** Performance optimizations must be demonstrated using the 5-run benchmark methodology.
3. **No Unresolved Failures:** Pull requests must pass the complete test suite (`pytest`) and linting (`ruff check`).

## Submitting Pull Requests

1. Fork the repository and create your feature branch:
   ```bash
   git checkout -b feature/your-feature-name
   ```
2. Write automated tests for all new behavior.
3. Verify all tests pass:
   ```bash
   pytest
   ```
4. Verify code formatting and linting:
   ```bash
   ruff check src tests
   ```
5. Commit your changes following conventional commits format (`feat: ...`, `fix: ...`, `docs: ...`).
6. Open a pull request against the `master` branch.

