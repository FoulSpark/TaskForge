# TaskForge Security Policy

## 1. Security Model

TaskForge is engineered with defense-in-depth principles:

1. **Zero Secret Leakage:** Database credentials, Redis secrets, and API keys are managed exclusively via environment variables (`.env`). Secrets are never committed to version control.
2. **Strict Request Validation:** All inputs are strictly parsed and validated using Pydantic schemas. Unrecognized or dangerous attributes are rejected with HTTP 422.
3. **API Rate Limiting:** Built-in sliding window rate limiting prevents Denial of Service (DoS) and brute-force flooding from malicious clients.
4. **Task Execution Sandboxing:** Registered handlers only execute pre-compiled Python functions. Arbitrary code execution (`eval()` / `exec()`) is strictly prohibited.
5. **Execution Leases & Timeouts:** Tasks are constrained by mandatory execution deadlines to prevent algorithmic denial of service or infinite loop resource exhaustion.

---

## 2. Reporting Security Vulnerabilities

If you discover a security vulnerability within TaskForge, please disclose responsibly:

- **Do NOT** open a public GitHub issue.
- Please email the maintainers with a detailed description, reproduction steps, and proof-of-concept.
- We will acknowledge receipt within 48 hours and work with you on a coordinated fix.
