"""Secret sanitization utilities for Milestone G9.1.5.

Masks passwords, database URLs, Redis connection strings, and full lease tokens
to ensure no sensitive credentials appear in error responses or logs.
"""

import hashlib
import re

# Regex patterns for credential detection
_DB_URL_PATTERN = re.compile(r"([a-zA-Z0-9\+\-\.]+)://([^:]+):([^@]+)@([^/\:]+)(:\d+)?(/.*)?")
_BEARER_PATTERN = re.compile(r"(Bearer\s+tf_live_[a-zA-Z0-9_-]{24}_)([a-zA-Z0-9_-]{32})")
_LEASE_TOKEN_PATTERN = re.compile(r"(lease-[a-zA-Z0-9_-]{8,})")


def mask_secret_string(text: str) -> str:
    """Mask credentials, passwords, and tokens in a plain text or serialized string."""
    if not text:
        return text

    # Mask database connection passwords
    text = _DB_URL_PATTERN.sub(r"\1://\2:***@\4\5\6", text)

    # Mask bearer token secrets (keep key_id, mask secret)
    text = _BEARER_PATTERN.sub(r"\1********************************", text)

    # Mask lease tokens to 12-char SHA-256 fingerprint
    def _mask_lease(match):
        raw = match.group(1)
        fp = hashlib.sha256(raw.encode()).hexdigest()[:12]
        return f"lease-fp-{fp}"

    text = _LEASE_TOKEN_PATTERN.sub(_mask_lease, text)
    return text
