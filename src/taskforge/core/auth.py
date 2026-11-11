"""API Key lifecycle and authentication service for Milestone G9.1.

Generates high-entropy random keys with prefix `tf_live_`, stores only salted/SHA-256 digests,
verifies incoming Bearer tokens, and tracks last-used timestamps.
"""

import hashlib
import hmac
import secrets
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from taskforge.database.models import ApiKey


@dataclass(frozen=True)
class AuthenticatedPrincipal:
    key_id: str
    tenant_id: str
    name: str
    scopes: list[str]


def hash_token_secret(key_id: str, secret: str) -> str:
    """Compute deterministic SHA-256 digest of key_id + secret."""
    combined = f"{key_id}:{secret}".encode()
    return hashlib.sha256(combined).hexdigest()


class ApiKeyService:
    def __init__(self, db: Session):
        self.db = db

    def create_api_key(
        self,
        tenant_id: str,
        name: str,
        scopes: list[str] | None = None,
        expires_at: datetime | None = None,
    ) -> tuple[str, ApiKey]:
        """
        Generate a new high-entropy API key.
        Returns the plaintext key (shown ONCE to caller) and the persisted ApiKey row.
        Format: tf_live_<24-hex-key-id>_<32-char-secret>
        Entropy: 24 hex chars (96-bit key-ID) + 32 URL-safe chars (192-bit secret).
        """
        # Use hex for key_id so it never contains underscores
        key_id = secrets.token_hex(12)  # 24 chars hex
        secret = secrets.token_urlsafe(24)[:32]
        plaintext_key = f"tf_live_{key_id}_{secret}"

        hashed_secret = hash_token_secret(key_id, secret)
        assigned_scopes = scopes if scopes is not None else ["tasks:read", "tasks:write"]

        record = ApiKey(
            id=str(uuid.uuid4()),
            key_id=key_id,
            hashed_secret=hashed_secret,
            tenant_id=tenant_id,
            name=name,
            scopes=assigned_scopes,
            is_active=True,
            expires_at=expires_at,
            created_at=datetime.now(UTC),
        )
        self.db.add(record)
        self.db.commit()
        self.db.refresh(record)
        return plaintext_key, record

    def authenticate_token(self, token: str) -> AuthenticatedPrincipal | None:
        """
        Authenticate a Bearer token string.
        Returns AuthenticatedPrincipal if valid, active, and unexpired; None otherwise.
        """
        if not token or not token.startswith("tf_live_"):
            return None

        parts = token[8:].split("_", 1)
        if len(parts) != 2:
            return None

        key_id, secret = parts[0], parts[1]
        record = self.db.scalar(select(ApiKey).where(ApiKey.key_id == key_id))
        if not record:
            return None

        if not record.is_active:
            return None

        now = datetime.now(UTC)
        if record.expires_at:
            exp = (
                record.expires_at.replace(tzinfo=UTC)
                if record.expires_at.tzinfo is None
                else record.expires_at.astimezone(UTC)
            )
            if exp < now:
                return None

        expected_hash = hash_token_secret(key_id, secret)
        if not hmac.compare_digest(record.hashed_secret, expected_hash):
            return None

        # Update last_used_at timestamp
        record.last_used_at = now
        self.db.commit()

        return AuthenticatedPrincipal(
            key_id=record.key_id,
            tenant_id=record.tenant_id,
            name=record.name,
            scopes=list(record.scopes or []),
        )

    def revoke_api_key(self, key_id: str) -> bool:
        """Revoke an active API key immediately."""
        record = self.db.scalar(select(ApiKey).where(ApiKey.key_id == key_id))
        if not record:
            return False
        record.is_active = False
        self.db.commit()
        return True
