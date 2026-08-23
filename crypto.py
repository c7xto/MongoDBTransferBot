"""
crypto.py — Fernet-based encryption for sensitive fields persisted to MongoDB.

Currently used for: userbot_session (prescan.py), which is a live Telegram
user-account login credential and must not sit in plaintext in the Master DB.

Key handling:
  - The key is read from the ENCRYPTION_KEY environment variable on first use
    (lazily, not at import time), so processes that never touch an encrypted
    field don't need the var set.
  - The key itself is never included in any log line or exception message
    raised from this module — only the fact that it's missing/invalid.
"""
from __future__ import annotations

import os

from cryptography.fernet import Fernet, InvalidToken

_fernet: Fernet | None = None


def _get_fernet() -> Fernet:
    global _fernet
    if _fernet is not None:
        return _fernet

    key = os.environ.get("ENCRYPTION_KEY", "").strip()
    if not key:
        raise RuntimeError(
            "ENCRYPTION_KEY is not set. Generate one with:\n"
            "  python -c \"from cryptography.fernet import Fernet; "
            "print(Fernet.generate_key().decode())\"\n"
            "and add it to your .env file.")

    try:
        _fernet = Fernet(key.encode())
    except Exception as e:
        raise RuntimeError(
            "ENCRYPTION_KEY is set but is not a valid Fernet key "
            "(must be a 32-byte urlsafe-base64-encoded string).") from e
    return _fernet


def encrypt_str(plaintext: str) -> str:
    """Encrypt a string for storage. Returns a urlsafe-base64 token string."""
    return _get_fernet().encrypt(plaintext.encode()).decode()


def decrypt_str(token: str) -> str:
    """
    Decrypt a token produced by encrypt_str.
    Raises ValueError if the token is tampered, truncated, or was encrypted
    under a different ENCRYPTION_KEY (e.g. after key rotation).
    """
    try:
        return _get_fernet().decrypt(token.encode()).decode()
    except InvalidToken as e:
        raise ValueError(
            "Could not decrypt value — invalid token or ENCRYPTION_KEY mismatch") from e
