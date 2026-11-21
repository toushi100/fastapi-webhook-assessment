import hashlib
import re

PII_PATTERNS = [
    re.compile(r"\b\d{3}-\d{2}-\d{4}\b"),
    re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b"),
    re.compile(r"\b(?:\d[ -]*?){13,16}\b"),
]


def scrub_and_fingerprint(raw_text: str, blob_iterations: int = 120_000) -> tuple[str, str]:
    """
    Redacts common PII patterns and computes a deterministic cryptographic fingerprint.
    CPU-bound (~40-60ms per invocation depending on host CPU).
    """
    cleaned = raw_text
    for pattern in PII_PATTERNS:
        cleaned = pattern.sub("[REDACTED]", cleaned)

    digest = cleaned.encode("utf-8")
    for _ in range(blob_iterations):
        digest = hashlib.sha256(digest).digest()

    return cleaned, digest.hex()
