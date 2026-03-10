import re

PII_PATTERNS = {
    "phone": re.compile(r"\b(\+91|0)?[6-9]\d{9}\b"),
    "email": re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b"),
    "aadhaar": re.compile(r"\b\d{4}\s?\d{4}\s?\d{4}\b"),
    "pan": re.compile(r"\b[A-Z]{5}[0-9]{4}[A-Z]\b"),
    "credit_card": re.compile(r"\b(?:\d[ -]?){13,16}\b"),
    "dob": re.compile(r"\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b"),
}


def mask_pii(text: str, replacement: str = "[REDACTED]") -> str:
    masked = text or ""
    for pattern in PII_PATTERNS.values():
        masked = pattern.sub(replacement, masked)
    return masked


def count_pii_occurrences(text: str) -> dict:
    source = text or ""
    return {name: len(pattern.findall(source)) for name, pattern in PII_PATTERNS.items()}
