"""
include/nammamart_masking.py
----------------------------
PII masking shared by the NammaMart ETL (pandas) and ELT (SQL macros mirror these rules).

    mask_email("jane.doe42@example.com")    -> "ja***@example.com"
    mask_phone("9876501234")                 -> "98******34"

Masking, not deletion: analysts can still tell records apart and spot bad formats
without ever seeing the real value. Invalid values are masked too, never passed through.
"""

import pandas as pd


def mask_email(value) -> str:
    if value is None or pd.isna(value):
        return ""
    text = str(value).strip()
    if "@" not in text:
        return "***"  # N/A, missing @: nothing safe to keep
    local, domain = text.rsplit("@", 1)
    return f"{local[:2]}***@{domain}"


def mask_phone(value) -> str:
    if value is None or pd.isna(value):
        return ""
    digits = "".join(ch for ch in str(value) if ch.isdigit())
    if len(digits) < 6:
        return "*" * len(digits)
    return f"{digits[:2]}{'*' * (len(digits) - 4)}{digits[-2:]}"


def mask_customers(df: pd.DataFrame, drop_raw: bool = True) -> pd.DataFrame:
    """Return a copy with `email_masked` / `phone_masked`; raw email, phone and full_name removed."""
    out = df.copy()
    out["email_masked"] = out["email"].map(mask_email)
    out["phone_masked"] = out["phone"].map(mask_phone)
    if drop_raw:
        out = out.drop(columns=["email", "phone", "full_name"], errors="ignore")
    return out
