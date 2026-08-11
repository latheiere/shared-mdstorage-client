from __future__ import annotations

import re


_STRUCTURAL_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")


def validate_structural_identifier(
    value: str, *, field: str = "identifier"
) -> str:
    """Return a path-safe structural identifier or raise ``ValueError``."""

    if not isinstance(value, str) or _STRUCTURAL_IDENTIFIER.fullmatch(value) is None:
        raise ValueError(
            f"{field} must match ^[A-Za-z0-9][A-Za-z0-9._-]{{0,127}}$"
        )
    return value
