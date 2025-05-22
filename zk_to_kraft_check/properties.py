"""Minimal reader for Java ``.properties`` files such as ``server.properties``."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class BrokerConfig:
    """One broker's static configuration."""

    name: str
    props: dict[str, str] = field(default_factory=dict)

    def get(self, key: str, default: str | None = None) -> str | None:
        value = self.props.get(key)
        return default if value is None or value == "" else value

    def has(self, key: str) -> bool:
        return self.get(key) is not None

    def get_bool(self, key: str, default: bool = False) -> bool:
        value = self.get(key)
        return default if value is None else value.strip().lower() == "true"

    def get_int(self, key: str) -> int | None:
        try:
            return int(self.get(key, ""))
        except ValueError:
            return None

    def get_list(self, key: str) -> list[str]:
        return [item.strip() for item in (self.get(key) or "").split(",") if item.strip()]


def parse_properties(text: str) -> dict[str, str]:
    """Parse ``key=value`` lines.

    Handles ``#`` and ``!`` comments, ``=`` / ``:`` / whitespace separators and
    backslash line continuations. Unicode escapes are not interpreted. As in
    Java, a later duplicate key overrides an earlier one.
    """
    props: dict[str, str] = {}
    pending = ""
    for raw in text.splitlines():
        line = pending + raw.lstrip()
        pending = ""
        if not line or line[0] in "#!":
            continue
        if line.endswith("\\") and not line.endswith("\\\\"):
            pending = line[:-1]
            continue
        for index, char in enumerate(line):
            if char in "=:" or char.isspace():
                key, value = line[:index], line[index + 1:]
                if char.isspace():
                    value = value.lstrip()
                    if value[:1] in ("=", ":"):
                        value = value[1:]
                break
        else:
            key, value = line, ""
        props[key.strip()] = value.strip()
    return props


def load_broker_config(path: str | Path) -> BrokerConfig:
    path = Path(path)
    return BrokerConfig(name=str(path), props=parse_properties(path.read_text(encoding="utf-8")))
