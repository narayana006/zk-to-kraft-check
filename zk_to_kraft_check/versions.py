"""Kafka release and inter-broker-protocol version parsing."""

from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class KafkaVersion:
    major: int
    minor: int
    patch: int | None = None

    @property
    def line(self) -> tuple[int, int]:
        return (self.major, self.minor)

    @property
    def line_str(self) -> str:
        return f"{self.major}.{self.minor}"

    def __str__(self) -> str:
        return self.line_str if self.patch is None else f"{self.line_str}.{self.patch}"


def parse_kafka_version(text: str) -> KafkaVersion:
    """Parse '3.9.1' or '3.9'. Vendor suffixes are not accepted."""
    match = re.fullmatch(r"(\d+)\.(\d+)(?:\.(\d+))?", text.strip())
    if not match:
        raise ValueError(f"not an Apache Kafka version: {text!r} (expected something like 3.9.1)")
    major, minor, patch = match.groups()
    return KafkaVersion(int(major), int(minor), None if patch is None else int(patch))


@dataclass(frozen=True)
class ProtocolVersion:
    """An inter.broker.protocol.version value such as '3.9' or '3.7-IV2'."""

    major: int
    minor: int
    iv: int | None = None  # None: no -IVn suffix, meaning the newest of that line

    @property
    def line(self) -> tuple[int, int]:
        return (self.major, self.minor)

    def at_least(self, major: int, minor: int, iv: int = 0) -> bool:
        if self.line != (major, minor):
            return self.line > (major, minor)
        return self.iv is None or self.iv >= iv


def parse_protocol_version(text: str) -> ProtocolVersion | None:
    match = re.fullmatch(r"(\d+)\.(\d+)(?:\.\d+)?(?:-IV(\d+))?", text.strip(), flags=re.IGNORECASE)
    if not match:
        return None
    major, minor, iv = match.groups()
    return ProtocolVersion(int(major), int(minor), None if iv is None else int(iv))
