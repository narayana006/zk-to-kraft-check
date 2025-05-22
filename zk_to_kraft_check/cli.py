"""Command-line entry point."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict

from . import __version__
from .checklist import MARKS, build_checklist
from .checks import BLOCKER, INFO, WARNING, run_checks
from .properties import load_broker_config
from .versions import parse_kafka_version

EXIT_READY, EXIT_NOT_READY, EXIT_USAGE = 0, 1, 2
LABELS = {BLOCKER: "BLOCKER", WARNING: "WARNING", INFO: "INFO"}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="zk-to-kraft-check",
        description="Check, from broker configuration files, whether a ZooKeeper-based Apache Kafka "
        "cluster is ready to migrate to KRaft. Reads files only; connects to nothing.",
        epilog="Exit codes: 0 no blockers, 1 blockers found (or warnings with --strict), 2 bad input.",
    )
    parser.add_argument("files", nargs="+", metavar="SERVER_PROPERTIES",
                        help="one server.properties file per broker")
    parser.add_argument("--kafka-version", required=True, metavar="X.Y.Z",
                        help="Apache Kafka version the brokers are running, e.g. 3.9.1")
    parser.add_argument("--controller-ids", metavar="ID[,ID...]", default="",
                        help="node.id values planned for the KRaft controllers, to check for clashes")
    parser.add_argument("--json", action="store_true", help="print JSON instead of text")
    parser.add_argument("--strict", action="store_true", help="exit 1 on warnings as well as blockers")
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    return parser


def verdict(findings) -> str:
    severities = {f.severity for f in findings}
    if BLOCKER in severities:
        return "NOT READY"
    return "READY WITH WARNINGS" if WARNING in severities else "READY"


def render_text(version, brokers, findings, steps) -> str:
    counts = {sev: sum(f.severity == sev for f in findings) for sev in (BLOCKER, WARNING, INFO)}
    lines = [
        f"ZooKeeper to KRaft migration readiness: {verdict(findings)}",
        f"  Kafka version   {version} (declared)",
        f"  Broker files    {len(brokers)}",
        f"  Findings        {counts[BLOCKER]} blocker(s), {counts[WARNING]} warning(s), {counts[INFO]} note(s)",
        "",
    ]
    for finding in findings:
        lines.append(f"[{LABELS[finding.severity]}] {finding.title}")
        lines.append(f"    {finding.detail}")
        if finding.brokers:
            lines.append(f"    Files: {', '.join(finding.brokers)}")
        if finding.action:
            lines.append(f"    Fix: {finding.action}")
        if finding.source:
            lines.append(f"    Source: {finding.source}")
        lines.append("")
    lines.append("Migration checklist")
    lines.append("  [x] done   [ ] to do   [!] blocked   [?] cannot be verified from files")
    for step in steps:
        lines.append(f"  {MARKS[step.state]} {step.number}. {step.title}")
        lines += [f"         - {note}" for note in step.notes]
    lines.append("")
    lines.append("Not checked (needs a live cluster): actual broker versions, topic and dynamic configs,")
    lines.append("ACLs, quotas and SCRAM credentials stored in ZooKeeper, and cluster health.")
    return "\n".join(lines)


def main(argv: list[str] | None = None, out=None, err=None) -> int:
    out = out or sys.stdout
    err = err or sys.stderr
    args = build_parser().parse_args(argv)

    try:
        version = parse_kafka_version(args.kafka_version)
    except ValueError as exc:
        print(f"zk-to-kraft-check: error: {exc}", file=err)
        return EXIT_USAGE
    try:
        controller_ids = [int(part) for part in args.controller_ids.split(",") if part.strip()]
    except ValueError:
        print("zk-to-kraft-check: error: --controller-ids must be comma-separated integers, "
              f"got {args.controller_ids!r}", file=err)
        return EXIT_USAGE

    brokers = []
    for path in args.files:
        try:
            brokers.append(load_broker_config(path))
        except (OSError, UnicodeDecodeError) as exc:
            reason = getattr(exc, "strerror", None) or str(exc)
            print(f"zk-to-kraft-check: error: cannot read {path}: {reason}", file=err)
            return EXIT_USAGE
        if not brokers[-1].props:
            print(f"zk-to-kraft-check: error: {path} contains no properties", file=err)
            return EXIT_USAGE

    findings, facts = run_checks(version, brokers, controller_ids)
    steps = build_checklist(version, findings, facts)
    result = verdict(findings)
    failed = result == "NOT READY" or (args.strict and result != "READY")
    code = EXIT_NOT_READY if failed else EXIT_READY

    if args.json:
        payload = {
            "verdict": result,
            "exit_code": code,
            "kafka_version": str(version),
            "brokers": [{"file": b.name, "broker_id": facts.broker_ids.get(b.name)} for b in brokers],
            "findings": [asdict(f) for f in findings],
            "checklist": [asdict(s) for s in steps],
        }
        print(json.dumps(payload, indent=2), file=out)
    else:
        print(render_text(version, brokers, findings, steps), file=out)
    return code


if __name__ == "__main__":
    sys.exit(main())
