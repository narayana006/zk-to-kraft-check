"""The ordered migration checklist, filled in with what the files show.

The steps follow the "ZooKeeper to KRaft Migration" section of the Apache
Kafka 3.9 operations documentation.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .checks import BLOCKER, BRIDGE_LINE, STANDARD_AUTHORIZER, Facts, Finding
from .versions import KafkaVersion

DONE, TODO, BLOCKED, MANUAL = "done", "todo", "blocked", "manual"
MARKS = {DONE: "[x]", TODO: "[ ]", BLOCKED: "[!]", MANUAL: "[?]"}


@dataclass
class Step:
    number: int
    title: str
    state: str
    notes: list[str] = field(default_factory=list)


def build_checklist(version: KafkaVersion, findings: list[Finding], facts: Facts) -> list[Step]:
    blockers = [f for f in findings if f.severity == BLOCKER]
    version_blocked = any(f.id.startswith(("version-", "ibp-")) for f in blockers)
    total = len(facts.zk_brokers) + len(facts.kraft_brokers)
    in_migration_mode = len(facts.migration_enabled) + len(facts.kraft_brokers)
    converted = len(facts.kraft_brokers)
    line = version.line_str
    steps: list[Step] = []

    def add(title: str, state: str, *notes: str) -> None:
        steps.append(Step(len(steps) + 1, title, state, [n for n in notes if n]))

    target = line if version.line >= (3, 7) and version.major < 4 else ".".join(map(str, BRIDGE_LINE))
    add(
        f"Run every broker on Kafka {target}.x with inter.broker.protocol.version={target}",
        BLOCKED if version_blocked else DONE,
        "Do not change the protocol version again until the migration is finalized.",
        "Only the files you passed were checked; make sure they cover every broker.",
    )
    other = [f for f in blockers if not f.id.startswith(("version-", "ibp-"))]
    add(
        "Clear the configuration blockers listed above",
        BLOCKED if other else DONE,
        *[f"{f.title}" for f in other],
    )
    add(
        "Find the cluster id",
        MANUAL,
        f"bin/zookeeper-shell.sh {facts.zookeeper_connect or '<zookeeper.connect>'} get /cluster/id",
        "It is also in meta.properties in each broker's log directory.",
    )
    ids = sorted(set(facts.broker_ids.values()))
    add(
        "Provision the KRaft controller quorum (three or more dedicated controllers)",
        MANUAL,
        "Format them with the cluster id from the previous step.",
        "Each controller needs zookeeper.metadata.migration.enable=true, zookeeper.connect and the "
        "brokers' inter.broker.listener.name, besides the usual KRaft controller settings.",
        f"node.id values must not collide with broker ids in use: {', '.join(map(str, ids))}." if ids else
        "node.id values must not collide with any broker.id.",
        "Recommended: log org.apache.kafka.metadata.migration at TRACE on the controllers.",
    )
    quorum_key = "controller.quorum.bootstrap.servers" if version.line >= BRIDGE_LINE else "controller.quorum.voters"
    add(
        "Put the brokers in migration mode, one rolling restart",
        DONE if total and in_migration_mode == total else TODO,
        f"{in_migration_mode} of {total} broker file(s) already have this.",
        f"Add: zookeeper.metadata.migration.enable=true, {quorum_key}, controller.listener.names, "
        "and the controller listener in listener.security.protocol.map.",
        "The metadata migration starts by itself when the last broker has been restarted.",
    )
    add(
        "Wait for the metadata migration to complete",
        MANUAL,
        "Look for 'Completed migration of metadata from Zookeeper to KRaft' at INFO level in the "
        "active controller's log.",
    )
    convert_notes = [
        f"{converted} of {total} broker file(s) already have process.roles.",
        "Per broker: replace broker.id with node.id (same number), add process.roles=broker, remove "
        "zookeeper.* settings, zookeeper.metadata.migration.enable and inter.broker.protocol.version.",
    ]
    if facts.control_plane_listener:
        convert_notes.append("Also remove control.plane.listener.name.")
    if facts.acl_authorizer:
        convert_notes.append(f"Also set authorizer.class.name={STANDARD_AUTHORIZER}.")
    convert_notes.append("Until the next step is done you can still revert to ZooKeeper mode.")
    add(
        "Restart each broker in KRaft mode, one at a time",
        DONE if total and converted == total else TODO,
        *convert_notes,
    )
    add(
        "Finalize: take the controllers out of migration mode",
        MANUAL,
        "Remove zookeeper.metadata.migration.enable and zookeeper.connect from each controller and "
        "restart them one at a time.",
        "This cannot be undone: after finalizing there is no way back to ZooKeeper mode.",
    )
    add(
        "Decommission ZooKeeper",
        MANUAL,
        "Only if nothing else uses it. The cluster can then be upgraded to Kafka 4.x.",
    )
    return steps
