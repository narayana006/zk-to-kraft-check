"""Readiness rules.

Every rule cites where it comes from. "3.9 docs" means the "ZooKeeper to KRaft
Migration" section of the Apache Kafka 3.9 operations documentation; "4.x docs"
means the "Differences Between KRaft mode and ZooKeeper mode" page; "3.9
KafkaConfig" means a start-up validation in the 3.9 broker source.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .properties import BrokerConfig
from .versions import KafkaVersion, parse_protocol_version

BLOCKER, WARNING, INFO = "blocker", "warning", "info"
SEVERITY_ORDER = {BLOCKER: 0, WARNING: 1, INFO: 2}

# Last release line that can run in ZooKeeper mode, and so the last that can migrate.
BRIDGE_LINE = (3, 9)
# Patch release each line's own documentation says to upgrade to before migrating.
DOCUMENTED_PATCH = {(3, 7): 2, (3, 8): 1, (3, 9): 1}

MIGRATION_DOCS = "3.9 docs: ZooKeeper to KRaft Migration"
DIFF_DOCS = "4.x docs: Differences Between KRaft mode and ZooKeeper mode"

ACL_AUTHORIZER = "kafka.security.authorizer.AclAuthorizer"
STANDARD_AUTHORIZER = "org.apache.kafka.metadata.authorizer.StandardAuthorizer"
DEFAULT_PRINCIPAL_BUILDER = "org.apache.kafka.common.security.authenticator.DefaultKafkaPrincipalBuilder"
DEFAULT_LISTENER_NAMES = {"PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"}
DEFAULT_RESERVED_BROKER_MAX_ID = 1000

# Broker settings that no longer exist in KRaft mode (4.x docs, "Configurations").
REMOVED_IN_KRAFT = [
    "password.encoder.secret",
    "password.encoder.old.secret",
    "password.encoder.keyfactory.algorithm",
    "password.encoder.cipher.algorithm",
    "password.encoder.key.length",
    "password.encoder.iterations",
    "controlled.shutdown.max.retries",
    "controlled.shutdown.retry.backoff.ms",
    "reserved.broker.max.id",
    "broker.id.generation.enable",
    "leader.imbalance.per.broker.percentage",
]


@dataclass
class Finding:
    id: str
    severity: str
    title: str
    detail: str
    action: str | None = None
    source: str | None = None
    brokers: list[str] = field(default_factory=list)


@dataclass
class Facts:
    """What the checklist needs to know about the cluster."""

    zk_brokers: list[BrokerConfig] = field(default_factory=list)
    kraft_brokers: list[BrokerConfig] = field(default_factory=list)
    migration_enabled: list[BrokerConfig] = field(default_factory=list)
    broker_ids: dict[str, int] = field(default_factory=dict)
    zookeeper_connect: str | None = None
    acl_authorizer: list[str] = field(default_factory=list)
    control_plane_listener: list[str] = field(default_factory=list)


def _names(brokers: list[BrokerConfig]) -> list[str]:
    return [b.name for b in brokers]


def check_version(version: KafkaVersion) -> list[Finding]:
    line = version.line
    if version.major >= 4:
        return [Finding(
            "version-no-zookeeper", BLOCKER,
            f"Kafka {version} cannot run in ZooKeeper mode",
            "ZooKeeper mode was removed in Apache Kafka 4.0, and a ZooKeeper-based cluster cannot "
            "be upgraded straight to 4.x.",
            "Run the migration on a 3.9.x release, finalize it, then upgrade to 4.x.",
            "4.0 docs: Upgrading Servers to 4.0 from any version 3.3.x through 3.9.x",
        )]
    if line < (3, 5):
        return [Finding(
            "version-too-old", BLOCKER,
            f"Kafka {version} predates a usable ZooKeeper to KRaft migration",
            "The Kafka 3.4 documentation still lists \"Upgrade from ZooKeeper mode\" among the features "
            "not fully implemented in KRaft mode.",
            "Upgrade the brokers to 3.9.x while still in ZooKeeper mode, then migrate.",
            "3.4 docs: KRaft Missing Features",
        )]
    if line < (3, 7):
        return [Finding(
            "version-early-access", BLOCKER,
            f"Migration in Kafka {version.line_str} is early access",
            f"The Kafka {version.line_str} documentation describes ZooKeeper to KRaft migration as an "
            "Early Access feature that is not recommended for production clusters.",
            "Upgrade the brokers to 3.9.x while still in ZooKeeper mode, then migrate.",
            f"{version.line_str} docs: ZooKeeper to KRaft Migration",
        )]
    findings = []
    if line < BRIDGE_LINE:
        findings.append(Finding(
            "version-not-bridge", WARNING,
            f"Kafka {version.line_str} can migrate, but 3.9 is the last ZooKeeper-capable line",
            "Migration is documented for this release line. 3.9.x is the final line that supports "
            "ZooKeeper mode, so it is where later migration fixes and documentation updates land.",
            "Consider upgrading to 3.9.x before migrating.",
            MIGRATION_DOCS,
        ))
    wanted = DOCUMENTED_PATCH[line]
    if version.patch is None:
        findings.append(Finding(
            "version-patch-unknown", INFO,
            "Patch release not given",
            f"The {version.line_str} documentation says to upgrade the brokers to {version.line_str}.{wanted} "
            "before migrating. Pass the full version (for example --kafka-version "
            f"{version.line_str}.{wanted}) to have this checked.",
            source=f"{version.line_str} docs: Preparing for migration",
        ))
    elif version.patch < wanted:
        findings.append(Finding(
            "version-patch-old", WARNING,
            f"Kafka {version} is older than the documented {version.line_str}.{wanted}",
            f"The {version.line_str} documentation says the brokers must be upgraded to software version "
            f"{version.line_str}.{wanted} before beginning the migration.",
            f"Upgrade the brokers to {version.line_str}.{wanted} or a later {version.line_str}.x patch release.",
            f"{version.line_str} docs: Preparing for migration",
        ))
    return findings


def check_protocol_version(version: KafkaVersion, brokers: list[BrokerConfig]) -> list[Finding]:
    findings = []
    key = "inter.broker.protocol.version"
    values = {b.name: b.get(key) for b in brokers}
    unparsable = [n for n, v in values.items() if v is not None and parse_protocol_version(v) is None]
    if unparsable:
        findings.append(Finding(
            "ibp-unreadable", WARNING, f"Cannot interpret {key}",
            "Value(s): " + ", ".join(sorted({values[n] for n in unparsable})) + ".",
            f"Set it to {version.line_str}.", MIGRATION_DOCS, unparsable,
        ))
    behind = []
    for broker in brokers:
        parsed = parse_protocol_version(values[broker.name] or "")
        if parsed is not None and parsed.line < version.line:
            behind.append(broker.name)
    if behind:
        shown = ", ".join(sorted({values[n] for n in behind}))
        findings.append(Finding(
            "ibp-behind", BLOCKER,
            f"{key} is {shown}, not {version.line_str}",
            f"Before migrating, the brokers must run {key}={version.line_str}, matching the software "
            "version. The version must not be changed while a migration is in progress. A broker "
            "also refuses to enable migration with a value below 3.4.",
            f"Finish the rolling upgrade: set {key}={version.line_str} on every broker and restart them "
            "one at a time.",
            f"{MIGRATION_DOCS} (Preparing for migration, Limitations); 3.9 KafkaConfig", behind,
        ))
    ahead = [b.name for b in brokers
             if (p := parse_protocol_version(values[b.name] or "")) is not None and p.line > version.line]
    if ahead:
        findings.append(Finding(
            "ibp-ahead", WARNING,
            f"{key} is newer than the declared Kafka version {version}",
            "Either --kafka-version is wrong or the file belongs to a different cluster.",
            brokers=ahead,
        ))
    unset = [n for n, v in values.items() if v is None]
    if unset and version.line >= (3, 7) and version.major < 4:
        findings.append(Finding(
            "ibp-unset", INFO, f"{key} is not set",
            "A 3.x broker then uses the newest protocol version it supports, which satisfies the "
            f"requirement. The migration guide's sample configuration sets {key}={version.line_str} "
            "explicitly.",
            source=MIGRATION_DOCS, brokers=unset,
        ))
    return findings


def check_broker_ids(brokers: list[BrokerConfig], controller_ids: list[int], facts: Facts) -> list[Finding]:
    findings = []
    missing, too_big = [], []
    seen: dict[int, list[str]] = {}
    for broker in brokers:
        raw = broker.get("node.id") if broker.has("process.roles") else broker.get("broker.id")
        broker_id = None
        try:
            broker_id = int(raw) if raw is not None else None
        except ValueError:
            pass
        if broker_id is None or broker_id < 0:
            missing.append(broker.name)
            continue
        facts.broker_ids[broker.name] = broker_id
        seen.setdefault(broker_id, []).append(broker.name)
        limit = broker.get_int("reserved.broker.max.id") or DEFAULT_RESERVED_BROKER_MAX_ID
        if (not broker.has("process.roles") and broker.get_bool("broker.id.generation.enable", True)
                and broker_id > limit):
            too_big.append(f"{broker.name} (broker.id={broker_id}, reserved.broker.max.id={limit})")
    if missing:
        findings.append(Finding(
            "broker-id-missing", BLOCKER, "broker.id is not set to a non-negative integer",
            "Generated broker ids are incompatible with the migration: a 3.9 broker refuses to start "
            "with zookeeper.metadata.migration.enable=true unless broker.id is 0 or greater.",
            "Set broker.id explicitly to the id the broker currently uses (see meta.properties in its "
            "log directory).",
            f"{MIGRATION_DOCS} (Enter Migration Mode on the Brokers); 3.9 KafkaConfig", missing,
        ))
    if too_big:
        findings.append(Finding(
            "broker-id-above-reserved", BLOCKER, "broker.id exceeds reserved.broker.max.id",
            "; ".join(too_big) + ". The migration guide says broker.id must not exceed "
            "reserved.broker.max.id, and the broker rejects such an id while id generation is enabled.",
            "Raise reserved.broker.max.id or set broker.id.generation.enable=false.",
            f"{MIGRATION_DOCS} (Enter Migration Mode on the Brokers); 3.9 KafkaConfig",
        ))
    for broker_id, names in sorted(seen.items()):
        if len(names) > 1:
            findings.append(Finding(
                "broker-id-duplicate", BLOCKER, f"broker.id {broker_id} is used by more than one file",
                "Each broker must keep its own unique id through the migration.",
                "Check that each file belongs to a different broker.", brokers=names,
            ))
    clashes = sorted(set(controller_ids) & set(seen))
    if clashes:
        findings.append(Finding(
            "controller-id-clash", BLOCKER,
            "Planned controller node.id collides with a broker.id: " + ", ".join(map(str, clashes)),
            "KRaft controllers and brokers share one node id namespace, so the new controllers' "
            "node.id values must differ from every existing broker.id.",
            "Choose controller ids outside the broker id range (the guide uses 3000).",
            f"{MIGRATION_DOCS} (Provisioning the KRaft controller quorum)",
        ))
    return findings


def check_storage(version: KafkaVersion, brokers: list[BrokerConfig]) -> list[Finding]:
    jbod = [b for b in brokers if len(b.get_list("log.dirs")) > 1]
    if not jbod:
        return []
    names = _names(jbod)
    if version.line < (3, 7):
        return [Finding(
            "jbod-unsupported", BLOCKER, "Multiple log directories (JBOD) are not supported in KRaft here",
            f"Kafka {version.line_str} lists JBOD among the features not implemented in KRaft mode. "
            "JBOD support in KRaft starts with metadata version 3.7-IV2.",
            "Upgrade to 3.9.x first.", "3.6 docs: KRaft Missing Features; 3.9 upgrade notes", names,
        )]
    findings = []
    low_ibp = [b.name for b in jbod
               if (p := parse_protocol_version(b.get("inter.broker.protocol.version") or "")) is not None
               and not p.at_least(3, 7, 2)]
    if low_ibp:
        findings.append(Finding(
            "jbod-ibp", BLOCKER, "JBOD migration needs inter.broker.protocol.version 3.7-IV2 or higher",
            "A broker with more than one log directory refuses to enable migration below that version.",
            f"Set inter.broker.protocol.version={version.line_str}.", "3.9 KafkaConfig", low_ibp,
        ))
    detail = ("During the migration, if a ZooKeeper-mode broker with multiple log directories has any "
              "directory fail, the broker shuts down, and it can only migrate once the directory is "
              "repaired (KAFKA-16431).")
    if version.line == (3, 7):
        detail += " In 3.7, JBOD in KRaft is early access and not recommended for production."
    findings.append(Finding(
        "jbod-attention", WARNING, "Brokers use multiple log directories (JBOD)", detail,
        "Check every log directory is healthy before starting, and watch disk health during the migration.",
        f"{MIGRATION_DOCS} (Limitations)", names,
    ))
    return findings


def check_migration_settings(version: KafkaVersion, brokers: list[BrokerConfig], facts: Facts) -> list[Finding]:
    findings = []
    for broker in brokers:
        if broker.has("process.roles"):
            continue
        enabled = broker.get_bool("zookeeper.metadata.migration.enable")
        listener_names = broker.get_list("controller.listener.names")
        if not enabled:
            if listener_names:
                findings.append(Finding(
                    "controller-listener-without-migration", BLOCKER,
                    "controller.listener.names is set but migration is not enabled",
                    "A ZooKeeper-mode broker refuses to start with controller.listener.names set unless "
                    "zookeeper.metadata.migration.enable=true.",
                    "Add zookeeper.metadata.migration.enable=true together with the controller settings, "
                    "or remove controller.listener.names until you are ready.",
                    "3.9 KafkaConfig", [broker.name],
                ))
            continue
        facts.migration_enabled.append(broker)
        problems = []
        has_bootstrap = broker.has("controller.quorum.bootstrap.servers")
        has_voters = broker.has("controller.quorum.voters")
        if not has_bootstrap and not has_voters:
            problems.append("neither controller.quorum.bootstrap.servers nor controller.quorum.voters is set")
        elif has_bootstrap and not has_voters and version.line < (3, 9):
            problems.append(
                f"controller.quorum.bootstrap.servers is a 3.9 setting; Kafka {version.line_str} needs "
                "controller.quorum.voters"
            )
        if not listener_names:
            problems.append("controller.listener.names is empty")
        else:
            mapping = broker.get_list("listener.security.protocol.map")
            mapped = {entry.split(":", 1)[0].strip() for entry in mapping} if mapping else DEFAULT_LISTENER_NAMES
            unmapped = [name for name in listener_names if name not in mapped]
            if unmapped:
                problems.append(
                    "listener.security.protocol.map has no entry for controller listener "
                    + ", ".join(unmapped)
                )
        if problems:
            findings.append(Finding(
                "migration-settings-incomplete", BLOCKER,
                "zookeeper.metadata.migration.enable=true but the controller settings are incomplete",
                "; ".join(problems) + ".",
                "Add the missing settings before restarting this broker.",
                f"{MIGRATION_DOCS} (Enter Migration Mode on the Brokers); 3.9 KafkaConfig", [broker.name],
            ))
    return findings


def check_features(version: KafkaVersion, brokers: list[BrokerConfig], facts: Facts) -> list[Finding]:
    findings = []

    def using(predicate) -> list[str]:
        return [b.name for b in brokers if predicate(b)]

    facts.control_plane_listener = using(lambda b: b.has("control.plane.listener.name"))
    if facts.control_plane_listener:
        findings.append(Finding(
            "control-plane-listener", WARNING, "control.plane.listener.name is set",
            "This setting does not exist in KRaft mode.",
            "Remove it from each broker's configuration before restarting that broker in KRaft mode.",
            f"{MIGRATION_DOCS} (Migrating brokers to KRaft); {DIFF_DOCS}", facts.control_plane_listener,
        ))

    facts.acl_authorizer = using(lambda b: b.get("authorizer.class.name") == ACL_AUTHORIZER)
    if facts.acl_authorizer:
        findings.append(Finding(
            "acl-authorizer", WARNING, "ACLs use the ZooKeeper-based AclAuthorizer",
            f"authorizer.class.name={ACL_AUTHORIZER} is the ZooKeeper-mode authorizer.",
            f"When each broker is restarted in KRaft mode, change it to {STANDARD_AUTHORIZER}.",
            f"{MIGRATION_DOCS} (Migrating brokers to KRaft)", facts.acl_authorizer,
        ))
    custom = using(lambda b: b.has("authorizer.class.name")
                   and b.get("authorizer.class.name") not in (ACL_AUTHORIZER, STANDARD_AUTHORIZER))
    if custom:
        findings.append(Finding(
            "custom-authorizer", WARNING, "A custom authorizer is configured",
            "This tool cannot tell whether a third-party authorizer depends on ZooKeeper.",
            "Confirm with its vendor or source that it works on KRaft brokers and controllers.",
            brokers=custom,
        ))

    def uses_scram(b: BrokerConfig) -> bool:
        mechanisms = b.get_list("sasl.enabled.mechanisms") + b.get_list("sasl.mechanism.inter.broker.protocol")
        return any(m.upper().startswith("SCRAM") for m in mechanisms)

    scram = using(uses_scram)
    if scram and version.line < (3, 5):
        findings.append(Finding(
            "scram-unsupported", BLOCKER, "SCRAM is not implemented in KRaft in this release",
            "The 3.4 documentation lists configuring SCRAM users among the features missing in KRaft mode.",
            "Upgrade to 3.9.x first.", "3.4 docs: KRaft Missing Features", scram,
        ))
    tokens = using(lambda b: b.has("delegation.token.secret.key") or b.has("delegation.token.master.key"))
    if tokens and version.line < (3, 7):
        findings.append(Finding(
            "delegation-tokens-unsupported", BLOCKER,
            "Delegation tokens are not implemented in KRaft in this release",
            "The documentation up to 3.6 lists delegation tokens among the features missing in KRaft mode.",
            "Upgrade to 3.9.x first.", "3.6 docs: KRaft Missing Features", tokens,
        ))

    policies = using(lambda b: b.has("create.topic.policy.class.name") or b.has("alter.config.policy.class.name"))
    if policies:
        alter = using(lambda b: b.has("alter.config.policy.class.name"))
        detail = ("In KRaft mode CreateTopicPolicy and AlterConfigPolicy plugins run on the controllers, "
                  "not the brokers.")
        if alter:
            detail += (" An AlterConfigPolicy also receives different arguments in KRaft mode for "
                       "SUBTRACT, DELETE and APPEND operations; 3.9.2 adds the "
                       "alter.config.policy.kraft.compatibility.enable setting for this (KIP-1252).")
        findings.append(Finding(
            "policy-plugins", WARNING, "Topic or config policy plugins are configured", detail,
            "Deploy the policy JARs on the KRaft controllers and set the same policy class names there.",
            f"{DIFF_DOCS}; {MIGRATION_DOCS} (Limitations)", policies,
        ))

    builders = using(lambda b: b.has("principal.builder.class")
                     and b.get("principal.builder.class") != DEFAULT_PRINCIPAL_BUILDER)
    if builders:
        findings.append(Finding(
            "principal-builder", WARNING, "A custom principal.builder.class is configured",
            "In KRaft mode a custom KafkaPrincipalBuilder must also implement KafkaPrincipalSerde, "
            "otherwise brokers cannot forward requests to the controller.",
            "Check the class implements KafkaPrincipalSerde before restarting brokers in KRaft mode.",
            DIFF_DOCS, builders,
        ))

    zk_acl = using(lambda b: b.get_bool("zookeeper.set.acl"))
    if zk_acl:
        findings.append(Finding(
            "zookeeper-acl", WARNING, "zookeeper.set.acl=true: ZooKeeper nodes are ACL-protected",
            "If the ZooKeeper Security Migration Tool was run on this cluster, the migration can stall "
            "on a malformed /migration znode (KAFKA-19480).",
            "If the migration stalls, remove the node with 'delete /migration' in zookeeper-shell.sh.",
            f"{MIGRATION_DOCS} (Provisioning the KRaft controller quorum)", zk_acl,
        ))

    removed: dict[str, list[str]] = {}
    for broker in brokers:
        for key in REMOVED_IN_KRAFT:
            if broker.has(key):
                removed.setdefault(key, []).append(broker.name)
    if removed:
        findings.append(Finding(
            "removed-settings", INFO, "Settings that do not exist in KRaft mode",
            "Present: " + ", ".join(sorted(removed)) + ".",
            "Remove them, together with inter.broker.protocol.version and the zookeeper.* settings, "
            "when each broker is converted to KRaft.",
            DIFF_DOCS, sorted({name for names in removed.values() for name in names}),
        ))
    return findings


def run_checks(
    version: KafkaVersion, brokers: list[BrokerConfig], controller_ids: list[int] | None = None
) -> tuple[list[Finding], Facts]:
    facts = Facts()
    findings = check_version(version)

    for broker in brokers:
        if broker.has("process.roles"):
            facts.kraft_brokers.append(broker)
        else:
            facts.zk_brokers.append(broker)
            facts.zookeeper_connect = facts.zookeeper_connect or broker.get("zookeeper.connect")

    no_mode = [b.name for b in facts.zk_brokers if not b.has("zookeeper.connect")]
    if no_mode:
        findings.append(Finding(
            "not-a-broker-config", WARNING, "Neither zookeeper.connect nor process.roles is set",
            "This does not look like a complete broker configuration, so the result may be misleading.",
            "Pass the server.properties the broker is actually started with.", brokers=no_mode,
        ))
    if facts.kraft_brokers:
        findings.append(Finding(
            "already-kraft", INFO, "Already configured for KRaft mode (process.roles is set)",
            "ZooKeeper-mode checks were skipped for these files.", brokers=_names(facts.kraft_brokers),
        ))

    zk = facts.zk_brokers
    findings += check_protocol_version(version, zk)
    findings += check_broker_ids(brokers, controller_ids or [], facts)
    findings += check_storage(version, zk)
    findings += check_migration_settings(version, zk, facts)
    findings += check_features(version, zk, facts)
    findings.sort(key=lambda f: SEVERITY_ORDER[f.severity])
    return findings, facts
