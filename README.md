# zk-to-kraft-check

Check whether an Apache Kafka cluster is ready to migrate from ZooKeeper to KRaft, and list what blocks it, by reading the brokers' `server.properties` files.

**Status:** the offline check works today. Give it one `server.properties` per broker and the Kafka version they run; it reports blockers, things that need attention and an ordered migration checklist filled in for your cluster. It does not connect to brokers or ZooKeeper yet, so anything that lives only in the running cluster is out of scope for now (see [Limitations](#limitations)).

**Kafka versions:** the rules follow the "ZooKeeper to KRaft Migration" section of the Apache Kafka **3.9** documentation, the last release line with ZooKeeper mode. Clusters declared as 3.7 or 3.8 are checked against the same steps with their own version numbers. Anything older, and 4.0 or newer (where ZooKeeper mode no longer exists), is reported as not ready with the reason.

## Install

Python 3.10 or newer, no dependencies.

```bash
git clone https://github.com/narayana006/zk-to-kraft-check.git
cd zk-to-kraft-check
pip install .
```

## Quick start

The repository ships three example clusters. This one has several problems:

```console
$ zk-to-kraft-check --kafka-version 3.8.0 --controller-ids 1,3000,3001 examples/needs-work/broker-*.properties
ZooKeeper to KRaft migration readiness: NOT READY
  Kafka version   3.8.0 (declared)
  Broker files    3
  Findings        4 blocker(s), 7 warning(s), 1 note(s)

[BLOCKER] inter.broker.protocol.version is 3.6, not 3.8
    Before migrating, the brokers must run inter.broker.protocol.version=3.8, matching the software version. The version must not be changed while a migration is in progress. A broker also refuses to enable migration with a value below 3.4.
    Files: examples/needs-work/broker-1.properties, examples/needs-work/broker-2.properties, examples/needs-work/broker-3.properties
    Fix: Finish the rolling upgrade: set inter.broker.protocol.version=3.8 on every broker and restart them one at a time.
    Source: 3.9 docs: ZooKeeper to KRaft Migration (Preparing for migration, Limitations); 3.9 KafkaConfig

[BLOCKER] broker.id is not set to a non-negative integer
    Generated broker ids are incompatible with the migration: a 3.9 broker refuses to start with zookeeper.metadata.migration.enable=true unless broker.id is 0 or greater.
    Files: examples/needs-work/broker-3.properties
    Fix: Set broker.id explicitly to the id the broker currently uses (see meta.properties in its log directory).
    Source: 3.9 docs: ZooKeeper to KRaft Migration (Enter Migration Mode on the Brokers); 3.9 KafkaConfig

[BLOCKER] Planned controller node.id collides with a broker.id: 1
    KRaft controllers and brokers share one node id namespace, so the new controllers' node.id values must differ from every existing broker.id.
    Fix: Choose controller ids outside the broker id range (the guide uses 3000).
    Source: 3.9 docs: ZooKeeper to KRaft Migration (Provisioning the KRaft controller quorum)

[BLOCKER] JBOD migration needs inter.broker.protocol.version 3.7-IV2 or higher
    A broker with more than one log directory refuses to enable migration below that version.
    Files: examples/needs-work/broker-1.properties, examples/needs-work/broker-2.properties, examples/needs-work/broker-3.properties
    Fix: Set inter.broker.protocol.version=3.8.
    Source: 3.9 KafkaConfig

[WARNING] Kafka 3.8 can migrate, but 3.9 is the last ZooKeeper-capable line
    Migration is documented for this release line. 3.9.x is the final line that supports ZooKeeper mode, so it is where later migration fixes and documentation updates land.
    Fix: Consider upgrading to 3.9.x before migrating.
    Source: 3.9 docs: ZooKeeper to KRaft Migration

[WARNING] Kafka 3.8.0 is older than the documented 3.8.1
    The 3.8 documentation says the brokers must be upgraded to software version 3.8.1 before beginning the migration.
    Fix: Upgrade the brokers to 3.8.1 or a later 3.8.x patch release.
    Source: 3.8 docs: Preparing for migration

[WARNING] Brokers use multiple log directories (JBOD)
    During the migration, if a ZooKeeper-mode broker with multiple log directories has any directory fail, the broker shuts down, and it can only migrate once the directory is repaired (KAFKA-16431).
    Files: examples/needs-work/broker-1.properties, examples/needs-work/broker-2.properties, examples/needs-work/broker-3.properties
    Fix: Check every log directory is healthy before starting, and watch disk health during the migration.
    Source: 3.9 docs: ZooKeeper to KRaft Migration (Limitations)

[WARNING] control.plane.listener.name is set
    This setting does not exist in KRaft mode.
    Files: examples/needs-work/broker-1.properties, examples/needs-work/broker-2.properties, examples/needs-work/broker-3.properties
    Fix: Remove it from each broker's configuration before restarting that broker in KRaft mode.
    Source: 3.9 docs: ZooKeeper to KRaft Migration (Migrating brokers to KRaft); 4.x docs: Differences Between KRaft mode and ZooKeeper mode

[WARNING] ACLs use the ZooKeeper-based AclAuthorizer
    authorizer.class.name=kafka.security.authorizer.AclAuthorizer is the ZooKeeper-mode authorizer.
    Files: examples/needs-work/broker-1.properties, examples/needs-work/broker-2.properties, examples/needs-work/broker-3.properties
    Fix: When each broker is restarted in KRaft mode, change it to org.apache.kafka.metadata.authorizer.StandardAuthorizer.
    Source: 3.9 docs: ZooKeeper to KRaft Migration (Migrating brokers to KRaft)

[WARNING] Topic or config policy plugins are configured
    In KRaft mode CreateTopicPolicy and AlterConfigPolicy plugins run on the controllers, not the brokers. An AlterConfigPolicy also receives different arguments in KRaft mode for SUBTRACT, DELETE and APPEND operations; 3.9.2 adds the alter.config.policy.kraft.compatibility.enable setting for this (KIP-1252).
    Files: examples/needs-work/broker-1.properties, examples/needs-work/broker-2.properties, examples/needs-work/broker-3.properties
    Fix: Deploy the policy JARs on the KRaft controllers and set the same policy class names there.
    Source: 4.x docs: Differences Between KRaft mode and ZooKeeper mode; 3.9 docs: ZooKeeper to KRaft Migration (Limitations)

[WARNING] zookeeper.set.acl=true: ZooKeeper nodes are ACL-protected
    If the ZooKeeper Security Migration Tool was run on this cluster, the migration can stall on a malformed /migration znode (KAFKA-19480).
    Files: examples/needs-work/broker-1.properties, examples/needs-work/broker-2.properties, examples/needs-work/broker-3.properties
    Fix: If the migration stalls, remove the node with 'delete /migration' in zookeeper-shell.sh.
    Source: 3.9 docs: ZooKeeper to KRaft Migration (Provisioning the KRaft controller quorum)

[INFO] Settings that do not exist in KRaft mode
    Present: controlled.shutdown.max.retries, leader.imbalance.per.broker.percentage.
    Files: examples/needs-work/broker-1.properties, examples/needs-work/broker-2.properties, examples/needs-work/broker-3.properties
    Fix: Remove them, together with inter.broker.protocol.version and the zookeeper.* settings, when each broker is converted to KRaft.
    Source: 4.x docs: Differences Between KRaft mode and ZooKeeper mode

Migration checklist
  [x] done   [ ] to do   [!] blocked   [?] cannot be verified from files
  [!] 1. Run every broker on Kafka 3.8.x with inter.broker.protocol.version=3.8
         - Do not change the protocol version again until the migration is finalized.
         - Only the files you passed were checked; make sure they cover every broker.
  [!] 2. Clear the configuration blockers listed above
         - broker.id is not set to a non-negative integer
         - Planned controller node.id collides with a broker.id: 1
         - JBOD migration needs inter.broker.protocol.version 3.7-IV2 or higher
  [?] 3. Find the cluster id
         - bin/zookeeper-shell.sh zk-0.example.internal:2181,zk-1.example.internal:2181,zk-2.example.internal:2181 get /cluster/id
         - It is also in meta.properties in each broker's log directory.
  [?] 4. Provision the KRaft controller quorum (three or more dedicated controllers)
         - Format them with the cluster id from the previous step.
         - Each controller needs zookeeper.metadata.migration.enable=true, zookeeper.connect and the brokers' inter.broker.listener.name, besides the usual KRaft controller settings.
         - node.id values must not collide with broker ids in use: 1, 2.
         - Recommended: log org.apache.kafka.metadata.migration at TRACE on the controllers.
  [ ] 5. Put the brokers in migration mode, one rolling restart
         - 0 of 3 broker file(s) already have this.
         - Add: zookeeper.metadata.migration.enable=true, controller.quorum.voters, controller.listener.names, and the controller listener in listener.security.protocol.map.
         - The metadata migration starts by itself when the last broker has been restarted.
  [?] 6. Wait for the metadata migration to complete
         - Look for 'Completed migration of metadata from Zookeeper to KRaft' at INFO level in the active controller's log.
  [ ] 7. Restart each broker in KRaft mode, one at a time
         - 0 of 3 broker file(s) already have process.roles.
         - Per broker: replace broker.id with node.id (same number), add process.roles=broker, remove zookeeper.* settings, zookeeper.metadata.migration.enable and inter.broker.protocol.version.
         - Also remove control.plane.listener.name.
         - Also set authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer.
         - Until the next step is done you can still revert to ZooKeeper mode.
  [?] 8. Finalize: take the controllers out of migration mode
         - Remove zookeeper.metadata.migration.enable and zookeeper.connect from each controller and restart them one at a time.
         - This cannot be undone: after finalizing there is no way back to ZooKeeper mode.
  [?] 9. Decommission ZooKeeper
         - Only if nothing else uses it. The cluster can then be upgraded to Kafka 4.x.

Not checked (needs a live cluster): actual broker versions, topic and dynamic configs,
ACLs, quotas and SCRAM credentials stored in ZooKeeper, and cluster health.
$ echo $?
1
```

A cluster with nothing in the way starts like this, followed by the same checklist with steps 1 and 2 ticked:

```console
$ zk-to-kraft-check --kafka-version 3.9.1 examples/ready/broker-*.properties
ZooKeeper to KRaft migration readiness: READY
  Kafka version   3.9.1 (declared)
  Broker files    3
  Findings        0 blocker(s), 0 warning(s), 0 note(s)
...
$ echo $?
0
```

`examples/in-migration/` shows a cluster part-way through, where the checklist tracks how many brokers have reached each stage.

## Usage

```text
zk-to-kraft-check --kafka-version X.Y.Z [--controller-ids ID,ID,...] [--json] [--strict] SERVER_PROPERTIES...
```

| Argument | Meaning |
|---|---|
| `SERVER_PROPERTIES...` | One file per broker. Copy them from the brokers; the tool only reads them. |
| `--kafka-version` | The Apache Kafka version the brokers run, for example `3.9.1`. Required, because a properties file does not record it. `3.9` is accepted; the patch-level check is then skipped with a note. |
| `--controller-ids` | The `node.id` values you plan to give the new KRaft controllers. Checked against the broker ids, since both share one id namespace. |
| `--json` | Machine-readable output: `verdict`, `exit_code`, `kafka_version`, `brokers`, `findings` (each with `id`, `severity`, `title`, `detail`, `action`, `source`, `brokers`) and `checklist`. |
| `--strict` | Exit 1 on warnings as well as blockers. |

Exit codes: `0` no blockers, `1` blockers found (or warnings with `--strict`), `2` unreadable input or bad arguments.

Checklist marks: `[x]` done as far as the files show, `[ ]` to do, `[!]` blocked by a finding, `[?]` cannot be verified from configuration files.

## What is checked

Each finding names its source. "3.9 docs" is the migration section of the Kafka 3.9 operations documentation, "4.x docs" is the "Differences Between KRaft mode and ZooKeeper mode" page, and "3.9 KafkaConfig" is a start-up validation in the 3.9 broker source.

**Blockers**

| Check | Rule |
|---|---|
| Kafka version | 4.0+ has no ZooKeeper mode. 3.4 and older predate a usable migration. 3.5 documents it as early access, not for production. 3.6 was the first production-ready release for migration, but this tool does not cover it: its rules are written for 3.7 to 3.9. |
| `inter.broker.protocol.version` | Must match the running release line (for example `3.9`); a lower value blocks. Unset is fine on 3.x, where it defaults to the newest version the broker supports. |
| `broker.id` | Must be set to a non-negative integer: generated ids are incompatible with migration. Must not exceed `reserved.broker.max.id` (default 1000) while id generation is enabled. Must be unique across the files, and must not collide with `--controller-ids`. |
| JBOD (`log.dirs` with several directories) | Not supported in KRaft before 3.7; from 3.7 it needs `inter.broker.protocol.version` 3.7-IV2 or higher. |
| Half-configured migration mode | `zookeeper.metadata.migration.enable=true` without a controller quorum setting, without `controller.listener.names`, or without the controller listener in `listener.security.protocol.map`; `controller.quorum.bootstrap.servers` alone on a pre-3.9 broker; `controller.listener.names` set without the migration flag. |
| SCRAM, delegation tokens | Only on the old releases whose documentation lists them as missing in KRaft (SCRAM: 3.4; delegation tokens: up to 3.6). |

**Warnings**

| Check | Why it needs attention |
|---|---|
| 3.7 or 3.8 | Migration works, but 3.9 is the final ZooKeeper-capable line. This is the tool's recommendation, not a Kafka requirement. |
| Patch release | Each line's documentation names a patch release to be on before migrating: 3.7.2, 3.8.1, 3.9.1. |
| JBOD | During migration a failed log directory shuts the broker down (KAFKA-16431). |
| `control.plane.listener.name` | Must be removed before a broker restarts in KRaft mode. |
| `authorizer.class.name` | `kafka.security.authorizer.AclAuthorizer` must become `org.apache.kafka.metadata.authorizer.StandardAuthorizer` when the broker restarts in KRaft mode. Any other custom authorizer is flagged for manual confirmation. |
| `create.topic.policy.class.name`, `alter.config.policy.class.name` | Policies run on the controllers in KRaft mode; `AlterConfigPolicy` argument differences (KIP-1252). |
| `principal.builder.class` | A custom builder must also implement `KafkaPrincipalSerde`. |
| `zookeeper.set.acl=true` | Possible stalled migration if the ZooKeeper Security Migration Tool was used (KAFKA-19480). |

**Notes**

Settings that no longer exist in KRaft mode and should be dropped when a broker is converted: `password.encoder.*`, `controlled.shutdown.max.retries`, `controlled.shutdown.retry.backoff.ms`, `reserved.broker.max.id`, `broker.id.generation.enable`, `leader.imbalance.per.broker.percentage`.

## Features

- [x] Checks broker versions and inter-broker protocol (from the version you declare and the `inter.broker.protocol.version` in the files)
- [x] Flags features and configurations not supported in KRaft
- [ ] Checks ACL and quota storage (today: only the configured authorizer class; ACLs and quotas stored in ZooKeeper are not read)
- [x] Produces a step-by-step migration checklist for your cluster
- [x] Read-only: it changes nothing
- [ ] Live inspection with `--bootstrap` and `--zookeeper`

## Limitations

- **Offline only.** The planned `--bootstrap broker1:9092 --zookeeper zk1:2181` interface does not exist yet. The tool sees only the files you give it.
- The Kafka version is whatever you declare; it is not verified against the brokers.
- Dynamic broker and topic configuration, ACLs, quotas, SCRAM credentials and delegation tokens stored in ZooKeeper are not inspected.
- Settings supplied outside `server.properties` (command-line `--override`, environment-driven images, configuration providers) are invisible to it. `$Ellipsis` placeholders are treated as literal text.
- Controller configuration files are not validated; controller requirements appear only as checklist notes.
- A clean result means no known static-configuration blocker was found. It is not proof that a migration will succeed. Rehearse on a non-production cluster and read the Kafka migration guide for your version.
- The rules were written from the Apache Kafka documentation and 3.9 source. The tool has not been validated against a real ZooKeeper-to-KRaft migration.
- Apache Kafka version numbers only. Vendor distributions with their own numbering are rejected by `--kafka-version`; pass the underlying Apache Kafka version.
- The `.properties` reader does not interpret unicode escapes.

## Development

```bash
pip install -e ".[test]"
pytest
```

## Contributing

Reports of migration blockers the tool missed are the most valuable contribution: open an issue with the setting, the Kafka version and a link to the documentation or JIRA ticket. New rules should cite their source the same way the existing ones do.

## Author

Built and maintained by [Narayana Vemburaj](https://github.com/narayana006).

## License

[MIT](LICENSE)
