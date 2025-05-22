# Example inputs

Hand-written `server.properties` files for three fictional clusters. Host names are placeholders.

| Folder | Declared version | What it shows |
|---|---|---|
| `ready/` | 3.9.1 | Three ZooKeeper-mode brokers with nothing in the way. |
| `needs-work/` | 3.8.0 | Old `inter.broker.protocol.version`, a broker with a generated id, JBOD, `control.plane.listener.name`, `AclAuthorizer`, an `AlterConfigPolicy` and ACL-protected ZooKeeper. |
| `in-migration/` | 3.9.1 | Part-way through: one broker already restarted in KRaft mode, two in migration mode. |

```bash
zk-to-kraft-check --kafka-version 3.9.1 examples/ready/broker-*.properties
zk-to-kraft-check --kafka-version 3.8.0 --controller-ids 1,3000,3001 examples/needs-work/broker-*.properties
zk-to-kraft-check --kafka-version 3.9.1 examples/in-migration/broker-*.properties
```
