import pytest

from zk_to_kraft_check.properties import BrokerConfig

BASE = {
    "broker.id": "0",
    "listeners": "PLAINTEXT://:9092",
    "inter.broker.protocol.version": "3.9",
    "log.dirs": "/var/lib/kafka/data",
    "zookeeper.connect": "zk1:2181,zk2:2181/kafka",
}


@pytest.fixture
def broker():
    """Factory for a healthy ZooKeeper-mode broker config with overrides.

    Pass None as a value to remove a key.
    """

    def make(name="b0.properties", **overrides):
        props = dict(BASE)
        for key, value in overrides.items():
            key = key.replace("__", ".")
            if value is None:
                props.pop(key, None)
            else:
                props[key] = str(value)
        return BrokerConfig(name=name, props=props)

    return make
