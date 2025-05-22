import pytest

from zk_to_kraft_check.checks import BLOCKER, INFO, WARNING, run_checks
from zk_to_kraft_check.versions import parse_kafka_version, parse_protocol_version


def ids(findings, severity=None):
    return [f.id for f in findings if severity is None or f.severity == severity]


def check(version, *brokers, controller_ids=None):
    findings, facts = run_checks(parse_kafka_version(version), list(brokers), controller_ids)
    return findings, facts


def test_clean_3_9_cluster_has_no_findings(broker):
    findings, facts = check("3.9.1", broker("b0", broker__id=0), broker("b1", broker__id=1))
    assert findings == []
    assert facts.broker_ids == {"b0": 0, "b1": 1}
    assert facts.zookeeper_connect == "zk1:2181,zk2:2181/kafka"


# --- versions -----------------------------------------------------------

@pytest.mark.parametrize("version, expected", [
    ("4.0.0", "version-no-zookeeper"),
    ("4.4.0", "version-no-zookeeper"),
    ("2.8.2", "version-too-old"),
    ("3.4.1", "version-too-old"),
    ("3.5.2", "version-early-access"),
    ("3.6.2", "version-not-covered"),
])
def test_versions_that_cannot_migrate(broker, version, expected):
    findings, _ = check(version, broker(inter__broker__protocol__version=None))
    assert expected in ids(findings, BLOCKER)


def test_3_7_and_3_8_get_a_warning_not_a_blocker(broker):
    for version, ibp in (("3.7.2", "3.7"), ("3.8.1", "3.8")):
        findings, _ = check(version, broker(inter__broker__protocol__version=ibp))
        assert ids(findings) == ["version-not-bridge"]
        assert findings[0].severity == WARNING


@pytest.mark.parametrize("version, expected", [
    ("3.9.0", ["version-patch-old"]),
    ("3.9.1", []),
    ("3.9.3", []),
    ("3.9", ["version-patch-unknown"]),
])
def test_patch_level(broker, version, expected):
    findings, _ = check(version, broker())
    assert ids(findings) == expected


def test_version_parsing_rejects_garbage():
    with pytest.raises(ValueError):
        parse_kafka_version("latest")
    with pytest.raises(ValueError):
        parse_kafka_version("7.5.0-ccs")


# --- inter.broker.protocol.version -----------------------------------------

def test_protocol_version_parsing():
    assert parse_protocol_version("3.9").line == (3, 9)
    assert parse_protocol_version("3.7-IV2").iv == 2
    assert parse_protocol_version("3.7-IV1").at_least(3, 7, 2) is False
    assert parse_protocol_version("3.7").at_least(3, 7, 2) is True
    assert parse_protocol_version("3.8-IV0").at_least(3, 7, 2) is True
    assert parse_protocol_version("2.8.1").line == (2, 8)
    assert parse_protocol_version("newest") is None


def test_old_protocol_version_blocks(broker):
    findings, _ = check("3.9.1", broker("b0"), broker("b1", broker__id=1, inter__broker__protocol__version="3.6-IV2"))
    blocker = next(f for f in findings if f.id == "ibp-behind")
    assert blocker.severity == BLOCKER and blocker.brokers == ["b1"]


def test_unset_protocol_version_is_only_a_note(broker):
    findings, _ = check("3.9.1", broker(inter__broker__protocol__version=None))
    assert ids(findings) == ["ibp-unset"] and findings[0].severity == INFO


def test_unreadable_and_too_new_protocol_versions_warn(broker):
    findings, _ = check("3.9.1", broker("b0", inter__broker__protocol__version="banana"),
                        broker("b1", broker__id=1, inter__broker__protocol__version="4.0"))
    assert set(ids(findings, WARNING)) == {"ibp-unreadable", "ibp-ahead"}


# --- broker ids -------------------------------------------------------------

@pytest.mark.parametrize("value", [None, "-1", "auto"])
def test_missing_or_generated_broker_id_blocks(broker, value):
    findings, facts = check("3.9.1", broker(broker__id=value))
    assert ids(findings, BLOCKER) == ["broker-id-missing"]
    assert facts.broker_ids == {}


def test_broker_id_above_reserved_max(broker):
    findings, _ = check("3.9.1", broker(broker__id=1001))
    assert "broker-id-above-reserved" in ids(findings, BLOCKER)
    findings, _ = check("3.9.1", broker(broker__id=1001, reserved__broker__max__id=5000))
    assert "broker-id-above-reserved" not in ids(findings)
    findings, _ = check("3.9.1", broker(broker__id=1001, broker__id__generation__enable="false"))
    assert "broker-id-above-reserved" not in ids(findings)


def test_duplicate_broker_ids_block(broker):
    findings, _ = check("3.9.1", broker("a"), broker("b"))
    dup = next(f for f in findings if f.id == "broker-id-duplicate")
    assert dup.brokers == ["a", "b"]


def test_controller_id_clash(broker):
    findings, _ = check("3.9.1", broker(broker__id=3000), controller_ids=[3000, 3001, 3002])
    assert "controller-id-clash" in ids(findings, BLOCKER)
    findings, _ = check("3.9.1", broker(broker__id=3000, reserved__broker__max__id=9999), controller_ids=[4000])
    assert ids(findings, BLOCKER) == []


# --- storage ----------------------------------------------------------------

def test_jbod_warns_on_3_9(broker):
    findings, _ = check("3.9.1", broker(log__dirs="/d1,/d2"))
    assert ids(findings) == ["jbod-attention"]
    assert "KAFKA-16431" in findings[0].detail


def test_jbod_blocks_before_3_7(broker):
    findings, _ = check("3.6.2", broker(log__dirs="/d1,/d2", inter__broker__protocol__version="3.6"))
    assert "jbod-unsupported" in ids(findings, BLOCKER)


def test_jbod_needs_ibp_3_7_iv2(broker):
    findings, _ = check("3.7.2", broker(log__dirs="/d1,/d2", inter__broker__protocol__version="3.7-IV1"))
    assert "jbod-ibp" in ids(findings, BLOCKER)
    assert "early access" in next(f for f in findings if f.id == "jbod-attention").detail


# --- migration settings -----------------------------------------------------

MIGRATION = {
    "zookeeper__metadata__migration__enable": "true",
    "controller__quorum__bootstrap__servers": "c1:9093",
    "controller__listener__names": "CONTROLLER",
    "listener__security__protocol__map": "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT",
}


def test_complete_migration_settings_pass(broker):
    findings, facts = check("3.9.1", broker(**MIGRATION))
    assert findings == [] and len(facts.migration_enabled) == 1


@pytest.mark.parametrize("override, fragment", [
    ({"controller__quorum__bootstrap__servers": None}, "neither controller.quorum.bootstrap.servers"),
    ({"controller__listener__names": None}, "controller.listener.names is empty"),
    ({"listener__security__protocol__map": "PLAINTEXT:PLAINTEXT"}, "no entry for controller listener CONTROLLER"),
    ({"listener__security__protocol__map": None}, "no entry for controller listener CONTROLLER"),
])
def test_incomplete_migration_settings_block(broker, override, fragment):
    findings, _ = check("3.9.1", broker(**{**MIGRATION, **override}))
    assert ids(findings, BLOCKER) == ["migration-settings-incomplete"]
    assert fragment in findings[0].detail


def test_bootstrap_servers_setting_is_3_9_only(broker):
    findings, _ = check("3.8.1", broker(**{**MIGRATION, "inter__broker__protocol__version": "3.8"}))
    assert "migration-settings-incomplete" in ids(findings, BLOCKER)
    voters = {**MIGRATION, "controller__quorum__bootstrap__servers": None,
              "controller__quorum__voters": "3000@c1:9093", "inter__broker__protocol__version": "3.8"}
    findings, _ = check("3.8.1", broker(**voters))
    assert ids(findings, BLOCKER) == []


def test_controller_listener_without_migration_flag_blocks(broker):
    findings, _ = check("3.9.1", broker(controller__listener__names="CONTROLLER"))
    assert ids(findings, BLOCKER) == ["controller-listener-without-migration"]


# --- features ---------------------------------------------------------------

def test_feature_warnings(broker):
    findings, facts = check("3.9.1", broker(
        control__plane__listener__name="CONTROL",
        authorizer__class__name="kafka.security.authorizer.AclAuthorizer",
        alter__config__policy__class__name="com.example.Policy",
        principal__builder__class="com.example.Builder",
        zookeeper__set__acl="true",
        password__encoder__secret="placeholder",
    ))
    assert ids(findings, BLOCKER) == []
    assert set(ids(findings, WARNING)) == {
        "control-plane-listener", "acl-authorizer", "policy-plugins", "principal-builder", "zookeeper-acl",
    }
    assert ids(findings, INFO) == ["removed-settings"]
    assert "KIP-1252" in next(f for f in findings if f.id == "policy-plugins").detail
    assert facts.acl_authorizer and facts.control_plane_listener


def test_custom_authorizer_and_default_principal_builder(broker):
    findings, _ = check("3.9.1", broker(
        authorizer__class__name="com.example.RangerLikeAuthorizer",
        principal__builder__class="org.apache.kafka.common.security.authenticator.DefaultKafkaPrincipalBuilder",
    ))
    assert ids(findings) == ["custom-authorizer"]


def test_create_topic_policy_alone_does_not_mention_kip_1252(broker):
    findings, _ = check("3.9.1", broker(create__topic__policy__class__name="com.example.P"))
    assert "KIP-1252" not in findings[0].detail


def test_scram_and_delegation_tokens_only_block_old_versions(broker):
    old = broker(sasl__enabled__mechanisms="PLAIN,SCRAM-SHA-512", delegation__token__secret__key="placeholder",
                 inter__broker__protocol__version="3.4")
    assert {"scram-unsupported", "delegation-tokens-unsupported"} <= set(ids(check("3.4.1", old)[0], BLOCKER))
    new = broker(sasl__enabled__mechanisms="SCRAM-SHA-512", delegation__token__secret__key="placeholder")
    assert check("3.9.1", new)[0] == []


# --- mode detection ---------------------------------------------------------

def test_kraft_file_is_noted_and_skipped(broker):
    kraft = broker("k", broker__id=None, zookeeper__connect=None, inter__broker__protocol__version=None,
                   process__roles="broker", node__id=5)
    findings, facts = check("3.9.1", kraft, broker("z", broker__id=6))
    assert ids(findings) == ["already-kraft"]
    assert facts.broker_ids == {"k": 5, "z": 6} and len(facts.kraft_brokers) == 1


def test_file_without_zookeeper_or_roles_warns(broker):
    findings, _ = check("3.9.1", broker(zookeeper__connect=None))
    assert ids(findings) == ["not-a-broker-config"]


def test_findings_are_sorted_blockers_first(broker):
    findings, _ = check("3.9.0", broker(broker__id=None, control__plane__listener__name="X",
                                        controlled__shutdown__max__retries=3))
    severities = [f.severity for f in findings]
    assert severities == sorted(severities, key=[BLOCKER, WARNING, INFO].index)
    assert severities[0] == BLOCKER and severities[-1] == INFO
