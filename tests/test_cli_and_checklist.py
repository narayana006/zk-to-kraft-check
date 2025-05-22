import io
import json
from pathlib import Path

import pytest

from zk_to_kraft_check import cli
from zk_to_kraft_check.checklist import BLOCKED, DONE, MANUAL, TODO, build_checklist
from zk_to_kraft_check.checks import run_checks
from zk_to_kraft_check.versions import parse_kafka_version

EXAMPLES = Path(__file__).resolve().parent.parent / "examples"


def files(folder):
    return sorted(str(p) for p in (EXAMPLES / folder).glob("*.properties"))


def run(argv):
    out, err = io.StringIO(), io.StringIO()
    code = cli.main(argv, out=out, err=err)
    return code, out.getvalue(), err.getvalue()


def steps_for(version, brokers):
    v = parse_kafka_version(version)
    findings, facts = run_checks(v, brokers)
    return build_checklist(v, findings, facts)


def test_ready_example():
    code, out, _ = run(["--kafka-version", "3.9.1", *files("ready")])
    assert code == 0
    assert out.startswith("ZooKeeper to KRaft migration readiness: READY\n")
    assert "0 blocker(s), 0 warning(s), 0 note(s)" in out
    assert "[x] 1. Run every broker on Kafka 3.9.x" in out
    assert "zk-0.example.internal:2181,zk-1.example.internal:2181,zk-2.example.internal:2181/kafka get /cluster/id" in out
    assert "broker ids in use: 0, 1, 2." in out


def test_needs_work_example():
    code, out, _ = run(["--kafka-version", "3.8.0", "--controller-ids", "1,3000,3001", *files("needs-work")])
    assert code == 1
    assert "readiness: NOT READY" in out
    assert "4 blocker(s), 7 warning(s), 1 note(s)" in out
    assert "[!] 1. Run every broker on Kafka 3.8.x" in out
    assert "controller.quorum.voters" in out  # pre-3.9 quorum setting
    assert "Also remove control.plane.listener.name." in out
    assert "Also set authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer." in out


def test_in_migration_example_tracks_progress():
    code, out, _ = run(["--kafka-version", "3.9.1", *files("in-migration")])
    assert code == 0
    assert "[x] 5. Put the brokers in migration mode" in out
    assert "3 of 3 broker file(s) already have this." in out
    assert "[ ] 7. Restart each broker in KRaft mode" in out
    assert "1 of 3 broker file(s) already have process.roles." in out


def test_json_output_is_complete():
    code, out, _ = run(["--json", "--kafka-version", "3.8.0", *files("needs-work")])
    data = json.loads(out)
    assert code == 1 and data["exit_code"] == 1 and data["verdict"] == "NOT READY"
    assert data["kafka_version"] == "3.8.0"
    assert [b["broker_id"] for b in data["brokers"]] == [1, 2, None]
    assert {"id", "severity", "title", "detail", "action", "source", "brokers"} <= set(data["findings"][0])
    assert [s["number"] for s in data["checklist"]] == list(range(1, 10))


def test_strict_turns_warnings_into_failure(tmp_path):
    cfg = tmp_path / "server.properties"
    cfg.write_text("broker.id=1\nzookeeper.connect=zk:2181\ninter.broker.protocol.version=3.9\n"
                   "control.plane.listener.name=CONTROL\n")
    assert run(["--kafka-version", "3.9.1", str(cfg)])[0] == 0
    code, out, _ = run(["--kafka-version", "3.9.1", "--strict", str(cfg)])
    assert code == 1 and "READY WITH WARNINGS" in out


def test_kafka_4_is_not_ready():
    code, out, _ = run(["--kafka-version", "4.1.0", *files("ready")])
    assert code == 1 and "cannot run in ZooKeeper mode" in out
    assert "[!] 1. Run every broker on Kafka 3.9.x" in out


@pytest.mark.parametrize("argv, message", [
    (["--kafka-version", "three", "x.properties"], "not an Apache Kafka version"),
    (["--kafka-version", "3.9.1", "/no/such/server.properties"], "cannot read"),
    (["--kafka-version", "3.9.1", "--controller-ids", "a,b", "x.properties"], "--controller-ids must be comma-separated integers"),
])
def test_bad_input_exits_2(argv, message):
    code, out, err = run(argv)
    assert code == 2 and out == "" and message in err


def test_empty_file_exits_2(tmp_path):
    empty = tmp_path / "empty.properties"
    empty.write_text("# nothing here\n")
    code, _, err = run(["--kafka-version", "3.9.1", str(empty)])
    assert code == 2 and "contains no properties" in err


def test_missing_required_arguments_is_a_usage_error():
    with pytest.raises(SystemExit) as exc:
        cli.main(["broker.properties"])
    assert exc.value.code == 2


def test_tool_never_writes_to_its_inputs(tmp_path):
    cfg = tmp_path / "server.properties"
    cfg.write_text("broker.id=1\nzookeeper.connect=zk:2181\n")
    before = (cfg.read_bytes(), cfg.stat().st_mtime_ns, sorted(p.name for p in tmp_path.iterdir()))
    run(["--kafka-version", "3.9.1", str(cfg)])
    assert before == (cfg.read_bytes(), cfg.stat().st_mtime_ns, sorted(p.name for p in tmp_path.iterdir()))


def test_checklist_states(broker):
    steps = steps_for("3.9.1", [broker()])
    assert [s.state for s in steps] == [DONE, DONE, MANUAL, MANUAL, TODO, MANUAL, TODO, MANUAL, MANUAL]
    steps = steps_for("3.9.1", [broker(broker__id=None, inter__broker__protocol__version="3.5")])
    assert steps[0].state == BLOCKED and steps[1].state == BLOCKED
    assert steps[1].notes == ["broker.id is not set to a non-negative integer"]


def test_checklist_all_converted(broker):
    kraft = broker(broker__id=None, zookeeper__connect=None, inter__broker__protocol__version=None,
                   process__roles="broker", node__id=1)
    steps = steps_for("3.9.1", [kraft])
    assert steps[4].state == DONE and steps[6].state == DONE
    assert "<zookeeper.connect>" in steps[2].notes[0]
