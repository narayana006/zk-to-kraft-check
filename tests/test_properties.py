from zk_to_kraft_check.properties import BrokerConfig, load_broker_config, parse_properties


def test_basic_pairs_comments_and_blank_lines():
    text = "# comment\n! also a comment\n\nbroker.id=3\nlisteners = PLAINTEXT://:9092\n"
    assert parse_properties(text) == {"broker.id": "3", "listeners": "PLAINTEXT://:9092"}


def test_value_keeps_later_separators():
    props = parse_properties("listener.security.protocol.map=A:PLAINTEXT,B:SSL\nsasl.jaas.config=x required a=\"b\";\n")
    assert props["listener.security.protocol.map"] == "A:PLAINTEXT,B:SSL"
    assert props["sasl.jaas.config"] == 'x required a="b";'


def test_colon_and_whitespace_separators():
    assert parse_properties("a:1\nb 2\nc = 3\n") == {"a": "1", "b": "2", "c": "3"}


def test_line_continuation():
    text = "log.dirs=/data/a,\\\n    /data/b\nbroker.id=1\n"
    assert parse_properties(text) == {"log.dirs": "/data/a,/data/b", "broker.id": "1"}


def test_last_duplicate_wins_and_empty_value():
    assert parse_properties("a=1\na=2\nb=\n") == {"a": "2", "b": ""}


def test_broker_config_accessors():
    cfg = BrokerConfig("x", {"flag": "TRUE", "n": "12", "bad": "x", "items": "a, b ,,c", "empty": ""})
    assert cfg.get_bool("flag") is True and cfg.get_bool("missing", True) is True
    assert cfg.get_int("n") == 12 and cfg.get_int("bad") is None and cfg.get_int("missing") is None
    assert cfg.get_list("items") == ["a", "b", "c"] and cfg.get_list("missing") == []
    assert cfg.has("empty") is False


def test_load_from_file(tmp_path):
    path = tmp_path / "server.properties"
    path.write_text("broker.id=7\n")
    cfg = load_broker_config(path)
    assert cfg.name == str(path) and cfg.get_int("broker.id") == 7
