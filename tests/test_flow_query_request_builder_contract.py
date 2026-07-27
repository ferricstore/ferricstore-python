from __future__ import annotations

import math
from collections.abc import Iterator, Mapping
from typing import Any

import pytest

import ferricstore.flow_query_builder as query_builder
from ferricstore.flow_query_builder import (
    MAX_FLOW_QUERY_PARTITION_BYTES,
    MAX_FLOW_QUERY_RESULTS,
    MAX_FLOW_QUERY_TIME,
    build_flow_lineage_query,
    build_flow_list_query,
    build_flow_search_query,
    build_flow_stuck_query,
    build_flow_terminal_query,
)
from ferricstore.flow_query_request import (
    FLOW_QUERY_LANGUAGE_VERSION,
    FLOW_QUERY_MAX_BYTES,
    FLOW_QUERY_MAX_PARAMETER_NAME_BYTES,
    FLOW_QUERY_MAX_PARAMETERS,
    _with_flow_query_command_options,
    build_flow_query_args,
    build_flow_query_payload,
    has_explain_prefix,
    normalize_flow_query_parameter,
    normalize_flow_query_params,
    validate_flow_query_index_id,
    validate_flow_query_parameter_name,
    validate_flow_query_text,
)
from ferricstore.flow_query_selectors import normalize_flow_selector_segment

QUERY = "FROM runs WHERE partition_key = @partition RETURN COUNT"


def _list_query(**overrides: Any) -> tuple[str, dict[str, Any]]:
    options: dict[str, Any] = {
        "partition_key": "tenant-a",
        "state": None,
        "count": None,
        "from_ms": None,
        "to_ms": None,
        "rev": None,
        "attributes": None,
        "include_cold": None,
        "consistent_projection": None,
    }
    flow_type = overrides.pop("flow_type", "invoice")
    options.update(overrides)
    return build_flow_list_query(flow_type, **options)


def _search_query(**overrides: Any) -> tuple[str, dict[str, Any]]:
    options: dict[str, Any] = {
        "partition_key": "tenant-a",
        "state": None,
        "count": None,
        "from_ms": None,
        "to_ms": None,
        "rev": None,
        "attributes": {"tenant": "acme"},
        "state_meta": None,
        "terminal_only": None,
        "include_cold": None,
        "consistent_projection": None,
    }
    flow_type = overrides.pop("flow_type", "invoice")
    options.update(overrides)
    return build_flow_search_query(flow_type, **options)


class LengthOnlyMapping(Mapping[str, int]):
    """Report a known oversized length and fail if rejection iterates the mapping."""

    def __init__(self, length: int) -> None:
        self.length = length

    def __getitem__(self, key: str) -> int:
        raise AssertionError(f"oversized mapping accessed key {key!r}")

    def __iter__(self) -> Iterator[str]:
        raise AssertionError("oversized mapping was iterated")

    def __len__(self) -> int:
        return self.length


class UnderreportedMapping(Mapping[str, int]):
    def __getitem__(self, key: str) -> int:
        return int(key.removeprefix("p"))

    def __iter__(self) -> Iterator[str]:
        return iter(f"p{index}" for index in range(FLOW_QUERY_MAX_PARAMETERS + 1))

    def __len__(self) -> int:
        return 1


class UnderreportedMetadataMapping(Mapping[str, Any]):
    """Yield one entry over the predicate budget, then fail on further iteration."""

    def __init__(self, value: Any) -> None:
        self.value = value

    def __getitem__(self, key: str) -> Any:
        return self.value

    def __iter__(self) -> Iterator[str]:
        yield from (f"m{index}" for index in range(13))
        raise AssertionError("metadata iteration exceeded its streaming limit")

    def __len__(self) -> int:
        return 1


class EncodeBomb(str):
    def encode(self, *_args: Any, **_kwargs: Any) -> bytes:
        raise AssertionError("oversized text must be rejected before UTF-8 encoding")


def test_parameter_normalization_enforces_streaming_limit_for_lying_mapping() -> None:
    with pytest.raises(ValueError, match="64 parameters"):
        normalize_flow_query_params(UnderreportedMapping())


def test_metadata_normalization_enforces_streaming_limit_for_lying_mappings() -> None:
    with pytest.raises(ValueError, match="at most 12 predicates"):
        _list_query(attributes=UnderreportedMetadataMapping(1))

    with pytest.raises(ValueError, match="at most 12 predicates"):
        _search_query(
            attributes=None,
            state_meta=UnderreportedMetadataMapping({"risk": 1}),
        )


def test_raw_query_payload_accepts_bytes_and_deadline_without_parameters() -> None:
    option = _with_flow_query_command_options(
        build_flow_query_args(QUERY),
        deadline_ms=123,
        routing_key=None,
    )[-1]

    assert build_flow_query_payload([b"FQL1", QUERY.encode()]) == {
        "version": "FQL1",
        "query": QUERY,
    }
    assert build_flow_query_payload(["FQL1", QUERY, option]) == {
        "version": "FQL1",
        "query": QUERY,
        "deadline_ms": 123,
    }


def test_public_query_argument_builder_is_wire_only() -> None:
    assert build_flow_query_args(QUERY, {"item": "value"}) == [
        "FLOW.QUERY",
        "FQL1",
        QUERY,
        "item",
        "value",
    ]
    with pytest.raises(TypeError):
        build_flow_query_args(QUERY, deadline_ms=123)  # type: ignore[call-arg]
    with pytest.raises(TypeError):
        build_flow_query_args(QUERY, routing_key="route")  # type: ignore[call-arg]
    assert build_flow_query_payload([b"FQL1", QUERY.encode(), b"item", b"value"]) == {
        "version": "FQL1",
        "query": QUERY,
        "params": {"item": b"value"},
    }


@pytest.mark.parametrize(
    ("args", "exception", "message"),
    [
        ([], ValueError, "requires version and query"),
        (["FQL1"], ValueError, "requires version and query"),
        (["FQL1", QUERY, "dangling"], ValueError, "name/value pairs"),
        (["FQL0", QUERY], ValueError, "requires version FQL1"),
        ([1, QUERY], TypeError, "version must be text"),
        ([b"\xff", QUERY], ValueError, "version must be valid UTF-8"),
        (["FQL1", 1], TypeError, "query must be text"),
        (["FQL1", b"\xff"], ValueError, "query must be valid UTF-8"),
        (["FQL1", QUERY, 1, "value"], TypeError, "parameter name must be text"),
        (["FQL1", QUERY, b"\xff", "value"], ValueError, "name must be valid UTF-8"),
        (["FQL1", QUERY, "same", 1, "same", 2], ValueError, "duplicated"),
    ],
)
def test_raw_query_payload_rejects_malformed_structure_and_text(
    args: list[Any], exception: type[Exception], message: str
) -> None:
    with pytest.raises(exception, match=message):
        build_flow_query_payload(args)


def test_raw_query_payload_rejects_misplaced_options_and_too_many_params() -> None:
    option = _with_flow_query_command_options(
        build_flow_query_args(QUERY),
        deadline_ms=123,
        routing_key=None,
    )[-1]
    with pytest.raises(ValueError, match="options must be last"):
        build_flow_query_payload(["FQL1", option, QUERY])

    args: list[Any] = [FLOW_QUERY_LANGUAGE_VERSION, QUERY]
    for index in range(FLOW_QUERY_MAX_PARAMETERS + 1):
        args.extend((f"p{index}", index))
    with pytest.raises(ValueError, match="at most 64 parameters"):
        build_flow_query_payload(args)


def test_parameter_mapping_size_is_rejected_before_iteration() -> None:
    params = LengthOnlyMapping(FLOW_QUERY_MAX_PARAMETERS + 1)

    with pytest.raises(ValueError, match="at most 64 parameters"):
        normalize_flow_query_params(params)


@pytest.mark.parametrize("params", [[], ["name", "value"], "name=value"])
def test_parameter_collection_must_be_a_mapping(params: Any) -> None:
    with pytest.raises(TypeError, match="params must be a mapping"):
        normalize_flow_query_params(params)


def test_parameter_names_enforce_type_utf8_and_byte_bounds() -> None:
    assert normalize_flow_query_params({"name": 1}) == {"name": 1}
    with pytest.raises(TypeError, match="names must be strings"):
        normalize_flow_query_params({1: "value"})

    validate_flow_query_parameter_name("x" * FLOW_QUERY_MAX_PARAMETER_NAME_BYTES)
    for name in ("0value", ".value", "-value", "_value"):
        validate_flow_query_parameter_name(name)
    for name in ("", "x" * (FLOW_QUERY_MAX_PARAMETER_NAME_BYTES + 1)):
        with pytest.raises(ValueError, match=r"1\.\.128 bytes"):
            validate_flow_query_parameter_name(name)
    for name in ("has space", "é", "@value", "value/part"):
        with pytest.raises(ValueError, match="ASCII"):
            validate_flow_query_parameter_name(name)
    with pytest.raises(ValueError, match="valid UTF-8"):
        validate_flow_query_parameter_name("\ud800")


def test_parameter_values_accept_only_protocol_scalars_and_signed_integers() -> None:
    accepted = (
        "text",
        b"bytes",
        True,
        False,
        -(2**63),
        2**63 - 1,
        -1.5,
        0.0,
    )
    for value in accepted:
        assert normalize_flow_query_parameter(value, name="value") == value

    rejected = (-(2**63) - 1, 2**63, math.inf, -math.inf, math.nan, None, object())
    for value in rejected:
        with pytest.raises(TypeError, match="signed 64-bit integer"):
            normalize_flow_query_parameter(value, name="value")

    with pytest.raises(ValueError, match="valid UTF-8"):
        normalize_flow_query_parameter("\ud800", name="value")


@pytest.mark.parametrize("value", ["x" * 65_535, b"x" * 65_535])
def test_raw_query_parameter_accepts_the_largest_legal_binary(value: str | bytes) -> None:
    assert normalize_flow_query_parameter(value, name="value") == value


@pytest.mark.parametrize("value", ["x" * 65_536, b"x" * 65_536])
def test_raw_query_parameter_rejects_oversized_binary_before_io(value: str | bytes) -> None:
    with pytest.raises(ValueError, match="65535 bytes"):
        normalize_flow_query_parameter(value, name="value")


def test_oversized_text_is_rejected_before_temporary_utf8_allocation() -> None:
    checks = (
        lambda: normalize_flow_query_parameter(EncodeBomb("x" * 65_536), name="value"),
        lambda: validate_flow_query_text(EncodeBomb("x" * (FLOW_QUERY_MAX_BYTES + 1))),
        lambda: validate_flow_query_parameter_name(
            EncodeBomb("x" * (FLOW_QUERY_MAX_PARAMETER_NAME_BYTES + 1))
        ),
        lambda: normalize_flow_selector_segment(
            EncodeBomb("x" * 65),
            "metadata",
            maximum_bytes=64,
            reject_reserved=True,
        ),
        lambda: query_builder._required_partition(EncodeBomb("x" * 65_536)),
    )

    for check in checks:
        with pytest.raises(ValueError):
            check()


def test_query_text_enforces_type_utf8_nonempty_and_encoded_byte_bound() -> None:
    validate_flow_query_text("é" * (FLOW_QUERY_MAX_BYTES // 2))

    with pytest.raises(TypeError, match="query must be text"):
        validate_flow_query_text(b"RETURN COUNT")  # type: ignore[arg-type]
    for query in ("", " \t\n"):
        with pytest.raises(ValueError, match="must not be empty"):
            validate_flow_query_text(query)
    with pytest.raises(ValueError, match="valid UTF-8"):
        validate_flow_query_text("\ud800")
    with pytest.raises(ValueError, match="exceeds"):
        validate_flow_query_text("é" * (FLOW_QUERY_MAX_BYTES // 2 + 1))


@pytest.mark.parametrize(
    ("index_id", "valid"),
    [
        ("index.v1:active-2", True),
        ("x" * 64, True),
        ("", False),
        ("x" * 65, False),
        ("has space", False),
        ("é", False),
    ],
)
def test_query_index_id_accepts_only_the_wire_identifier_domain(index_id: str, valid: bool) -> None:
    if valid:
        validate_flow_query_index_id(index_id)
    else:
        with pytest.raises(ValueError, match="query index id"):
            validate_flow_query_index_id(index_id)

    with pytest.raises(TypeError, match="must be text"):
        validate_flow_query_index_id(1)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        ("EXPLAIN", True),
        ("  explain\tFROM runs", True),
        ("ExPlAiN\nFROM runs", True),
        ("\u00a0EXPLAIN FROM runs", False),
        ("EXPLA\u0131N FROM runs", False),
        ("explained FROM runs", False),
        ("explain_analyze FROM runs", False),
        ("FROM runs", False),
        ("", False),
    ],
)
def test_explain_prefix_requires_a_complete_leading_keyword(query: str, expected: bool) -> None:
    assert has_explain_prefix(query) is expected


def test_list_builder_covers_defaults_any_filters_and_open_time_windows() -> None:
    query, params = _list_query()
    assert "type = @type AND state = @state" in query
    assert "ORDER BY updated_at_ms ASC LIMIT 100" in query
    assert params["state"] == "queued"

    query, params = _list_query(
        flow_type="any",
        state="any",
        attributes={"kind": "invoice"},
        from_ms=10,
        rev=True,
    )
    assert "type =" not in query and "state =" not in query
    assert "updated_at_ms BETWEEN @from_ms AND @to_ms" in query
    assert "ORDER BY updated_at_ms DESC" in query
    assert params["from_ms"] == 10
    assert params["to_ms"] == MAX_FLOW_QUERY_TIME

    _, params = _list_query(to_ms=20)
    assert params["from_ms"] == 0
    assert params["to_ms"] == 20


def test_builder_rejects_reversed_and_invalid_time_bounds() -> None:
    with pytest.raises(ValueError, match="must not exceed"):
        _list_query(from_ms=2, to_ms=1)

    for name, value in (("from_ms", -1), ("to_ms", MAX_FLOW_QUERY_TIME + 1)):
        with pytest.raises(ValueError, match=name):
            _list_query(**{name: value})
    with pytest.raises(ValueError, match="older_than_ms must not exceed"):
        build_flow_stuck_query(
            "invoice",
            partition_key="tenant-a",
            count=None,
            older_than_ms=2,
            now_ms=1,
        )


def test_builder_enforces_partition_count_and_boolean_types() -> None:
    _list_query(partition_key=b"tenant-a", count=MAX_FLOW_QUERY_RESULTS, rev=False)
    _list_query(partition_key=b"x" * MAX_FLOW_QUERY_PARTITION_BYTES)

    for partition in (None, 1, b"", b"x" * (MAX_FLOW_QUERY_PARTITION_BYTES + 1)):
        with pytest.raises(ValueError, match="partition_key"):
            _list_query(partition_key=partition)
    with pytest.raises(ValueError, match="valid UTF-8"):
        _list_query(partition_key="\ud800")

    for count in (0, MAX_FLOW_QUERY_RESULTS + 1, True, "1"):
        with pytest.raises(ValueError, match="count"):
            _list_query(count=count)
    for option in ("rev", "include_cold", "consistent_projection"):
        with pytest.raises(TypeError, match=option):
            _list_query(**{option: 1})
    with pytest.raises(ValueError, match="include_cold"):
        _list_query(include_cold=True)
    with pytest.raises(ValueError, match="consistent_projection"):
        _list_query(consistent_projection=True)


def test_builder_enforces_required_types_states_and_lineage_options() -> None:
    for flow_type in ("", 1):
        with pytest.raises(ValueError, match="flow type"):
            _list_query(flow_type=flow_type)
    with pytest.raises(ValueError, match="state"):
        _list_query(state="")

    with pytest.raises(ValueError, match="terminal state"):
        build_flow_terminal_query("invoice", partition_key="tenant-a", state="queued")
    with pytest.raises(ValueError, match="concrete flow type"):
        build_flow_terminal_query("any", partition_key="tenant-a")

    with pytest.raises(ValueError, match="terminal_only"):
        build_flow_lineage_query(
            "parent_flow_id",
            "parent-1",
            partition_key="tenant-a",
            terminal_only=True,
        )
    with pytest.raises(ValueError, match="attribute"):
        build_flow_lineage_query(
            "parent_flow_id",
            "parent-1",
            partition_key="tenant-a",
            attributes={"tenant": "acme"},
        )
    with pytest.raises(ValueError, match="lineage id"):
        build_flow_lineage_query("parent_flow_id", "", partition_key="tenant-a")


@pytest.mark.parametrize(
    "build",
    [
        lambda: _list_query(flow_type="x" * 1_025),
        lambda: _list_query(state="x" * 1_025),
        lambda: _search_query(attributes={"tenant": "x" * 1_025}),
        lambda: _search_query(
            attributes=None,
            state_meta={"queued": {"owner": b"x" * 1_025}},
        ),
        lambda: build_flow_lineage_query(
            "parent_flow_id",
            "x" * 1_025,
            partition_key="tenant-a",
        ),
    ],
)
def test_builder_enforces_server_field_value_byte_limit(build: Any) -> None:
    with pytest.raises(ValueError, match="1024 bytes"):
        build()


def test_terminal_search_and_lineage_compile_each_supported_state_shape() -> None:
    terminal, terminal_params = build_flow_terminal_query(
        "invoice", partition_key="tenant-a", state="any"
    )
    assert "state IN" in terminal
    assert terminal_params["terminal_2"] == "cancelled"

    completed, completed_params = build_flow_terminal_query(
        "invoice", partition_key="tenant-a", state="completed"
    )
    assert "state = @state" in completed
    assert completed_params["state"] == "completed"

    searched, _ = _search_query(state="any", terminal_only=True)
    assert "state IN" in searched
    searched, _ = _search_query(state="", terminal_only=False)
    assert "state =" not in searched
    with pytest.raises(TypeError, match="terminal_only"):
        _search_query(terminal_only=1)

    lineage, lineage_params = build_flow_lineage_query(
        "root_flow_id",
        "root-1",
        partition_key="tenant-a",
        state="failed",
        rev=True,
    )
    assert "root_flow_id = @lineage_id" in lineage
    assert "state = @state" in lineage
    assert "ORDER BY updated_at_ms DESC" in lineage
    assert lineage_params["state"] == "failed"

    any_state, any_state_params = build_flow_lineage_query(
        "root_flow_id",
        "root-1",
        partition_key="tenant-a",
        state="any",
    )
    assert "state =" not in any_state
    assert "state" not in any_state_params


def test_search_builder_validates_flat_nested_and_mixed_state_metadata() -> None:
    empty, _ = _search_query(state_meta={})
    assert "attribute['tenant']" in empty

    flat, flat_params = _search_query(
        state="queued",
        attributes=None,
        state_meta={"risk": 3},
    )
    assert "state_meta['queued']['risk'] = @state_meta_0" in flat
    assert flat_params["state_meta_0"] == 3

    nested, nested_params = _search_query(
        attributes=None,
        state_meta={"running": {"owner": "worker"}, "queued": {"risk": 3}},
    )
    assert nested.index("state_meta['queued']") < nested.index("state_meta['running']")
    assert nested_params == {
        "partition_key": "tenant-a",
        "type": "invoice",
        "state_meta_0": 3,
        "state_meta_1": "worker",
    }

    with pytest.raises(ValueError, match="search requires"):
        _search_query(attributes=None, state_meta=None)
    with pytest.raises(ValueError, match="at least one metadata predicate"):
        _search_query(attributes=None, state_meta={"queued": {}})
    with pytest.raises(TypeError, match="cannot mix"):
        _search_query(
            attributes=None,
            state_meta={"queued": {"risk": 3}, "owner": "worker"},
        )
    with pytest.raises(ValueError, match="concrete state"):
        _search_query(attributes=None, state_meta={"risk": 3})
    with pytest.raises(TypeError, match="must be a mapping"):
        _search_query(state_meta=[])  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="concrete flow type"):
        _search_query(
            flow_type="any",
            attributes=None,
            state_meta={"queued": {"risk": 3}},
        )

    broad, _ = _search_query(flow_type="any", attributes={"tenant": "acme"})
    assert "type =" not in broad


@pytest.mark.parametrize(
    "attributes",
    [
        [("key", "value")],
        {1: "value"},
        {"\ud800": "value"},
        {"": "value"},
        {"__reserved": "value"},
        {"x" * 65: "value"},
    ],
)
def test_builder_rejects_invalid_metadata_mappings_and_names(attributes: Any) -> None:
    with pytest.raises((TypeError, ValueError), match=r"metadata|attribute"):
        _search_query(attributes=attributes)


@pytest.mark.parametrize("state", [1, "\ud800", "", "x" * 65])
def test_builder_rejects_invalid_state_metadata_state_names(state: Any) -> None:
    with pytest.raises((TypeError, ValueError), match="state_meta state"):
        _search_query(attributes=None, state_meta={state: {"risk": 3}})


def test_metadata_capacity_rejects_before_iteration_and_before_extra_window() -> None:
    with pytest.raises(ValueError, match="at most 12 predicates"):
        _list_query(attributes=LengthOnlyMapping(10))

    accepted, _ = _list_query(
        attributes={f"key{index}": index for index in range(8)},
        from_ms=1,
    )
    assert "attribute['key7'] = @attribute_7" in accepted
    assert "updated_at_ms BETWEEN @from_ms AND @to_ms" in accepted

    with pytest.raises(ValueError, match="at most 12 predicates"):
        _list_query(
            attributes={f"key{index}": index for index in range(9)},
            from_ms=1,
        )


def test_state_metadata_capacity_rejects_each_inner_mapping_before_iteration() -> None:
    state_meta: dict[str, Mapping[str, int]] = {
        "queued": {f"q{index}": index for index in range(6)},
        "running": LengthOnlyMapping(5),
    }

    with pytest.raises(ValueError, match="at most 12 predicates"):
        _search_query(attributes=None, state_meta=state_meta)


def test_internal_state_metadata_defense_rejects_non_mapping_values() -> None:
    builder = query_builder._FlowCollectionQuery("tenant-a", 100, False)

    with pytest.raises(TypeError, match="map states to metadata mappings"):
        builder.state_metadata({"queued": 1})  # type: ignore[dict-item]


def test_stuck_query_uses_clock_default_and_optional_zero_age(monkeypatch: Any) -> None:
    monkeypatch.setattr(query_builder.time, "time", lambda: 123.456)

    query, params = build_flow_stuck_query(
        "invoice",
        partition_key="tenant-a",
        count=None,
        older_than_ms=None,
        now_ms=None,
    )

    assert "ORDER BY lease_deadline_ms ASC" in query
    assert params["lease_from_ms"] == 0
    assert params["lease_to_ms"] == 123_456
