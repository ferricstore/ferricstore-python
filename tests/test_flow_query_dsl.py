from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import FrozenInstanceError
from typing import Any

import pytest

from ferricstore import (
    FlowEventField,
    FlowField,
    FlowFields,
    FlowQuery,
    FlowQueryParameter,
    FlowRunMapField,
    flow_param,
)
from ferricstore.flow_query_dsl_types import FlowOrder, FlowPredicate, _FlowBoundValue


class OversizedBindingMapping(Mapping[str, Any]):
    def __getitem__(self, key: str) -> Any:
        raise AssertionError(f"oversized binding accessed {key!r}")

    def __iter__(self) -> Iterator[str]:
        raise AssertionError("oversized binding was iterated")

    def __len__(self) -> int:
        return 1_000_000


def test_builds_deterministic_parameterized_run_collection() -> None:
    query = (
        FlowQuery.runs()
        .where(
            FlowFields.partition_key.eq(flow_param("partition")),
            FlowFields.type.eq("invoice"),
            FlowFields.state.in_("queued", "running"),
            FlowFields.priority.between(1, 5),
            FlowFields.updated_at_ms.from_to(100, 200),
            FlowFields.attribute("customer's.region").is_missing(),
            FlowFields.state_meta("review.v2", "ai.model").is_null(),
        )
        .order_by(FlowFields.updated_at_ms.desc(), FlowFields.priority.asc())
        .limit(25)
        .return_records()
        .bind(partition="tenant:a")
    )

    text, params = query.compile()

    assert text == (
        "FROM runs WHERE partition_key = @partition AND type = @_fql_0 "
        "AND state IN (@_fql_1, @_fql_2) AND priority BETWEEN @_fql_3 AND @_fql_4 "
        "AND updated_at_ms FROM @_fql_5 TO @_fql_6 "
        "AND attribute['customer''s.region'] IS MISSING "
        "AND state_meta['review.v2']['ai.model'] IS NULL "
        "ORDER BY updated_at_ms DESC, priority ASC LIMIT 25 RETURN RECORDS"
    )
    assert params == {
        "partition": "tenant:a",
        "_fql_0": "invoice",
        "_fql_1": "queued",
        "_fql_2": "running",
        "_fql_3": 1,
        "_fql_4": 5,
        "_fql_5": 100,
        "_fql_6": 200,
    }
    assert query.compile() == (text, params)


def test_builds_point_count_and_event_queries() -> None:
    point = FlowQuery.runs().where(FlowFields.run_id.eq("run-1")).return_record()
    count = (
        FlowQuery.runs()
        .where(
            FlowFields.partition_key.eq("tenant"),
            FlowFields.state.eq("failed"),
        )
        .return_count()
    )
    events = (
        FlowQuery.events()
        .where(
            FlowFields.partition_key.eq("tenant"),
            FlowFields.run_id.eq("run-1"),
        )
        .order_by(FlowFields.event_id.desc())
        .limit(20)
        .return_records()
    )

    assert point.compile() == (
        "FROM runs WHERE run_id = @_fql_0 RETURN RECORD",
        {"_fql_0": "run-1"},
    )
    assert count.compile() == (
        "FROM runs WHERE partition_key = @_fql_0 AND state = @_fql_1 RETURN COUNT",
        {"_fql_0": "tenant", "_fql_1": "failed"},
    )
    assert events.compile() == (
        "FROM events WHERE partition_key = @_fql_0 AND run_id = @_fql_1 "
        "ORDER BY event_id DESC LIMIT 20 RETURN RECORDS",
        {"_fql_0": "tenant", "_fql_1": "run-1"},
    )


def test_builds_source_aware_run_and_event_return_projections() -> None:
    run = (
        FlowQuery.runs()
        .where(FlowFields.run_id.eq("run-1"))
        .return_record(
            FlowFields.run_id,
            FlowFields.state,
            FlowFields.attributes,
            FlowFields.state_metadata,
            FlowFields.attribute("customer.tier"),
            FlowFields.state_meta("review's", "risk tier"),
        )
    )
    events = (
        FlowQuery.events()
        .where(FlowFields.run_id.eq("run-1"))
        .order_by(FlowFields.event_id.asc())
        .limit(20)
        .return_records(
            FlowFields.event_id,
            FlowFields.fields,
            FlowFields.event_field("worker's.pool"),
        )
    )

    assert run.compile()[0].endswith(
        "RETURN RECORD (run_id, state, attributes, state_meta, attribute['customer.tier'], "
        "state_meta['review''s']['risk tier'])"
    )
    assert events.compile()[0].endswith(
        "RETURN RECORDS (event_id, fields, fields['worker''s.pool'])"
    )


def test_return_projection_validation_is_bounded_distinct_and_source_aware() -> None:
    run = FlowQuery.runs().where(FlowFields.run_id.eq("run-1"))
    events = (
        FlowQuery.events()
        .where(FlowFields.run_id.eq("run-1"))
        .order_by(FlowFields.event_id.asc())
        .limit(20)
    )

    with pytest.raises(ValueError, match="duplicate projection"):
        run.return_record(FlowFields.state, FlowField("STATE"))
    with pytest.raises(ValueError, match=r"runs.*event_id"):
        run.return_record(FlowFields.event_id)
    with pytest.raises(ValueError, match=r"runs.*event field"):
        run.return_record(FlowFields.fields)
    with pytest.raises(ValueError, match=r"events.*run_id"):
        events.return_records(FlowFields.run_id)
    with pytest.raises(ValueError, match=r"events.*attribute"):
        events.return_records(FlowFields.attribute("tenant"))
    with pytest.raises(ValueError, match=r"events.*attributes"):
        events.return_records(FlowFields.attributes)
    with pytest.raises(TypeError, match="projection fields"):
        run.return_record("state")  # type: ignore[arg-type]

    fields = tuple(FlowFields.attribute(f"field_{index}") for index in range(33))
    with pytest.raises(ValueError, match="at most 32 projection fields"):
        run.return_record(*fields)


@pytest.mark.parametrize("name", ["", "__private", "x" * 65, "\ud800"])
def test_event_projection_field_names_are_validated(name: str) -> None:
    with pytest.raises(ValueError):
        FlowFields.event_field(name)


def test_projection_token_budget_and_count_reset_are_enforced() -> None:
    base = FlowQuery.runs().where(FlowFields.partition_key.eq("tenant"))
    projected = base.return_records(
        *(FlowFields.state_meta(f"state_{index}", "value") for index in range(32))
    )
    projected = projected.order_by(FlowFields.updated_at_ms.asc()).limit(10)

    with pytest.raises(ValueError, match="256 lexical tokens"):
        projected.compile()

    count = base.return_records(FlowFields.state).return_count()
    assert count.compile()[0].endswith("RETURN COUNT")


def test_cursor_page_retains_bound_parameters_without_mutating_original() -> None:
    base = (
        FlowQuery.runs()
        .where(
            FlowFields.partition_key.eq(flow_param("partition")),
            FlowFields.type.eq(flow_param("type")),
        )
        .order_by(FlowFields.updated_at_ms.asc())
        .limit(10)
        .return_records()
        .bind(partition="tenant", type="invoice")
    )

    next_page = base.cursor("fqc1_" + "c" * 11)
    base_text, base_params = base.compile()
    page_text, page_params = next_page.compile()

    assert " CURSOR " not in base_text
    assert " CURSOR @_fql_cursor RETURN RECORDS" in page_text
    assert base_params == {"partition": "tenant", "type": "invoice"}
    assert page_params == {
        "partition": "tenant",
        "type": "invoice",
        "_fql_cursor": "fqc1_" + "c" * 11,
    }


def test_cursor_can_be_bound_after_the_rest_of_the_query() -> None:
    base = (
        FlowQuery.runs()
        .where(FlowFields.partition_key.eq(flow_param("partition")))
        .order_by(FlowFields.updated_at_ms.asc())
        .limit(10)
        .return_records()
        .bind(partition="tenant")
    )

    page = base.cursor(flow_param("page")).bind(page=b"fqc1_" + b"x" * 11)

    assert page.compile()[1] == {"partition": "tenant", "page": b"fqc1_" + b"x" * 11}


def test_query_and_query_components_are_immutable() -> None:
    query = FlowQuery.runs()
    field = FlowFields.run_id
    predicate = field.eq("run-1")
    order = FlowFields.updated_at_ms.asc()

    with pytest.raises(FrozenInstanceError):
        query.source = "events"  # type: ignore[misc]
    with pytest.raises(FrozenInstanceError):
        field.name = "unknown"  # type: ignore[misc]
    with pytest.raises(FrozenInstanceError):
        predicate.operator = "missing"  # type: ignore[misc]
    with pytest.raises(FrozenInstanceError):
        order.direction = "DESC"  # type: ignore[misc]


def test_bind_rejects_declared_oversized_mapping_before_copying() -> None:
    query = FlowQuery.runs().where(FlowFields.run_id.eq(flow_param("id"))).return_record()

    with pytest.raises(ValueError, match="64 parameters"):
        query.bind(OversizedBindingMapping())


def test_public_constructors_defend_runtime_invariants() -> None:
    with pytest.raises(ValueError, match="source"):
        FlowQuery("jobs")  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="predicates"):
        FlowQuery("runs", predicates=("run_id = 'run-1'",))  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="orders"):
        FlowQuery("runs", orders=("updated_at_ms ASC",))  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="return shape"):
        FlowQuery("runs", _return_shape="rows")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="limit"):
        FlowQuery("runs", _result_limit=0)
    with pytest.raises(TypeError, match="field"):
        FlowField(1)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="run map field"):
        FlowRunMapField("payload")
    with pytest.raises(TypeError, match="field"):
        FlowPredicate("run_id", "null")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="predicate"):
        FlowPredicate(FlowFields.run_id, "unknown")
    with pytest.raises(ValueError, match="arity"):
        FlowPredicate(FlowFields.run_id, "eq")
    with pytest.raises(ValueError, match="direction"):
        FlowOrder(FlowFields.updated_at_ms, "UP")
    with pytest.raises(TypeError, match="operands"):
        FlowPredicate(FlowFields.run_id, "eq", ("run-1",))  # type: ignore[arg-type]
    with pytest.raises(ValueError, match=r"IN requires 1\.\.20"):
        FlowPredicate(FlowFields.state, "in")
    with pytest.raises(TypeError, match="FlowField"):
        FlowOrder("updated_at_ms", "ASC")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="metadata field"):
        FlowField("attribute", _state="queued", _metadata_name="owner")
    with pytest.raises(TypeError, match=r"event field.*text"):
        FlowEventField(1)  # type: ignore[arg-type]


def test_flow_query_constructor_defends_aggregate_invariants() -> None:
    predicates = tuple(FlowFields.state.eq(str(index)) for index in range(13))
    with pytest.raises(ValueError, match="at most 12 predicates"):
        FlowQuery("runs", predicates=predicates)

    too_many_orders = (
        FlowFields.updated_at_ms.asc(),
        FlowFields.priority.asc(),
        FlowFields.created_at_ms.asc(),
    )
    with pytest.raises(ValueError, match="at most 2 order fields"):
        FlowQuery("runs", orders=too_many_orders)
    with pytest.raises(ValueError, match="duplicate order field"):
        FlowQuery(
            "runs",
            orders=(FlowFields.updated_at_ms.asc(), FlowFields.updated_at_ms.desc()),
        )
    with pytest.raises(TypeError, match="cursor"):
        FlowQuery("runs", _page_cursor=object())  # type: ignore[arg-type]
    with pytest.raises(ValueError, match=r"COUNT.*projection"):
        FlowQuery("runs", _return_shape="count", _projection=(FlowFields.state,))


def test_bind_and_compile_defend_full_internal_parameter_state() -> None:
    point = FlowQuery.runs().where(FlowFields.run_id.eq("run-1")).return_record()
    with pytest.raises(ValueError, match="at most 64 parameters"):
        point.bind(**{f"value_{index}": index for index in range(65)})

    malformed = FlowQuery(
        "runs",
        predicates=(FlowFields.run_id.eq("run-1"),),
        _return_shape="record",
        _bindings=(("extra", "value"),),
    )
    with pytest.raises(ValueError, match="unknown Flow query binding"):
        malformed.compile()


def test_routing_hint_prefers_partition_then_run_id_and_handles_unscoped_values() -> None:
    partitioned = FlowQuery.runs().where(FlowFields.partition_key.eq("tenant"))
    bound_partition = (
        FlowQuery.runs()
        .where(FlowFields.partition_key.eq(flow_param("partition")))
        .bind(partition=b"tenant")
    )
    by_id = FlowQuery.runs().where(FlowFields.run_id.eq("run-1"))
    unscoped = FlowQuery.runs().where(FlowFields.state.eq("queued"))

    assert partitioned._routing_hint() == ("partition", "tenant")
    assert bound_partition._routing_hint() == ("partition", b"tenant")
    assert by_id._routing_hint() == ("auto_id", "run-1")
    assert unscoped._routing_hint() is None


def test_composition_leaves_prior_queries_unchanged() -> None:
    base = FlowQuery.runs().where(FlowFields.partition_key.eq("tenant"))
    filtered = base.where(FlowFields.state.eq("queued"))
    ordered = filtered.order_by(FlowFields.updated_at_ms.asc())

    assert len(base.predicates) == 1
    assert len(filtered.predicates) == 2
    assert len(ordered.orders) == 1
    assert base.orders == ()


def test_composition_rejects_empty_or_wrong_clause_objects() -> None:
    with pytest.raises(ValueError, match="at least one predicate"):
        FlowQuery.runs().where()
    with pytest.raises(TypeError, match="FlowPredicate"):
        FlowQuery.runs().where("state = 'queued'")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="at least one order"):
        FlowQuery.runs().order_by()
    with pytest.raises(TypeError, match="FlowOrder"):
        FlowQuery.runs().order_by("updated_at_ms ASC")  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="must be a mapping"):
        FlowQuery.runs().bind([])  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "factory,args",
    [
        (FlowFields.attribute, ("",)),
        (FlowFields.attribute, ("__private",)),
        (FlowFields.attribute, ("x" * 65,)),
        (FlowFields.state_meta, ("", "key")),
        (FlowFields.state_meta, ("queued", "__private")),
        (FlowFields.state_meta, ("queued", "x" * 65)),
    ],
)
def test_rejects_invalid_metadata_selectors(factory: object, args: tuple[str, ...]) -> None:
    with pytest.raises(ValueError):
        factory(*args)  # type: ignore[operator]


def test_flow_field_constructor_accepts_only_builtins() -> None:
    assert FlowField("RUN_ID") is not FlowFields.run_id
    assert FlowField("RUN_ID").selector == "run_id"
    with pytest.raises(ValueError, match="unsupported Flow query field"):
        FlowField("payload")
    with pytest.raises(ValueError, match="unsupported Flow query field"):
        FlowField("ſtate")  # noqa: RUF001 - intentional Unicode confusable


def test_query_repr_redacts_literal_and_bound_values() -> None:
    literal_secret = "literal-api-secret"
    bound_secret = "bound-api-secret"
    literal = FlowQuery.runs().where(FlowFields.run_id.eq(literal_secret)).return_record()
    bound = (
        FlowQuery.runs()
        .where(FlowFields.run_id.eq(flow_param("run")))
        .return_record()
        .bind(run=bound_secret)
    )

    assert literal_secret not in repr(literal)
    assert bound_secret not in repr(bound)


def test_metadata_factories_require_text_and_from_to_requires_time() -> None:
    with pytest.raises(TypeError, match="must be text"):
        FlowFields.attribute(1)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="must be text"):
        FlowFields.state_meta(1, "key")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="timestamp"):
        FlowFields.priority.from_to(1, 2)


@pytest.mark.parametrize("name", ["", "bad name", "@name", "a/b", "_fql_0"])
def test_rejects_unsafe_or_reserved_parameter_names(name: str) -> None:
    with pytest.raises(ValueError):
        FlowQueryParameter(name)


def test_parameter_name_requires_text() -> None:
    with pytest.raises(TypeError, match="must be text"):
        FlowQueryParameter(1)  # type: ignore[arg-type]


@pytest.mark.parametrize("name", ["0value", ".value", "-value"])
def test_parameter_names_match_the_full_server_identifier_domain(name: str) -> None:
    query = FlowQuery.runs().where(FlowFields.run_id.eq(flow_param(name))).return_record()

    text, params = query.bind({name: "run-1"}).compile()

    assert f"@{name}" in text
    assert params == {name: "run-1"}


def test_bind_rejects_missing_extra_duplicate_and_invalid_values() -> None:
    query = (
        FlowQuery.runs()
        .where(
            FlowFields.partition_key.eq(flow_param("partition")),
            FlowFields.state.eq(flow_param("state")),
        )
        .return_count()
    )

    with pytest.raises(ValueError, match=r"missing.*state"):
        query.bind(partition="tenant").compile()
    with pytest.raises(ValueError, match=r"unknown.*extra"):
        query.bind(extra="value")
    with pytest.raises(ValueError, match=r"already bound.*partition"):
        query.bind(partition="tenant").bind(partition="other")
    with pytest.raises(TypeError, match="must be text, bytes"):
        query.bind(partition=object())


def test_bind_mapping_detects_keyword_duplicates() -> None:
    query = FlowQuery.runs().where(FlowFields.run_id.eq(flow_param("id"))).return_record()

    with pytest.raises(ValueError, match="provided more than once"):
        query.bind({"id": "run-1"}, id="run-2")
    with pytest.raises(TypeError, match="parameter names must be strings"):
        query.bind({1: "run-1"})  # type: ignore[dict-item]


def test_predicate_and_clause_bounds_are_enforced_eagerly() -> None:
    predicates = tuple(FlowFields.state.eq(str(index)) for index in range(13))
    with pytest.raises(ValueError, match="at most 12 predicates"):
        FlowQuery.runs().where(*predicates)
    with pytest.raises(ValueError, match="at most 20 values"):
        FlowFields.state.in_(*(str(index) for index in range(21)))
    with pytest.raises(ValueError, match="at least one value"):
        FlowFields.state.in_()
    with pytest.raises(ValueError, match="at most 2 order fields"):
        FlowQuery.runs().order_by(
            FlowFields.updated_at_ms.asc(),
            FlowFields.priority.asc(),
            FlowFields.created_at_ms.asc(),
        )
    with pytest.raises(ValueError, match="between 1 and 100"):
        FlowQuery.runs().limit(0)
    with pytest.raises(ValueError, match="between 1 and 100"):
        FlowQuery.runs().limit(True)


def test_ordering_accepts_only_server_orderable_fields_and_no_duplicates() -> None:
    with pytest.raises(ValueError, match="cannot be ordered"):
        FlowFields.state.asc()
    with pytest.raises(ValueError, match="cannot be ordered"):
        FlowFields.attribute("rank").desc()
    with pytest.raises(ValueError, match="duplicate order field"):
        FlowQuery.runs().order_by(FlowFields.updated_at_ms.asc(), FlowFields.updated_at_ms.desc())


def test_compile_rejects_invalid_server_shapes_locally() -> None:
    invalid_queries = [
        FlowQuery.runs().return_records(),
        FlowQuery.runs().where(FlowFields.partition_key.eq("p")).return_records(),
        (
            FlowQuery.runs()
            .where(FlowFields.partition_key.eq("p"))
            .order_by(FlowFields.updated_at_ms.asc())
            .return_records()
        ),
        FlowQuery.runs().where(FlowFields.run_id.eq("r")).return_count(),
        FlowQuery.events().where(FlowFields.run_id.eq("r")).return_count(),
        (
            FlowQuery.events()
            .where(FlowFields.run_id.eq("r"))
            .order_by(FlowFields.updated_at_ms.asc())
            .limit(10)
            .return_records()
        ),
        FlowQuery.events().where(FlowFields.run_id.eq("r")).return_record(),
        (
            FlowQuery.events()
            .where(FlowFields.run_id.eq("r"), FlowFields.state.eq("queued"))
            .order_by(FlowFields.event_id.asc())
            .limit(10)
            .return_records()
        ),
        (
            FlowQuery.runs()
            .where(FlowFields.partition_key.eq("p"))
            .order_by(FlowFields.event_id.asc())
            .limit(10)
            .return_records()
        ),
    ]

    for query in invalid_queries:
        with pytest.raises(ValueError, match="query shape"):
            query.compile()


@pytest.mark.parametrize(
    "predicates",
    [
        (
            FlowFields.partition_key.eq("p"),
            FlowFields.run_id.eq("r"),
        ),
        (
            FlowFields.run_id.eq("r"),
            FlowFields.partition_key.eq("p"),
        ),
    ],
)
def test_partition_and_run_id_collection_is_rejected_in_either_order(
    predicates: tuple[Any, ...],
) -> None:
    query = (
        FlowQuery.runs()
        .where(*predicates)
        .order_by(FlowFields.updated_at_ms.asc())
        .limit(10)
        .return_records()
    )

    with pytest.raises(ValueError, match="use RETURN RECORD"):
        query.compile()


def test_compile_validates_bound_values_against_field_types() -> None:
    invalid_queries = [
        (
            FlowQuery.runs()
            .where(FlowFields.partition_key.eq(flow_param("partition")))
            .return_count()
            .bind(partition=1)
        ),
        (
            FlowQuery.runs()
            .where(
                FlowFields.partition_key.eq("p"),
                FlowFields.priority.eq(flow_param("priority")),
            )
            .return_count()
            .bind(priority="high")
        ),
    ]

    for query in invalid_queries:
        with pytest.raises(TypeError, match="incompatible"):
            query.compile()


def test_compile_rejects_duplicate_in_values_and_reversed_ranges() -> None:
    duplicate = (
        FlowQuery.runs()
        .where(
            FlowFields.partition_key.eq("p"),
            FlowFields.state.in_(flow_param("first"), flow_param("second")),
        )
        .return_count()
        .bind(first="failed", second="failed")
    )
    reversed_range = (
        FlowQuery.runs()
        .where(
            FlowFields.partition_key.eq("p"),
            FlowFields.priority.between(flow_param("lower"), flow_param("upper")),
        )
        .return_count()
        .bind(lower=10, upper=1)
    )

    with pytest.raises(ValueError, match="duplicate IN"):
        duplicate.compile()
    with pytest.raises(ValueError, match="lower bound"):
        reversed_range.compile()


def test_dynamic_ranges_support_each_wire_scalar_domain() -> None:
    for lower, upper in [(b"a", b"z"), (False, True), (1, 2), (1.0, 2.0)]:
        query = (
            FlowQuery.runs()
            .where(
                FlowFields.partition_key.eq("p"),
                FlowFields.attribute("rank").between(lower, upper),
            )
            .return_count()
        )
        assert "attribute['rank'] BETWEEN" in query.compile()[0]

    incompatible = (
        FlowQuery.runs()
        .where(
            FlowFields.partition_key.eq("p"),
            FlowFields.attribute("rank").between(1, "two"),
        )
        .return_count()
    )
    with pytest.raises(TypeError, match="incompatible value types"):
        incompatible.compile()


def test_keyword_and_dynamic_value_size_contracts_are_local() -> None:
    empty_keyword = FlowQuery.runs().where(FlowFields.run_id.eq("")).return_record()
    maximum_run_id = FlowQuery.runs().where(FlowFields.run_id.eq("x" * 65_483)).return_record()
    oversized_run_id = FlowQuery.runs().where(FlowFields.run_id.eq("x" * 65_484)).return_record()
    with pytest.raises(ValueError, match="65535 bytes"):
        FlowFields.partition_key.eq("x" * 65_536)
    oversized_dynamic = (
        FlowQuery.runs()
        .where(
            FlowFields.partition_key.eq("p"),
            FlowFields.attribute("note").eq("x" * 1_025),
        )
        .return_count()
    )
    oversized_multibyte_dynamic = (
        FlowQuery.runs()
        .where(
            FlowFields.partition_key.eq("p"),
            FlowFields.attribute("note").eq("é" * 600),
        )
        .return_count()
    )

    assert maximum_run_id.compile()[1]["_fql_0"] == "x" * 65_483
    for query in (
        empty_keyword,
        oversized_run_id,
        oversized_dynamic,
        oversized_multibyte_dynamic,
    ):
        with pytest.raises(ValueError, match="bytes"):
            query.compile()


def test_malformed_internal_range_operand_still_fails_closed() -> None:
    malformed = FlowPredicate(
        FlowFields.attribute("rank"),
        "between",
        (_FlowBoundValue(object()), _FlowBoundValue(object())),
    )
    query = FlowQuery.runs().where(FlowFields.partition_key.eq("p"), malformed).return_count()

    with pytest.raises(TypeError, match="not comparable"):
        query.compile()


def test_field_metadata_marker_matches_selector_kind() -> None:
    assert FlowFields.attribute("owner").is_metadata
    assert FlowFields.state_meta("queued", "owner").is_metadata
    assert not FlowFields.state.is_metadata


def test_point_read_rejects_extra_equality_predicates() -> None:
    query = (
        FlowQuery.runs()
        .where(FlowFields.run_id.eq("run-1"), FlowFields.state.eq("queued"))
        .return_record()
    )

    with pytest.raises(ValueError, match="point read requires run_id"):
        query.compile()


def test_cursor_bounds_are_checked_before_execution() -> None:
    query = (
        FlowQuery.runs()
        .where(FlowFields.partition_key.eq("p"))
        .order_by(FlowFields.updated_at_ms.asc())
        .limit(10)
        .return_records()
    )

    with pytest.raises(ValueError, match=r"16\.\.4096 bytes"):
        query.cursor("short").compile()
    with pytest.raises(ValueError, match=r"16\.\.4096 bytes"):
        query.cursor(b"x" * 4097).compile()
    with pytest.raises(ValueError, match="fqc1_"):
        query.cursor("x" * 16).compile()
    with pytest.raises(TypeError, match="cursor must be text or bytes"):
        query.cursor(123).compile()

    assert "CURSOR @_fql_cursor" in query.cursor("fqc1_" + "x" * 11).compile()[0]


def test_compile_enforces_server_lexical_token_ceiling() -> None:
    values = tuple(flow_param(f"value_{index}") for index in range(20))
    predicates = [FlowFields.partition_key.eq(flow_param("partition"))]
    predicates.extend(
        FlowFields.state_meta(f"state_{index}", "field").in_(*values) for index in range(11)
    )
    query = (
        FlowQuery.runs()
        .where(*predicates)
        .order_by(FlowFields.updated_at_ms.asc())
        .limit(10)
        .return_records()
        .bind(
            partition="tenant",
            **{f"value_{index}": f"value-{index}" for index in range(20)},
        )
    )

    with pytest.raises(ValueError, match="256 lexical tokens"):
        query.compile()


def test_compile_enforces_aggregate_generated_and_named_parameter_limit() -> None:
    named_states = tuple(flow_param(f"state_{index}") for index in range(20))
    base = (
        FlowQuery.runs()
        .where(
            FlowFields.partition_key.eq("tenant"),
            FlowFields.state.in_(*named_states),
            FlowFields.type.in_(*(f"type-{index}" for index in range(20))),
            FlowFields.run_state.in_(*(f"run-state-{index}" for index in range(20))),
        )
        .order_by(FlowFields.updated_at_ms.asc())
        .limit(10)
        .return_records()
        .bind(**{f"state_{index}": f"state-{index}" for index in range(20)})
    )

    at_limit = base.where(FlowFields.parent_flow_id.in_("parent-0", "parent-1", "parent-2"))
    over_limit = base.where(
        FlowFields.parent_flow_id.in_("parent-0", "parent-1", "parent-2", "parent-3")
    )

    assert len(at_limit.compile()[1]) == 64
    with pytest.raises(ValueError, match="at most 64 parameters"):
        over_limit.compile()


def test_compile_counts_generated_cursor_in_aggregate_parameter_limit() -> None:
    base = (
        FlowQuery.runs()
        .where(
            FlowFields.partition_key.eq("tenant"),
            FlowFields.state.in_(*(f"state-{index}" for index in range(20))),
            FlowFields.type.in_(*(f"type-{index}" for index in range(20))),
            FlowFields.run_state.in_(*(f"run-state-{index}" for index in range(20))),
        )
        .order_by(FlowFields.updated_at_ms.asc())
        .limit(10)
        .return_records()
    )

    at_limit = base.where(FlowFields.parent_flow_id.in_("parent-0", "parent-1")).cursor(
        "fqc1_" + "x" * 11
    )
    over_limit = base.where(
        FlowFields.parent_flow_id.in_("parent-0", "parent-1", "parent-2")
    ).cursor("fqc1_" + "x" * 11)

    assert len(at_limit.compile()[1]) == 64
    with pytest.raises(ValueError, match="at most 64 parameters"):
        over_limit.compile()


def test_public_exports_are_lazy_and_discoverable() -> None:
    import ferricstore

    for name in (
        "FlowEventField",
        "FlowField",
        "FlowFields",
        "FlowQuery",
        "FlowQueryParameter",
        "FlowRunMapField",
        "flow_param",
    ):
        assert name in ferricstore.__all__
        assert name in dir(ferricstore)
