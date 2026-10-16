from __future__ import annotations

import asyncio

from ferricstore import AsyncFlowClient, FlowClient
from ferricstore.protocol import ProtocolCommand, build_protocol_command
from ferricstore.protocol_retry import request_may_mutate

_FEATURES_REPLY = "supported: 1\r\nactive: 0\r\nshard_0: 0\r\nshard_1: 0"


class _RecordingExecutor:
    def __init__(self, reply):
        self.calls = []
        self.reply = reply

    def execute_command(self, *args):
        self.calls.append(args)
        return self.reply


class _RecordingAsyncExecutor(_RecordingExecutor):
    async def execute_command(self, *args):
        self.calls.append(args)
        return self.reply


def test_cluster_feature_commands_use_their_native_opcodes():
    assert build_protocol_command("CLUSTER.FEATURES") == ProtocolCommand(0x0311, {"args": []}, 1)
    assert build_protocol_command("CLUSTER.FEATURES.RAISE", "1") == ProtocolCommand(
        0x0312, {"args": ["1"]}, 1
    )


def test_cluster_features_is_read_only_and_raise_is_a_mutation():
    features = build_protocol_command("CLUSTER.FEATURES")
    raise_level = build_protocol_command("CLUSTER.FEATURES.RAISE", "1")

    assert request_may_mutate(features.opcode, features.payload) is False
    assert request_may_mutate(raise_level.opcode, raise_level.payload) is True


def test_cluster_features_reports_feature_levels():
    executor = _RecordingExecutor(_FEATURES_REPLY)
    client = FlowClient(executor)

    assert client.cluster_features() == {
        "supported": 1,
        "active": 0,
        "shard_0": 0,
        "shard_1": 0,
    }
    assert executor.calls[-1] == ("CLUSTER.FEATURES",)

    client.cluster_features_raise(1)
    assert executor.calls[-1] == ("CLUSTER.FEATURES.RAISE", 1)


def test_async_cluster_features_reports_feature_levels():
    executor = _RecordingAsyncExecutor(_FEATURES_REPLY)
    client = AsyncFlowClient(executor)

    async def run():
        features = await client.cluster_features()
        await client.cluster_features_raise(1)
        return features

    assert asyncio.run(run())["supported"] == 1
    assert executor.calls == [("CLUSTER.FEATURES",), ("CLUSTER.FEATURES.RAISE", 1)]
