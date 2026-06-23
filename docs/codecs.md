# Payload Codecs

The SDK defaults to raw bytes. This is fastest and avoids accidental JSON work.

## RawCodec

```python
from ferricstore import RawCodec, WorkflowClient

client = WorkflowClient.from_url("ferric://127.0.0.1:6388", codec=RawCodec())
```

Accepts:

* `bytes`
* `bytearray`
* `str`
* `None`

Returns `bytes | None` on decode.

## JsonCodec

```python
from ferricstore import JsonCodec, WorkflowClient

client = WorkflowClient.from_url("ferric://127.0.0.1:6388", codec=JsonCodec())
orders = client.workflow(type="order")

orders.start(
    "flow-1",
    payload={"amount": 123},
)
```

Use JSON when language-neutral payloads matter.

## Protobuf/Avro/MsgPack

Implement `Codec`:

```python
class ProtobufCodec:
    def encode(self, value):
        return value.SerializeToString()

    def decode(self, value):
        if value is None:
            return None
        msg = MyMessage()
        msg.ParseFromString(value)
        return msg
```

For performance and storage size, schema codecs like Protobuf or Avro are usually
better than JSON.

## Large Payload Rule

Payload should stay raw. Indexes, lineage, and counters should not duplicate it.
Only request payload when handler needs it.
