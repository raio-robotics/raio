"""Serializers: what converts the values of a connection to and from bytes."""

from __future__ import annotations

from typing import Protocol, TypeVar

import msgpack

T = TypeVar("T")


class Serializer(Protocol[T]):
    """Converts the values of one connection to and from bytes.

    Both ends of a connection know a serializer by its `name`, if it has one, and by the name
    of its class otherwise.
    """

    def serialize(self, value: T) -> bytes: ...

    def deserialize(self, data: bytes) -> T: ...


class MessagePack:
    """Serializes the values that Python nodes and Rust nodes both have, as MessagePack."""

    name = "msgpack"  # the name under which a Rust node has its MessagePack serializer

    def serialize(self, value) -> bytes:
        return msgpack.packb(value)

    def deserialize(self, data: bytes):
        return msgpack.unpackb(data)
