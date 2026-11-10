# -*- coding: utf-8 -*-
import pickle
from time import time
from typing import Optional, Union

import dill
import redis
from redis.exceptions import RedisClusterException, RedisError

# RedisClusterException não herda de RedisError no redis-py
REDIS_ERRORS = (RedisError, RedisClusterException)


class SerializationError(Exception):
    """
    Exception raised when
    serialization fails.
    """


class DeserializationError(Exception):
    """
    Exception raised when
    deserialization fails.
    """


class FatalError(Exception):
    """
    General purpose exception.
    """


class _RedisPalMixin:
    """
    RedisPal helps you store data on Redis
    without worrying about serialization
    stuff. Besides that, it also stores
    timestamps for last changes so you can
    check that whenever you want.

    You can use this as a key-value data
    structure using "get" and "set"
    methods.

    The Redis client implementation is provided by a subclass, allowing
    standalone and cluster clients to share the serialization behavior.
    """

    def __repr__(self) -> str:
        """
        Representation of this object
        """

        client_state = getattr(self, "connection_pool", None)
        if client_state is None:
            client_state = getattr(self, "nodes_manager", None)
        return "<{} <pool={}>>".format(type(self).__name__, client_state)

    def __str__(self) -> str:
        """
        Representation of this object
        """

        return self.__repr__()

    @classmethod
    def _serialize(cls, o: object) -> bytes:
        try:
            return pickle.dumps(o)
        except Exception:
            try:
                return dill.dumps(o)
            except Exception as err:
                raise SerializationError(
                    "Failed to serialize object {} of type {}".format(o, type(o))
                ) from err

    @classmethod
    def _deserialize(cls, e: Union[str, int, float, bytes]) -> object:
        if e is None:
            return None
        try:
            return pickle.loads(e)
        except Exception:
            try:
                return dill.loads(e)
            except Exception as err:
                raise DeserializationError("Failed to deserialize {}".format(e)) from err

    def set(self, key, value, *args, **kwargs) -> bool:
        _ser = self._serialize(value)
        timestamp_key = "{}_timestamp".format(key)

        result = super().set(*args, name=key, value=_ser, **kwargs)
        if not result and not kwargs.get("get", False):
            return False

        timestamp_kwargs = {k: v for k, v in kwargs.items() if k not in ("nx", "xx", "get")}
        _b = super().set(*args, name=timestamp_key, value=time(), **timestamp_kwargs)
        return bool(_b)

    def get(self, key, *args, **kwargs) -> object:
        return self._deserialize(super().get(*args, name=key, **kwargs))

    def get_with_timestamp(self, key, *args, **kwargs) -> dict:
        last_modified = super().get(*args, name="{}_timestamp".format(key), **kwargs)
        return {
            "value": self._deserialize(super().get(*args, name=key, **kwargs)),
            "last_modified": float(last_modified) if last_modified else 0,
        }


class RedisPal(_RedisPalMixin, redis.Redis):
    """RedisPal client for standalone Redis deployments."""


class RedisClusterPal(_RedisPalMixin, redis.RedisCluster):
    """RedisPal client for Redis Cluster deployments.

    Constructor arguments are passed directly to ``redis.RedisCluster``,
    including ``startup_nodes`` and the other options supported by redis-py.
    """


class MigrationRedisPal:
    """Grava nos Redis antigo e novo e lê o valor mais recente durante a migração.

    O valor mais recente é escolhido pela chave ``{key}_timestamp``. Em caso de
    empate, o Redis novo vence. Uma falha em um dos Redis só gera erro quando
    os dois falham.
    """

    def __init__(self, old: RedisPal, new: Optional[RedisClusterPal]):
        self.old = old
        self.new = new

    def __repr__(self) -> str:
        return "<MigrationRedisPal <old={!r}> <new={!r}>>".format(self.old, self.new)

    def get_clients(self) -> list:
        clients = [("old", self.old)]
        if self.new is not None:
            clients.insert(0, ("new", self.new))
        return clients

    def set(self, key, value, *args, **kwargs) -> bool:
        results = []
        last_error = None
        for name, client in self.get_clients():
            try:
                results.append(client.set(key, value, *args, **kwargs))
            except REDIS_ERRORS as error:
                last_error = error
                print(f"Falha ao gravar a chave {key} no Redis {name}: {error}")
        if not results:
            raise RedisError(f"Falha ao gravar a chave {key} em todos os Redis") from last_error
        return all(results)

    def get_with_timestamp(self, key, *args, **kwargs) -> dict:
        newest = None
        last_error = None
        for name, client in self.get_clients():
            try:
                result = client.get_with_timestamp(key, *args, **kwargs)
            except REDIS_ERRORS as error:
                last_error = error
                print(f"Falha ao ler a chave {key} no Redis {name}: {error}")
                continue
            rank = (result["last_modified"], result["value"] is not None)
            if newest is None or rank > (newest["last_modified"], newest["value"] is not None):
                newest = result
        if newest is None:
            raise RedisError(f"Falha ao ler a chave {key} em todos os Redis") from last_error
        return newest

    def get(self, key, *args, **kwargs) -> object:
        return self.get_with_timestamp(key, *args, **kwargs)["value"]

    def delete(self, *names) -> int:
        deleted = 0
        for name, client in self.get_clients():
            try:
                deleted = max(deleted, client.delete(*names))
            except REDIS_ERRORS as error:
                print(f"Falha ao apagar {names} no Redis {name}: {error}")
        return deleted
