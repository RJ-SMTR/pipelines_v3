# -*- coding: utf-8 -*-
import pickle
from time import time
from typing import Union

import dill
import redis


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


class RedisPal(redis.Redis):
    """
    RedisPal helps you store data on Redis
    without worrying about serialization
    stuff. Besides that, it also stores
    timestamps for last changes so you can
    check that whenever you want.

    You can use this as a key-value data
    structure using "get" and "set"
    methods.

    This class inherits from redis.Redis
    and the main difference is that it
    handles serialization and deserialization
    of objects so you can set anything to
    a key.
    """

    def __repr__(self) -> str:
        """
        Representation of this object
        """

        return "<RedisPal <pool={}>>".format(self.connection_pool)

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

        result = super(RedisPal, self).set(name=key, value=_ser, *args, **kwargs)
        if not result and not kwargs.get("get", False):
            return False

        timestamp_kwargs = {k: v for k, v in kwargs.items() if k not in ("nx", "xx", "get")}
        _b = super(RedisPal, self).set(name=timestamp_key, value=time(), *args, **timestamp_kwargs)
        return bool(_b)

    def get(self, key, *args, **kwargs) -> object:
        return self._deserialize(super(RedisPal, self).get(name=key, *args, **kwargs))

    def get_with_timestamp(self, key, *args, **kwargs) -> dict:
        last_modified = super(RedisPal, self).get(name="{}_timestamp".format(key), *args, **kwargs)
        return {
            "value": self._deserialize(super(RedisPal, self).get(name=key, *args, **kwargs)),
            "last_modified": float(last_modified) if last_modified else 0,
        }
