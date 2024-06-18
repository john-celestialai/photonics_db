import dataclasses
import types

import numpy as np
from psycopg2.extensions import AsIs, register_adapter
from sqlalchemy.orm import DeclarativeBase, MappedAsDataclass


# We have to tell psycopg2 how to handle numpy datatypes
def addapt_numpy_float64(numpy_float64):
    return AsIs(numpy_float64)


def addapt_numpy_int64(numpy_int64):
    return AsIs(numpy_int64)


def addapt_numpy_float32(numpy_float32):
    return AsIs(numpy_float32)


def addapt_numpy_int32(numpy_int32):
    return AsIs(numpy_int32)


def addapt_numpy_array(numpy_array):
    return AsIs(numpy_array.tolist())


register_adapter(np.float64, addapt_numpy_float64)
register_adapter(np.int64, addapt_numpy_int64)
register_adapter(np.float32, addapt_numpy_float32)
register_adapter(np.int32, addapt_numpy_int32)
register_adapter(np.ndarray, addapt_numpy_array)


class Base(MappedAsDataclass, DeclarativeBase):
    """Base class for tables."""

    __table_args__ = {"schema": "PEGASUS2"}

    def __post_init__(self) -> None:
        """Perform type casting for all fields not matching their specified type."""
        for field in dataclasses.fields(self):
            value = getattr(self, field.name)
            try:
                if not isinstance(value, field.type):
                    try:
                        if isinstance(field.type, type(np.ndarray)):
                            setattr(self, field.name, np.array(value))
                        elif isinstance(field.type, types.UnionType):
                            setattr(self, field.name, field.type[0](value))
                        else:
                            setattr(self, field.name, field.type(value))
                    except TypeError as e:
                        pass
            except TypeError as e:
                # TODO: Fix this type casting for subscripted generics
                # e.g., List[ForwardRef('WDMSweepRaw')]
                pass
