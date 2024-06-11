import dataclasses
import types

import numpy as np
from sqlalchemy.orm import DeclarativeBase, MappedAsDataclass


class Base(MappedAsDataclass, DeclarativeBase):
    """Base class for tables."""

    __table_args__ = {"schema": "PEGASUS2"}

    def __post_init__(self) -> None:
        """Perform type casting for all fields not matching their specified type."""
        for field in dataclasses.fields(self):
            value = getattr(self, field.name)
            if not isinstance(value, field.type):
                try:
                    if isinstance(field.type, type(np.ndarray)):
                        setattr(self, field.name, np.array(value))
                    elif isinstance(field.type, types.UnionType):
                        setattr(self, field.name, field.type[0](value))
                    else:
                        print(field.type)
                        setattr(self, field.name, field.type(value))
                except TypeError as e:
                    pass
