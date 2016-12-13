import abc
from collections import defaultdict


class AbstractDescriptor:
    """
    This class was taken from Fluent Python
    """
    __counter = 0

    def __init__(self):
        cls = self.__class__
        prefix = cls.__name__
        index = cls.__counter
        self.storage_name = '_{}#{}'.format(prefix, index)
        cls.__counter += 1

    def __get__(self, instance, owner):
        if instance is None:
            return self
        else:
            return getattr(instance, self.storage_name)

    def __set__(self, instance, value):
        setattr(instance, self.storage_name, value)


class Validated(abc.ABC, AbstractDescriptor):
    def __set__(self, instance, value):
        value = self.validate(instance, value)
        super().__set__(instance, value)

    @abc.abstractmethod
    def validate(self, instance, value):
        """return validated value or raise ValueError"""


class GreaterThanZero(Validated):
    """A number greater than zero"""
    def validate(self, instance, value):
        if value <= 0:
            raise ValueError("value must be > 0, got {}".format(value))
        return value


class NonBlank(Validated):
    """A string with at least one non-space character"""
    def validate(self, instance, value):
        value = value.strip()
        if len(value) == 0:
            raise ValueError("value cannot be empty or blank")
        return value


class Unique(Validated):
    """Value is unique"""
    values = defaultdict(list)

    def __init__(self, category: str):
        self.category = category
        super().__init__()

    def validate(self, instance, value):
        if Unique.values[self.category].count(value) > 0:
            raise ValueError("value {} is not unique".format(value))
        Unique.values[self.category].append(value)
        return value
