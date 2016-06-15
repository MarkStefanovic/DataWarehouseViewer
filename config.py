"""Loads a json file into a dictionary of configuration variables.

This class will only load the file once.  Multiple calls to the configuration
settings will only result in a single file import.

One of the guiding principles of this package is to minimize file IO. The downside 
of this is that if multiple processes use the same config file, they may retrieve 
outdated results since the configuration file is only loaded once.

Examples:
>>> cfg = SimpleJsonConfig('test.config')
>>> cfg.get_or_set_variable('hello', 1)
1
>>> cfg.get_variable('hello')
1

>>> cfg.set_variable('test', [1, 2, 3])
>>> cfg.get_variable('test')
[1, 2, 3]

>>> cfg.get_variable('test2')

"""
import json
import os


class SimpleJsonConfig():
    def __init__(self, json_path='config.json'):
        """Initialize an instance of the SimpleJsonConfig class."""
        self.json_path = json_path
        self._cache = self.load()

    def variable(self, key) -> str:
        return self._cache.get(key, '')

    def set_variable(self, key, val) -> None:
        """Set the value of the configuration variable for the key specified and save it to disk.

        If the key value specified doesn't exist, then it will be created.  If the value specified
        matches the pre-existing value, then no IO will happen.
        """
        if self.variable(key) != val:
            self._cache[key] = val
            self.save()

    def field_types(self) -> dict:
        fields = self._cache.get('fields')
        if fields:
            return {val[0]: val[2] for val in fields}

    def get_variable(self, key):
        """Lookup a configuration variable's value using the variable's name."""
        return self._cache.get(key)

    def get_or_set_variable(self, key, default_value):
        """Lookup the value of a configuration variable by name.  If it doesn't exist create it."""
        try:
            return self._cache[key]
        except KeyError:
            self.set_variable(key, default_value)
        return default_value

    def load(self) -> dict:
        """Read json file into a dictionary."""
        try:
            with open(os.path.abspath(self.json_path), 'r') as fh:
                return json.load(fh)
        except FileNotFoundError:
            print('Config file not found in current directory.')
            return {}
        except Exception as e:
            print(str(e))
            return {}

    def save(self) -> None:
        """Save all configuration variables to disk."""
        with open(self.json_path, 'w') as fh:
            json.dump(self._cache, indent=4, fp=fh)


if __name__ == "__main__":
    import doctest
    doctest.testmod()

