from io import BytesIO

from portus.core.cache import Cache


class InMemCache(Cache):
    def __init__(self):
        self._cache = {}
        self._prefix = ""

    def put(self, k: str, v: BytesIO) -> None:
        self._cache[self._prefix + k] = v

    def get(self, k: str) -> BytesIO:
        return self._cache[self._prefix + k]

    def scoped(self, scope: str) -> Cache:
        self._prefix = scope
        return self
