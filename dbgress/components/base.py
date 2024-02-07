import re
from typing import List, Optional

from sqlalchemy.engine import Engine

metric_abbrs = {
    "average": "avg",
    "kurtosis": "kurt",
    "stddev": "std",
}


def query_kwargs(kwargs) -> str:
    return ",".join([f"{k} => {v}" for k, v in kwargs.items()])


class DbObj:
    @property
    def name(self) -> str:
        raise NotImplementedError(
            f"`name` property not implemented for {self.__class__.__name__}"
        )

    def create(self, engine: Engine):
        raise NotImplementedError(
            f"`create` method not implemented for {self.__class__.__name__}"
        )

    def drop(self):
        raise NotImplementedError(
            f"`drop` method not implemented for {self.__class__.__name__}"
        )

    @staticmethod
    def list_all(
        schema: Optional[str] = None, like_pattern: Optional[str] = None
    ) -> List[str]:
        raise NotImplementedError("`list_all` method not implemented.")

    @staticmethod
    def _filter_name(name: str) -> str:
        name = re.sub(r"\s+", "_", name).lower()
        for metric, abbr in metric_abbrs.items():
            name = name.replace(metric, abbr)
        return name

    def __repr__(self) -> str:
        kwargs = {"name": self.name}
        if comment := getattr(self, "comment", None):
            kwargs["comment"] = comment
        return f"{self.__class__.__name__}({query_kwargs(kwargs)})"
