import os
import shlex
from collections.abc import Mapping
from typing import Any

import attrs
from attrs import define, field
from frozendict import frozendict
from packaging.version import Version
from packaging.version import parse as parse_version

from slivka.utils.env import expandvars


__all__ = (
    "InputParameterConfig",
    "CommandLineArgumentConfig",
    "OutputFileConfig",
    "RunnerConfig",
    "ExecutionConfig",
    "ServiceTestConfig",
    "ServiceConfig",
    "DirectoriesConfig",
    "ServerConfig",
    "LocalQueueConfig",
    "MongoDBConfig",
    "SlivkaProjectConfig",
)


# Service configuration models

@define
class InputParameterConfig(Mapping):
    type: str = field()
    name: str = field()
    description: str = field(default="")
    default: Any = field(default=None)
    required: bool = field(default=True, converter=attrs.converters.to_bool)
    condition: str | None = field(default=None)
    _constraints: dict = field(factory=dict)

    def __getitem__(self, item):
        try:
            return getattr(self, item)
        except AttributeError:
            return self._constraints[item]

    def __iter__(self):
        yield from (
            f.name for f in attrs.fields(type(self))
            if not f.name.startswith("_")
        )
        yield from self._constraints

    def __len__(self):
        return sum(1 for _ in self)


@define
class CommandLineArgumentConfig:
    id: str = field()
    arg: str = field()
    symlink: str | None = field(default=None)
    default: str | None = field(default=None)
    join: str | None = field(default=None)


@define
class OutputFileConfig:
    id: str = field()
    path: str = field()
    name: str = field(default="")
    media_type: str = field(default="")


@define
class RunnerConfig:
    id: str = field()
    type: str = field()
    parameters: dict[str, Any] = field(factory=dict)
    consts: dict[str, Any] = field(factory=dict)
    env: dict[str, str] = field(factory=dict)
    selector_options: dict[str, Any] = field(factory=dict)


@define
class ExecutionConfig:
    runners: dict[str, RunnerConfig]
    selector: str | None = None


def _test_parameters_converter(parameters: dict[str, str | list[str]]) -> dict[str, str | list[str]]:
    converted = {}
    for key, val in parameters.items():
        if isinstance(val, str):
            converted[key] = expandvars(val)
        elif isinstance(val, list):
            converted[key] = [expandvars(v) for v in val]
        else:
            raise TypeError(type(val))
    return converted


@define
class ServiceTestConfig:
    applicable_runners: list[str] = field()
    parameters: dict[str, str | list[str]] = field(converter=_test_parameters_converter)
    timeout: int | None = field(default=None)
    interval: int | None = field(default=None)


def _normalize_command_args(value: str | list[str]) -> list[str]:
    if isinstance(value, str):
        return shlex.split(value)
    elif isinstance(value, list):
        return value
    else:
        raise TypeError(type(value))


@define(kw_only=True)
class ServiceConfig:
    id: str = field()
    slivka_version: Version = field(converter=parse_version)
    name: str = field()
    description: str = field(default="")
    author: str = field(default="")
    version: str = field(default="")
    license: str = field(default="")
    classifiers: list[str] = field(factory=list)
    aliases: list[str] = field(factory=list)
    parameters: Mapping[str, InputParameterConfig] = field(converter=frozendict)
    command: list[str] = field(converter=_normalize_command_args)
    args: list[CommandLineArgumentConfig] = field()
    env: Mapping[str, str] = field(converter=frozendict, factory=frozendict)
    outputs: list[OutputFileConfig] = field()
    execution: ExecutionConfig = field()
    tests: list[ServiceTestConfig] = field(factory=list)


# Slivka project configurations models

def _split_path(paths: str | list[str]):
    return paths.split(os.pathsep) if isinstance(paths, str) else paths


@define
class DirectoriesConfig:
    home: str = field(converter=os.path.realpath)
    uploads: str = "uploads"
    jobs: str = "jobs"
    logs: str = "logs"
    services: list[str] = field(
        factory=lambda: ["services"],
        converter=_split_path
    )

    def __attrs_post_init__(self):
        def _normalize(path):
            if not os.path.isabs(path):
                path = os.path.join(self.home, path)
            return os.path.realpath(path)

        self.uploads = _normalize(self.uploads)
        self.jobs = _normalize(self.jobs)
        self.logs = _normalize(self.logs)
        self.services = [_normalize(p) for p in self.services]


@define
class ServerConfig:
    prefix: str | None = None
    host: str = "127.0.0.1:4040"
    uploads_path: str = "/media//uploads"
    jobs_path: str = "/media/jobs"


@define
class LocalQueueConfig:
    host: str = "127.0.0.1:4041"


@define
class MongoDBConfig:
    uri: str = "127.0.0.1:27017"
    database: str = "slivka"


@define(kw_only=True)
class SlivkaProjectConfig:
    settings_file: str | None = None
    version: str | None = None
    directory: DirectoriesConfig
    server: ServerConfig
    local_queue: LocalQueueConfig
    mongodb: MongoDBConfig
    services: list[ServiceConfig]
