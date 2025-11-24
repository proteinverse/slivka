import collections
import json
import os
import re
import warnings
from collections.abc import Iterable
from pathlib import Path
from urllib.parse import urlsplit, quote_plus, urlencode, urlunsplit

import jsonschema
from attrs import define
from packaging.version import parse as parse_version
from ruamel.yaml import YAML, ScalarNode
from ruamel.yaml.constructor import SafeConstructor

from slivka.compat import resources
from slivka.utils import flatten_mapping
from .models import *


class ServiceConfigYAMLConstructor(SafeConstructor):
    pass


def include_constructor(constructor: SafeConstructor, node: ScalarNode):
    loader = constructor.loader
    root_path = os.path.realpath(
        os.path.dirname(loader.reader.name) if loader.reader.name else os.getcwd()
    )
    value = constructor.construct_scalar(node).split("::", maxsplit=1)
    filename, node_path = value if len(value) == 2 else (value[0], "/")
    file_path = os.path.join(root_path, filename)
    new_loader: YAML = type(loader)(typ=loader.typ, pure=loader.pure)
    with open(file_path, "r") as f:
        obj = new_loader.load(f)
    for key in filter(None, node_path.split("/")):
        obj = obj[key]
    return obj


ServiceConfigYAMLConstructor.add_constructor("!include", include_constructor)


class ServiceConfigYAMLLoader(YAML):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.Constructor = ServiceConfigYAMLConstructor


@define
class ServiceConfigSyntaxError(ValueError):
    path: list[str]
    message: str

    @property
    def path_string(self):
        return "/" + "/".join(self.path)


def construct_service_config_from_yaml(path: os.PathLike | str) -> ServiceConfig:
    basename = os.path.basename(path)
    name, pri_ext = os.path.splitext(basename)
    name, sec_ext = os.path.splitext(name)
    if not name:
        raise ValueError(
            f"Unable to get the name of {path}."
            f"The file name must end with '.service.yaml'"
        )
    if m := re.search(r"[^a-zA-Z0-9_\-.]", name):
        raise ValueError(
            f"Invalid service file name: {name}. "
            f"It contains an illegal character '{m.group()}'"
        )
    with open(path, 'rb') as f:
        config = ServiceConfigYAMLLoader().load(f)
    return construct_service_config_from_dict(name, config)


def construct_service_config_from_dict(service_id: str, config_dict: dict):
    with resources.open_text(__package__, "service-schema.json") as f:
        schema = json.load(f)
    try:
        jsonschema.validate(config_dict, schema)
    except jsonschema.ValidationError as e:
        raise ServiceConfigSyntaxError(list(map(str, e.path)), e.message)

    input_parameters_config = {
        key: _parse_input_parameter_config_dict(val)
        for key, val in config_dict["parameters"].items()
    }
    command_args_config = [
        CommandLineArgumentConfig(
            id=key,
            arg=val["arg"],
            symlink=val.get("symlink", None),
            default=val.get("default", None),
            join=val.get("join", None),
        )
        for key, val in config_dict["args"].items()
    ]
    outputs_config = [
        OutputFileConfig(
            id=key,
            path=val["path"],
            name=val.get("name", ""),
            media_type=val.get("media-type", ""),
        )
        for key, val in config_dict["outputs"].items()
    ]
    runners_config = {
        key: RunnerConfig(
            id=key,
            type=val["type"],
            parameters=val.get("parameters", {}),
            consts=val.get("consts", {}),
            env=val.get("env", {}),
            selector_options=val.get("selector-options"),
        )
        for key, val in config_dict["execution"]["runners"].items()
    }
    tests_config = [
        ServiceTestConfig(
            applicable_runners=val["applicable-runners"],
            parameters=val["parameters"],
            timeout=val.get("timeout", None),
            interval=val.get("interval", None),
        )
        for val in config_dict.get("tests", [])
    ]
    return ServiceConfig(
        id=service_id,
        slivka_version=config_dict["slivka-version"],
        name=config_dict["name"],
        description=config_dict.get("description", ""),
        author=config_dict.get("author", ""),
        version=config_dict.get("version", ""),
        license=config_dict.get("license", ""),
        classifiers=config_dict.get("classifiers", []),
        aliases=config_dict.get("aliases", []),
        parameters=input_parameters_config,
        command=config_dict["command"],
        args=command_args_config,
        env=config_dict.get("env", {}),
        outputs=outputs_config,
        execution=ExecutionConfig(
            runners=runners_config,
            selector=config_dict["execution"].get("selector")
        ),
        tests=tests_config
    )


def _parse_input_parameter_config_dict(data_dict: dict) -> InputParameterConfig:
    data_dict = data_dict.copy()
    return InputParameterConfig(
        type=data_dict.pop("type"),
        name=data_dict.pop("name"),
        description=data_dict.pop("description", ""),
        default=data_dict.pop("default", None),
        required=data_dict.pop("required", True),
        condition=data_dict.pop("condition", None),
        constraints=data_dict,
    )



class ProjectConfigurationError(Exception):
    pass


class ProjectConfigBuilder:
    compatible_config_ver = [
        "0.3",
        "0.8",
        "0.8.0",
        "0.8.1",
        "0.8.2",
        "0.8.3",
        "0.8.4",
        "0.8.5",
    ]

    def __init__(self):
        self._chain_map = collections.ChainMap()
        self._schema = json.load(
            resources.open_text("slivka.conf", "partial-settings-schema.json")
        )

    def read_dict(self, config):
        self._prepend_config(config)

    def read_yaml(self, path):
        with open(path) as f:
            config = YAML(typ="safe").load(f)
        config["settings-file"] = os.fspath(path)
        version = parse_version(config["version"])
        if version.base_version not in self.compatible_config_ver:
            raise ProjectConfigurationError(
                f"File {path} is not compatible with this slivka version."
            )
        self._prepend_config(config)

    def read_env(self, env):
        config = {
            config_prop: env[var_name]
            for var_name, config_prop in [
                ("SLIVKA_HOME", "directory.home"),
                ("SLIVKA_DIR_UPLOADS", "directory.uploads"),
                ("SLIVKA_DIR_JOBS", "directory.jobs"),
                ("SLIVKA_DIR_SERVICES", "directory.services"),
                ("SLIVKA_SERVER_PREFIX", "server.prefix"),
                ("SLIVKA_SERVER_HOST", "server.host"),
                ("SLIVKA_LOCAL_QUEUE_HOST", "local-queue.host"),
                ("SLIVKA_MONGODB_HOST", "mongodb.host"),
                ("SLIVKA_MONGODB_SOCKET", "mongodb.socket"),
                ("SLIVKA_MONGODB_USERNAME", "mongodb.username"),
                ("SLIVKA_MONGODB_PASSWORD", "mongodb.password"),
                ("SLIVKA_MONGODB_QUERY", "mongodb.query"),
                ("SLIVKA_MONGODB_DATABASE", "mongodb.database"),
                ("SLIVKA_MONGODB_URI", "mongodb.uri"),
            ]
            if var_name in env
        }
        self._prepend_config(config)

    def _prepend_config(self, config: dict):
        config = self._validate_config(config)
        self._chain_map.maps.insert(0, config)

    def _validate_config(self, config: dict):
        config = flatten_mapping(config)
        try:
            jsonschema.validate(instance=config, schema=self._schema)
        except jsonschema.ValidationError as e:
            raise ProjectConfigurationError(
                f"Settings file contains an error at '{'.'.join(e.path)}': {e.message}"
            )
        if "mongodb.host" in config or "mongodb.socket" in config or "mongodb.uri" in config:
            mongo_uri, mongo_database = self._parse_mongodb_config_dict(config)
            config["mongodb.uri"] = mongo_uri
            config["mongodb.database"] = mongo_database
        return config

    @staticmethod
    def _parse_mongodb_config_dict(dictionary):
        if "mongodb.uri" in dictionary:
            connection_uri = dictionary["mongodb.uri"]
            if "mongodb.database" in dictionary:
                database = dictionary["mongodb.database"]
            else:
                split_result = urlsplit(dictionary["mongodb.uri"])
                database = split_result.path.lstrip("/")
        else:
            options = {
                key.rsplit('.', 1)[-1]: val for key, val in dictionary.items()
                if key.startswith("mongodb.options.")
            }
            connection_uri = ProjectConfigBuilder._build_mongodb_uri(
                hostname=dictionary.get("mongodb.host"),
                socket=dictionary.get("mongodb.socket"),
                username=dictionary.get("mongodb.username"),
                password=dictionary.get("mongodb.password"),
                query=dictionary.get("mongodb.query"),
                options=options
            )
            database = dictionary["mongodb.database"]
        return connection_uri, database

    @staticmethod
    def _build_mongodb_uri(
            scheme="mongodb",
            hostname=None,
            socket=None,
            username=None,
            password=None,
            query=None,
            options=None,
    ):
        # >>> For backwards compatibility. Will remove in the future
        if hostname and "?" in hostname:
            hostname, host_query = hostname.split("?", 1)
            # merge query parameters in the hostname with explicit query parameters
            query = (f"{host_query}&{query}"
                     if (query and host_query)
                     else (query or host_query))
            warnings.warn(
                "Using query parameters in the host name will be removed in the future. "
                "Use \"mongodb.query\" or \"mongodb.options\" to set query parameters instead.",
                FutureWarning
            )
        if socket and "?" in socket:
            socket, socket_query = socket.split("?", 1)
            # merge query parameters in the socket with explicit query parameters
            query = (f"{socket_query}&{query}"
                     if (query and socket_query)
                     else (query or socket_query))
            warnings.warn(
                "Using query parameters in the socket name will be removed in the future. "
                "Use \"mongodb.query\" or \"mongodb.options\" to set query parameters instead.",
                FutureWarning
            )
        # <<<
        authority = ""
        if username is not None and password is not None:
            authority = f"{quote_plus(username)}:{quote_plus(password)}@"
        elif username is not None:
            authority = f"{quote_plus(username)}@"
        if socket is not None:
            authority += quote_plus(socket)
        elif hostname is not None:
            authority += hostname
        else:
            raise ValueError("Either a 'host' or a 'socket' must be set.")
        if options:
            if query:
                query = f"{query}&{urlencode(options)}"
            else:
                query = urlencode(options)
        return urlunsplit((scheme, authority, "", query, ""))

    def build(self) -> SlivkaProjectConfig:
        config = self._chain_map
        directory_config = DirectoriesConfig(
            home=config["directory.home"],
            uploads=config["directory.uploads"],
            jobs=config["directory.jobs"],
            logs=config["directory.logs"],
            services=config["directory.services"]
        )
        server_config = ServerConfig(
            prefix=config.get("server.prefix"),
            host=config.get("server.host", "127.0.0.1:4040"),
            uploads_path=config.get("uploads-path", "/media/uploads"),
            jobs_path=config.get("jobs_path", "/media/jobs")
        )
        local_queue_config = LocalQueueConfig(
            host=config.get("local-queue.host", "127.0.0.1:4041")
        )
        mongodb_config = MongoDBConfig(
            uri=config["mongodb.uri"],
            database=config["mongodb.database"]
        )
        services_config = [
            construct_service_config_from_yaml(filename)
            for search_path in directory_config.services
            for filename in self.find_service_files(search_path)
        ]
        return SlivkaProjectConfig(
            settings_file=config.get("config-file"),
            version=config.get("version"),
            directory=directory_config,
            server=server_config,
            local_queue=local_queue_config,
            mongodb=mongodb_config,
            services=services_config,
        )

    @staticmethod
    def find_service_files(search_path) -> Iterable[str]:
        """
        Recursively finds paths of service definition files
        within the search_path directory.

        :param search_path: A directory path.
        :return: Iterable of service file paths.
        """
        return (
            os.path.join(base, fn)
            for base, _dirs, files in os.walk(search_path)
            for fn in files
            if fn.endswith(".service.yaml") or fn.endswith(".service.yml")
        )
