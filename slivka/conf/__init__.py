import os
import sys
from types import ModuleType

from slivka.utils import cached_property
from .models import SlivkaProjectConfig
from .builders import ProjectConfigBuilder, ProjectConfigurationError


def _load():
    home = os.getenv("SLIVKA_HOME", os.getcwd())
    home = os.path.realpath(home)
    builder = ProjectConfigBuilder()
    builder.read_dict({"directory.home": home})
    files = ['settings.yaml', 'settings.yml', 'config.yaml', 'config.yml']
    files = (os.path.join(home, fn) for fn in files)
    try:
        file = next(filter(os.path.isfile, files))
        builder.read_yaml(file)
    except StopIteration:
        raise ProjectConfigurationError(
            'Settings file not found in %s. Check if SLIVKA_HOME environment '
            'variable is set correctly and the directory contains '
            'settings.yaml or config.yaml.' % home
        ) from None
    builder.read_env(os.environ)
    return builder.build()


def bootstrap(conf: SlivkaProjectConfig):
    os.makedirs(conf.directory.jobs, exist_ok=True)
    os.makedirs(conf.directory.logs, exist_ok=True)
    os.makedirs(conf.directory.uploads, exist_ok=True)


class _ConfModule(ModuleType):
    @cached_property
    def settings(self):
        conf = _load()
        bootstrap(conf)
        return conf

    def load_file(self, fp):
        builder = ProjectConfigBuilder()
        builder.read_yaml(fp)
        conf = builder.build()
        bootstrap(conf)
        self.settings = conf

    def load_dict(self, config):
        builder = ProjectConfigBuilder()
        builder.read_dict(config)
        conf = builder.build()
        bootstrap(conf)
        self.settings = conf


settings: SlivkaProjectConfig

sys.modules[__name__].__class__ = _ConfModule
