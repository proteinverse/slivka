import os
import sys
from types import ModuleType

from slivka.utils import cached_property
from . import loaders
from .loaders import ServiceConfig, SlivkaSettings, SettingsLoader_0_8_5b5


def _load():
    home = os.getenv("SLIVKA_HOME", os.getcwd())
    loader = SettingsLoader_0_8_5b5()
    loader.read_dict({"directory.home": home})
    files = ['settings.yaml', 'settings.yml', 'config.yaml', 'config.yml']
    files = (os.path.join(home, fn) for fn in files)
    try:
        file = next(filter(os.path.isfile, files))
        loader.read_yaml(file)
    except StopIteration:
        raise loaders.ImproperlyConfigured(
            'Settings file not found in %s. Check if SLIVKA_HOME environment '
            'variable is set correctly and the directory contains '
            'settings.yaml or config.yaml.' % home
        ) from None
    loader.read_env(os.environ)
    return loader.build()


def bootstrap(conf: SlivkaSettings):
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
        loader = SettingsLoader_0_8_5b5()
        loader.read_yaml(fp)
        conf = loader.build()
        bootstrap(conf)
        self.settings = conf

    def load_dict(self, config):
        loader = SettingsLoader_0_8_5b5()
        loader.read_dict(config)
        conf = loader.build()
        bootstrap(conf)
        self.settings = conf


settings: loaders.SlivkaSettings

sys.modules[__name__].__class__ = _ConfModule
