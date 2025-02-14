import sys
import types

import pymongo.database

import slivka.conf
from slivka.utils import cached_property


class _DBModule(types.ModuleType):
    def __init__(self):
        super().__init__(__name__)
        self.__path__ = __path__
        self.__file__ = __file__

    @cached_property
    def mongo(self):
        return pymongo.MongoClient(
            slivka.conf.settings.mongodb.uri,
            serverSelectionTimeoutMS=2000
        )

    @cached_property
    def database(self):
        return self.mongo[slivka.conf.settings.mongodb.database]


mongo = ...  # type: pymongo.MongoClient
database = ...  # type: pymongo.database.Database

sys.modules[__name__] = _DBModule()
