import os
from unittest import mock
from urllib.parse import quote_plus

import pytest
import yaml

import slivka.conf.loaders
from slivka.compat.resources import open_text
from slivka.conf import SlivkaSettings
from slivka.conf.loaders import SettingsLoader_0_8_5b5


@pytest.fixture
def minimal_settings():
    with open_text(__package__, 'data/settings.yaml') as stream:
        return yaml.safe_load(stream)


def test_conf_directory_real_path(tmp_path, minimal_settings):
    real_home = tmp_path / "real-slivka"
    os.mkdir(real_home)
    home = tmp_path / "slivka"
    os.symlink(real_home, home, target_is_directory=True)
    os.mkdir(home / "services")
    with mock.patch.dict(os.environ, SLIVKA_HOME=str(home)):
        conf = slivka.conf.loaders.load_settings_0_3(minimal_settings)
    assert conf.directory.home == str(real_home)
    assert conf.directory.jobs == str(real_home / 'jobs')
    assert conf.directory.uploads == str(real_home / 'uploads')
    assert conf.directory.logs == str(real_home / 'log')
    assert conf.directory.services == str(real_home/ 'services')

@pytest.mark.parametrize(
    ("settings_dict", "expected_uri"),
    [
        pytest.param(
            {
                "mongodb.host": "dev.example.com:27017/",
                "mongodb.database": "testdb"
            },
            "mongodb://dev.example.com:27017/",
            id="basic host"
        ),
        pytest.param(
            {
                "mongodb.host": "dev01.example.com,dev02.example.com/",
                "mongodb.database": "testdb"
            },
            "mongodb://dev01.example.com,dev02.example.com/",
            id="hosts set"
        ),
        pytest.param(
            {
                "mongodb.host": "dev.example.com/",
                "mongodb.username": "slivkauser",
                "mongodb.password": "p455w0rd",
                "mongodb.database": "testdb"
            },
            "mongodb://slivkauser:p455w0rd@dev.example.com/",
            id="credentials"
        ),
        pytest.param(
            {
                "mongodb.host": "example.com",
                "mongodb.username": "slivka user",
                "mongodb.database": "testdb"
            },
            "mongodb://slivka+user@example.com",
            id="quote-plus space"
        ),
        pytest.param(
            {
                "mongodb.host": "example.com",
                "mongodb.username": "slivka+user",
                "mongodb.database": "testdb"
            },
            "mongodb://slivka%2Buser@example.com",
            id="%-encode username"
        ),
        pytest.param(
            {
                "mongodb.host": "example.com",
                "mongodb.username": "slivka",
                "mongodb.password": "=&D$_*",
                "mongodb.database": "testdb"
            },
            "mongodb://slivka:%3D%26D%24_%2A@example.com",
            id="%-encode password"
        ),
        pytest.param(
            {
                "mongodb.host": "dev.example.com/admin",
                "mongodb.database": "testdb"
            },
            "mongodb://dev.example.com/admin",
            id="default authdb"
        ),
        pytest.param(
            {
                "mongodb.host": "dev.example.com/admin",
                "mongodb.query": "authSource=notadmin",
                "mongodb.database": "testdb"
            },
            "mongodb://dev.example.com/admin?authSource=notadmin",
            id="default authdb and authSource"
        ),
        pytest.param(
            {
                "mongodb.host": "dev01.host.com:27017,dev02.host.com:27017,dev03.host.com:27017/",
                "mongodb.username": "slivkauser",
                "mongodb.password": 'p455w0rd',
                "mongodb.database": "testdb",
                "mongodb.query": "replicaSet=rsDev&authSource=admin"
            },
            "mongodb://slivkauser:p455w0rd@dev01.host.com:27017,dev02.host.com:27017,dev03.host.com:27017/?replicaSet=rsDev&authSource=admin",
            id="credentials and replica set"
        ),
        pytest.param(
            {
                "mongodb.host": "dev.example.com/",
                "mongodb.query": "tlsCAFile=%2Fhome%2Fweb%2Ftls%2Fcert.crt",
                "mongodb.database": "testdb"
            },
            "mongodb://dev.example.com/?tlsCAFile=%2Fhome%2Fweb%2Ftls%2Fcert.crt",
            id="urlencoded query"
        ),
        pytest.param(
            {
                "mongodb.host": "dev.example.com:27017/",
                "mongodb.options": {
                    "replicaSet": "rs_dev",
                    "authSource": "admin",
                    "tlsCAFile": "/home/web/tls/cert.crt"
                },
                "mongodb.database": "testdb"
            },
            "mongodb://dev.example.com:27017/?replicaSet=rs_dev&authSource=admin&tlsCAFile=%2Fhome%2Fweb%2Ftls%2Fcert.crt",
            id="options"
        ),
        pytest.param(
            {
                "mongodb.host": "dev.example.com/",
                "mongodb.query": "compressors=zlib&zlibCompressionLevel=5",
                "mongodb.options": {"authSource": "admin", "replicaSet": "rs_dev"},
                "mongodb.database": "testdb"
            },
            "mongodb://dev.example.com/?compressors=zlib&zlibCompressionLevel=5&authSource=admin&replicaSet=rs_dev",
            id="query and options"
        )
    ]
)
def test_settings_loader_mongodb_uri(
        tmp_path,
        minimal_settings,
        settings_dict,
        expected_uri
):
    home = tmp_path
    os.mkdir(home / "services")
    loader = SettingsLoader_0_8_5b5()
    loader.read_dict(minimal_settings)
    loader.read_dict(settings_dict)
    loader.read_dict({"directory.home": str(home)})
    settings = loader.build()
    assert settings.mongodb.uri == expected_uri


@pytest.mark.parametrize(
    ("environ", "expected_settings"),
    [
        (
            {
                "SLIVKA_SERVER_PREFIX": "/slivka",
                "SLIVKA_SERVER_HOST": "0.0.0.0:5000"
            },
             SlivkaSettings.Server(
                prefix="/slivka",
                host="0.0.0.0:5000",
                uploads_path="/media/uploads",
                jobs_path="/media/jobs"
            )
        ),
        (
            {
                "SLIVKA_SERVER_PREFIX": "/my_slivka",
            },
            SlivkaSettings.Server(
                prefix="/my_slivka",
                host="127.0.0.1:4040",
                uploads_path="/media/uploads",
                jobs_path="/media/jobs"
            )
        )
    ]
)
def test_server_settings_loader_reads_from_env(
        tmp_path,
        minimal_settings,
        environ,
        expected_settings
):
    home = tmp_path
    os.mkdir(home / "services")
    loader = SettingsLoader_0_8_5b5()
    loader.read_dict(minimal_settings)
    loader.read_env(environ)
    loader.read_dict({"directory.home": str(home)})
    settings = loader.build()
    assert settings.server == expected_settings


@pytest.mark.parametrize(
    ("environ", "expected_settings"),
    [
        (
            {
                "SLIVKA_MONGODB_HOST": "example.com:27017",
                "SLIVKA_MONGODB_DATABASE": "slivka_tst"
            },
            SlivkaSettings.MongoDB(
                uri="mongodb://example.com:27017/slivka_tst",
                database="slivka_tst"
            )
        ),
        (
            {
                "SLIVKA_MONGODB_HOST": "example.com:27017",
                "SLIVKA_MONGODB_USERNAME": "slivka_user",
                "SLIVKA_MONGODB_DATABASE": "slivka_tst"
            },
            SlivkaSettings.MongoDB(
                uri="mongodb://slivka_user@example.com:27017/slivka_tst",
                database="slivka_tst"
            )
        ),
        (
            {
                "SLIVKA_MONGODB_HOST": "example.com:27017",
                "SLIVKA_MONGODB_USERNAME": "slivka_user",
                "SLIVKA_MONGODB_PASSWORD": "P4ssW0Rd",
                "SLIVKA_MONGODB_DATABASE": "slivka_tst"
            },
            SlivkaSettings.MongoDB(
                uri="mongodb://slivka_user:P4ssW0Rd@example.com:27017/slivka_tst",
                database="slivka_tst"
            )
        ),
        (
            {
                "SLIVKA_MONGODB_HOST": "example.com:27017",
                "SLIVKA_MONGODB_USERNAME": "slivka user",
                "SLIVKA_MONGODB_PASSWORD": "p#$_///Or)+",
                "SLIVKA_MONGODB_DATABASE": "slivka_tst"
            },
            SlivkaSettings.MongoDB(
                uri="mongodb://slivka+user:p%23%24_%2F%2F%2FOr%29%2B@example.com:27017/slivka_tst",
                database="slivka_tst"
            )
        ),
        (
            {
                "SLIVKA_MONGODB_HOST": "example.host0.com:27017,example.host1.com:27017,example.host2.com:27017",
                "SLIVKA_MONGODB_QUERY": "replicaSet=xyz",
                "SLIVKA_MONGODB_DATABASE": "slivka_tst"
            },
            SlivkaSettings.MongoDB(
                uri="mongodb://example.host0.com:27017,example.host1.com:27017,example.host2.com:27017/slivka_tst?replicaSet=xyz",
                database="slivka_tst"
            )
        ),
        (
            {
                "SLIVKA_MONGODB_HOST": "example.host.com:2137",
                "SLIVKA_MONGODB_QUERY": "authSource=admin&zlibCompressionLevel=6",
                "SLIVKA_MONGODB_DATABASE": "slivka_tst"
            },
            SlivkaSettings.MongoDB(
                uri="mongodb://example.host.com:2137/slivka_tst?authSource=admin&zlibCompressionLevel=6",
                database="slivka_tst"
            )
        ),
        (
            {
                "SLIVKA_MONGODB_SOCKET": "/var/run/mongodb-socket",
                "SLIVKA_MONGODB_DATABASE": "slivka_tst"
            },
            SlivkaSettings.MongoDB(
                uri="mongodb://%2Fvar%2Frun%2Fmongodb-socket/slivka_tst",
                database="slivka_tst"
            )
        ),
        (
            {
                "SLIVKA_MONGODB_SOCKET": "/var/run/mongodb-socket",
                "SLIVKA_MONGODB_USERNAME": "slivka_user",
                "SLIVKA_MONGODB_PASSWORD": "P4S$worD",
                "SLIVKA_MONGODB_DATABASE": "slivka_tst"
            },
            SlivkaSettings.MongoDB(
                uri="mongodb://slivka_user:P4S%24worD@%2Fvar%2Frun%2Fmongodb-socket/slivka_tst",
                database="slivka_tst"
            )
        )
    ]
)
def test_mongodb_settings_loader_reads_from_env(
        tmp_path,
        minimal_settings,
        environ,
        expected_settings
):
    home = tmp_path
    os.mkdir(home / "services")
    loader = SettingsLoader_0_8_5b5()
    loader.read_dict(minimal_settings)
    loader.read_env(environ)
    loader.read_dict({"directory.home": str(home)})
    settings = loader.build()
    assert settings.mongodb == expected_settings
