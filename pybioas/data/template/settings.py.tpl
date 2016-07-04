import os


# sets project root directory to the directory of the current file.
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# secret cryptographic key used for signing identifications
SECRET_KEY = {{ secret_key }}

# folder where uploaded files are stored
UPLOAD_DIR = "./uploads"

# folder where working directory for services will be placed
WORK_DIR = "./work_dir"

# location of service initialization file
SERVICE_INI = "./services.ini"

# list of available services ["Service1", "Service2", ...]
SERVICES = [{% if example %}"PyDummy"{% endif %}]