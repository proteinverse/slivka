import configparser
import os
import sys

import pybioas
from .exceptions import ValidationError
from .form_factory import FormFactory


def init_forms(config_file):
    """
    Loads services and their parameter files from configuration and
    automagically builds form classes. Forms are dynamically added to the forms
    module.
    :param config_file:
    """
    config = configparser.ConfigParser()
    with open(config_file, 'r') as f:
        config.read_file(f)
    assert set(pybioas.settings.SERVICES).issubset(set(config.sections())), \
        "One of the services is not configured"
    module = sys.modules[__name__]
    for service in pybioas.settings.SERVICES:
        form_name = get_form_name(service)
        form_file = os.path.normpath(config.get(service, "command_file"))
        form_class = \
            FormFactory.get_form_class(form_name, service, form_file)
        setattr(module, form_name, form_class)


def get_form(service):
    """
    :param service: name of the service
    :return: form associated with a given service
    """
    module = sys.modules[__name__]
    return getattr(module, get_form_name(service))


def get_form_name(service):
    """
    :param service: service for which form name is constructed
    :return: form name for the given service
    """
    return service.capitalize() + "Form"