"""Automatically load enabled configuration."""

import importlib
import os
import sys


__all__ = ('settings')


def get_config():
    settings_module = os.environ.get('APP_ENV')
    try:
        module = importlib.import_module(f'config.{settings_module}')
        settings = {k: v for k, v in vars(module).items()
                    if not k.startswith('_') and k.isupper()}
        return settings
    except Exception:
        sys.stderr.write('Failed to read config file: %s' % settings_module)
        sys.stderr.flush()
        raise


_config = os.environ.get('CORE_CONFIG', 'base')
settings = importlib.import_module('.' + _config, package=__package__)
settings.CONFIG = _config

config = get_config()
