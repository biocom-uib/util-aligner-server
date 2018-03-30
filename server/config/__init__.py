"""Automatically load enabled configuration."""

import importlib
import os


__all__ = ('settings')


_config = os.environ.get('CORE_CONFIG', 'base')
settings = importlib.import_module('.' + _config, package=__package__)
settings.CONFIG = _config
