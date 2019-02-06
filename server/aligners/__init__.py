import importlib
import json


def load_aligner_classes(path):
    with open(path, 'r') as f:
        aligner_classes = json.load(f)

    aligners = {}

    for aligner_name, aligner_data in aligner_classes.items():
        module_path = aligner_data['module']
        class_name = aligner_data['class']

        module = importlib.import_module(module_path)
        cls = getattr(module, class_name)

        aligners[aligner_name] = cls

    return aligners
