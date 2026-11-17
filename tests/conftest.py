"""Load the repo root as the `zmove` package (the repo *is* the package directory)."""
import importlib.util
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

if "zmove" not in sys.modules:
    spec = importlib.util.spec_from_file_location(
        "zmove", os.path.join(ROOT, "__init__.py"), submodule_search_locations=[ROOT])
    mod = importlib.util.module_from_spec(spec)
    sys.modules["zmove"] = mod
    spec.loader.exec_module(mod)
