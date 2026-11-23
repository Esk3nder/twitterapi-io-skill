"""Test package. Importing it isolates HOME and puts scripts/ on sys.path
before any test module can import a script, whatever its import order."""
from tests import e2e_base  # noqa: F401
