"""Conformance: dbt.tests.adapter.simple_seed.* and simple_copy.* — seeds and copies."""
from dbt.tests.adapter.simple_copy import test_copy_uppercase
from dbt.tests.adapter.simple_seed import test_seed, test_seed_type_override

from _subclass import export

export(globals(), test_seed, test_seed_type_override, test_copy_uppercase)
