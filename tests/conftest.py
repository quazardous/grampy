import pytest

# The contract lives in the package, not in a test module: ask pytest to
# rewrite its asserts so a failing driver shows the values it returned.
pytest.register_assert_rewrite("quazardous.grampy.testing")
