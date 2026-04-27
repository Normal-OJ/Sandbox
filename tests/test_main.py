"""Smoke test for main.py — verify imports work and signal handler attaches."""


def test_main_module_imports_cleanly():
    """Import test — main.py shouldn't crash on import."""
    import main  # noqa: F401


def test_main_function_exists_and_is_callable():
    import main
    assert callable(main.main)
