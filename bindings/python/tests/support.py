"""Test helpers (not collected by pytest)."""

from __future__ import annotations

import importlib
import types

import pytest


def require_pylance() -> types.ModuleType:
    """Official Lance format SDK: ``pip install pylance`` → ``import lance``.

    Not Microsoft's Pylance language server, and not the unrelated PyPI ``lance`` package.
    """
    pytest.importorskip("lance")
    lance = importlib.import_module("lance")
    if not hasattr(lance, "dataset"):
        pytest.skip("Install the Lance format SDK: pip install pylance")
    return lance
