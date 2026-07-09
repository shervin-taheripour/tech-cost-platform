"""Shared repository-path helpers for the local runtime."""

from __future__ import annotations

from pathlib import Path


def repo_root() -> Path:
    """Return the repository root from any package module."""
    return Path(__file__).resolve().parents[2]


def resolve_repo_path(path_value: str | Path) -> Path:
    """Resolve a repo-relative path into an absolute filesystem path."""
    path = Path(path_value)
    return path if path.is_absolute() else repo_root() / path
