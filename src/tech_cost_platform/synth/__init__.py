"""Deterministic synthetic source-data generator for packet P-002."""

from .generate import DEFAULT_GL_TOTAL_EUR, DEFAULT_SYNTH_CONFIG, generate_source_exports, main

__all__ = [
    "DEFAULT_GL_TOTAL_EUR",
    "DEFAULT_SYNTH_CONFIG",
    "generate_source_exports",
    "main",
]
