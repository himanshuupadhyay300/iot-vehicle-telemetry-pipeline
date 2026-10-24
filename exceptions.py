"""Custom exception hierarchy so callers can handle pipeline failures precisely."""
from __future__ import annotations


class PipelineError(Exception):
    """Base class for all pipeline errors."""


class ConfigError(PipelineError):
    """Configuration is missing, malformed or inconsistent."""


class IngestionError(PipelineError):
    """Source files could not be discovered, read or validated against the input contract."""


class DataQualityError(PipelineError):
    """A data quality gate failed (reserved for strict mode / fail-fast checks)."""
