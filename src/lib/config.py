"""Central project configuration.

Switching environments only requires a different catalog: every table name and
storage path is derived from it, so no notebook contains hard-coded locations.
"""
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    """Environment-level settings shared by every notebook.

    Attributes:
        catalog: Unity Catalog catalog for the environment (e.g. "dev", "prod").
        pipeline: Pipeline name recorded in the audit log.
    """
    catalog: str = "workspace"
    pipeline: str = "bakehouse"

    @property
    def landing_path(self) -> str:
        """Volume path where source systems deliver their files."""
        return f"/Volumes/{self.catalog}/raw/landing"

    @property
    def checkpoint_path(self) -> str:
        """Volume path for Auto Loader and streaming checkpoints."""
        return f"/Volumes/{self.catalog}/ops/checkpoints"

    def table(self, layer: str, name: str) -> str:
        """Fully qualified table name: <catalog>.<layer>.<name>."""
        return f"{self.catalog}.{layer}.{name}"