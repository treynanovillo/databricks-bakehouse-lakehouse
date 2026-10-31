"""Configuración central del proyecto. Cambiar de entorno = cambiar el catálogo."""
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    catalog: str = "workspace"          # en una empresa: "dev", "prod"...
    pipeline: str = "bakehouse"

    @property
    def landing_path(self) -> str:
        return f"/Volumes/{self.catalog}/raw/landing"

    @property
    def checkpoint_path(self) -> str:
        return f"/Volumes/{self.catalog}/ops/checkpoints"

    def table(self, layer: str, name: str) -> str:
        """Nombre completo de una tabla: catalogo.capa.tabla"""
        return f"{self.catalog}.{layer}.{name}"