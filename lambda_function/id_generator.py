"""
Deterministic UUID generation for transportistas.

Uses UUID v5 (SHA-1 based) with a fixed project namespace so the same
codigo_transportista always produces the same transportista_id, even
across full reloads.
"""
import uuid

NAMESPACE_TRANSPORTISTAS = uuid.UUID("b8f9e3a1-7c2d-4f5e-9a1b-3c4d5e6f7a8b")


def generate_transportista_id(codigo_transportista: str) -> str:
    return str(uuid.uuid5(NAMESPACE_TRANSPORTISTAS, codigo_transportista.strip()))
