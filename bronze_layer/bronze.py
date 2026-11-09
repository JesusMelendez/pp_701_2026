
from pathlib import Path
from datetime import datetime, timezone
import hashlib
import json


class BronzeLayer:
    """Utilidades básicas de identificación y trazabilidad Bronze."""

    def __init__(self, staging_dir, manifest_dir=None):
        self.staging_dir = Path(staging_dir)

        self.manifest_dir = (
            Path(manifest_dir)
            if manifest_dir
            else self.staging_dir / "metadata"
        )

        self.manifest_dir.mkdir(parents=True, exist_ok=True)

    def calcular_hash(self, archivo):
        """Calcula el hash SHA-256 de un archivo."""

        sha256 = hashlib.sha256()

        with open(archivo, "rb") as f:
            for bloque in iter(lambda: f.read(1024 * 1024), b""):
                sha256.update(bloque)

        return sha256.hexdigest()

    def validar_archivo(self, archivo):
        """Comprueba que el archivo exista y tenga contenido."""

        archivo = Path(archivo)

        if not archivo.is_file():
            return False

        if archivo.stat().st_size == 0:
            return False

        return True

    def obtener_metadatos(self, archivo, fuente):
        """Obtiene información básica de un archivo original."""

        archivo = Path(archivo)

        if not self.validar_archivo(archivo):
            raise ValueError(f"Archivo no válido: {archivo}")

        stat = archivo.stat()

        return {
            "fuente": fuente,
            "nombre_archivo": archivo.name,
            "ruta_origen": str(archivo.resolve()),
            "extension": archivo.suffix.lower(),
            "tamano_bytes": stat.st_size,
            "fecha_modificacion": datetime.fromtimestamp(
                stat.st_mtime,
                tz=timezone.utc
            ).isoformat(),
            "fecha_registro": datetime.now(
                timezone.utc
            ).isoformat(),
            "sha256": self.calcular_hash(archivo)
        }

    def generar_manifiesto(self, archivos):
        """Genera un inventario de los archivos existentes."""

        registros = []

        for archivo, fuente in archivos:
            try:
                registros.append(
                    self.obtener_metadatos(archivo, fuente)
                )
            except (OSError, ValueError) as error:
                registros.append({
                    "fuente": fuente,
                    "ruta_origen": str(archivo),
                    "estado": "error",
                    "error": str(error)
                })

        manifiesto = {
            "fecha_generacion": datetime.now(
                timezone.utc
            ).isoformat(),
            "total_archivos": len(registros),
            "archivos": registros
        }

        destino = (
            self.manifest_dir / "bronze_manifest.json"
        )

        with open(destino, "w", encoding="utf-8") as f:
            json.dump(
                manifiesto,
                f,
                ensure_ascii=False,
                indent=4
            )

        return destino

