from pathlib import Path

from .common import SilverBase, ENASIC_TABLAS


class EnasicSchema(SilverBase):
    """Inspección de esquema de la fuente ENASIC 2022."""

    def __init__(self, observatorio):
        super().__init__(observatorio, ENASIC_TABLAS, "enasic_2022")

    def mostrar_arbol(self):
        if not self.datos:
            self.cargar_bronze()

        lineas = ["ENASIC 2022", ""]

        tablas = list(self.datos.items())

        for i, (nombre, df) in enumerate(tablas):
            ultima_tabla = i == len(tablas) -1
            rama = "└──" if ultima_tabla else "├──"

            lineas.append(f"{rama} {nombre.upper()}")

            campos = df.schema.fields

            for j, campo in enumerate(campos):
                ultimo_campo = j == len(campos) - 1

                prefijo = "     " if ultima_tabla else "|   "
                rama_campo = "└──" if ultimo_campo else "├──"

                lineas.append(
                    f"{prefijo}{rama_campo}"
                    f"{campo.name} : {campo.dataType}"
                )
            lineas.append("")

            #Mostrar en terminal
            arbol = "\n".join(lineas)
            print(arbol)

            #guardar en la raíz del proyecto
            ruta_salida = (
                Path(__file__).resolve().parent.parent
                / "enasic_esquema.txt"
            )

            ruta_salida.write_text(
                arbol,
                encoding = "utf-8"
            )

            print(f"\nÁrbol completo guardado en: {ruta_salida}")