from pyspark.sql import DataFrame
from pathlib import Path

class EnasicSchema:
    """Procesamiento de la fuente ENASIC 2022 para la capa silver"""

    def __init__(self, observatorio):
        self.spark = observatorio.spark
        self.database ="cuidados_observatorio"

        self.tablas = {
            "vivienda":"bronze_enasic_2022_tvivienda",
            "hogar": "bronze_enasic_2022_thogar",
            "hogar_unipersonal": "bronze_enasic_2022_thog_unip",
            "personas":"bronze_enasic_2022_tper_ele",
            "residentes":"bronze_enasic_2022_tcsdempo",
            "cuidadores":"bronze_enasic_2022_tpob_cui"
        }

        self.datos = {}

    def cargar_bronze(self):
        """Carga las tablas ENASIC registradas en Apache-hive"."""
        for nombre, tabla in self.tablas.items():
            nombre_completo = f"{self.database}.{tabla}"

            if not self.spark.catalog.tableExists(nombre_completo):
                raise ValueError(
                    f"No existe la tabla Bronze: {nombre_completo}"
                )
            self.datos[nombre] = self.spark.table(nombre_completo)

        return self.datos
    
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

    @staticmethod
    def exigir_columnas(df: DataFrame, columnas: list, tabla: str):
        """Comprueba que existan las columnas necesarias."""

        faltantes = [
            columna for columna in columnas
            if columna not in df.columns
        ]

        if faltantes:
            raise ValueError(
                f"Faltan columnas en {tabla}: {faltantes}"
            )