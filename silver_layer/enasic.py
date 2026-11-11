from pyspark.sql import DataFrame


class EnasicSilver:
    """Procesamiento de la fuente ENASIC 2022 para la capa Silver."""

    def __init__(self, observatorio):
        self.spark = observatorio.spark
        self.database = "cuidados_observatorio"

        self.tablas = {
            "vivienda": "bronze_enasic_2022_tvivienda",
            "hogar": "bronze_enasic_2022_thogar",
            "hogar_unipersonal": "bronze_enasic_2022_thog_unip",
            "personas": "bronze_enasic_2022_tper_ele",
            "residentes": "bronze_enasic_2022_tcsdempo",
            "cuidadores": "bronze_enasic_2022_tpob_cui",
        }

        self.datos = {}

    def cargar_bronze(self):
        """Carga las tablas ENASIC registradas en Hive."""

        self.datos = {}

        for nombre, tabla in self.tablas.items():
            nombre_completo = f"{self.database}.{tabla}"

            if not self.spark.catalog.tableExists(nombre_completo):
                raise ValueError(
                    f"No existe la tabla Bronze: {nombre_completo}"
                )

            self.datos[nombre] = self.spark.table(nombre_completo)

        return self.datos

    @staticmethod
    def exigir_columnas(
        df: DataFrame,
        columnas: list[str],
        tabla: str
    ):
        """Comprueba que existan las columnas necesarias."""

        faltantes = [
            columna
            for columna in columnas
            if columna not in df.columns
        ]

        if faltantes:
            raise ValueError(
                f"Faltan columnas en {tabla}: {faltantes}"
            )
    
    def transformar_viviendas(self):
        """Limpieza inicial de la tabla de viviendas"""
        df = self.datos["vivienda"]

        self.exigir_columnas(
            df,
            ["LLAVEVIV"],
            "TVIVIENDA"
        )

        df = df.dropDuplicates(["LLAVEVIV"])

        self.guardar_silver(df, "viviendas")

        return df

    def transformar_hogares(self):
        """Limpieza inicial de la tabla de hogares"""

        df = self.datos["hogar"]

        self.exigir_columnas(
            df,
            ["LLAVEVIV","LLAVEHOG"],
            "THOGAR"
        )

        self.guardar_silver(df,"hogares")
        return df

    def guardar_silver(self, df, nombre):
        """Escribe los resultados Silver en Parquet y Hive"""

        tabla = f"Silver_enasic_2022_{nombre}"

        ruta =(
            f"s3a://lakehouse/"
            f"{self.database}.db/silver/{tabla}"
        )

        (
            df.write
            .mode("overwrite")
            .format("parquet")
            .option("path", ruta)
            .saveAsTable(f"{self.database}.{tabla}")
        )

        print(f"silver generado: {tabla}")