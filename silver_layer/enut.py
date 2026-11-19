from pyspark.sql import DataFrame, functions as F
from .common import (
    SilverBase,
    ENUT_TABLAS,
    a_int,
    a_double,
    etiqueta,
)

SEXO = {1: "Hombre", 2: "Mujer"}

COND_AEE_DESC = {
    1: "Ocupada",
    2: "Desocupada",
    3: "Jubilada o pensionada",
    4: "Estudiante",
    5: "Quehaceres del hogar o cuidados",
    6: "Otra situación",
}

ENTIDADES = {
    9: "Ciudad de México",
    15: "Estado de México",
}


class EnutSilver(SilverBase):
    """
    Procesamiento de la fuente ENUT 2024 para la capa Silver según lineamientos PP7:
    - Conserva factores de expansión FAC_*.
    - Variables de tiempo de ENUT se limitan a CUID_INT_0A5_SIN_CP y CUID_INT_0A5_CON_CP (0 a 5 años).
    - No se realiza unión de registros entre encuestas.
    - Se estandarizan sexo, edad, COND_AEE, entidad y prestaciones laborales.
    """

    def __init__(self, observatorio):
        super().__init__(observatorio, ENUT_TABLAS, "enut_2024")
        self.silver_dfs = {}

    def transformar_viviendas(self) -> DataFrame:
        df = self.datos["vivienda"]
        self.exigir_columnas(df, ["LLAVEVIV", "FAC_VIV"], "tvivienda")
        df = df.dropDuplicates(["LLAVEVIV"])
        df = a_double(a_int(df, ["CVE_ENT", "TLOC"]), ["FAC_VIV"])
        self.guardar_silver(df, "viviendas")
        self.silver_dfs["viviendas"] = df
        return df

    def transformar_hogares(self) -> DataFrame:
        df = self.datos["hogar"]
        self.exigir_columnas(df, ["LLAVEHOG", "LLAVEVIV", "FAC_HOG"], "thogar")
        df = a_double(a_int(df, ["CVE_ENT", "TLOC"]), ["FAC_HOG"])
        self.guardar_silver(df, "hogares")
        self.silver_dfs["hogares"] = df
        return df

    def transformar_residentes(self) -> DataFrame:
        tsd = self.datos["residentes"]
        self.exigir_columnas(tsd, ["LLAVEVIV", "LLAVEHOG", "SEXO", "EDAD", "FAC_HOG"], "tsdem")
        tsd = a_double(a_int(tsd, ["SEXO", "EDAD", "CVE_ENT"]), ["FAC_HOG"])
        tsd = tsd.withColumn("sexo_desc", etiqueta("SEXO", SEXO))
        tsd = tsd.withColumn("grupo_edad",
            F.when(F.col("EDAD") <= 5, "0 a 5 años")
            .when(F.col("EDAD") <= 11, "6 a 11 años")
            .when(F.col("EDAD") <= 17, "12 a 17 años")
            .when(F.col("EDAD") <= 59, "18 a 59 años")
            .otherwise("60 años y más")
        )
        self.guardar_silver(tsd, "residentes")
        self.silver_dfs["residentes"] = tsd
        return tsd

    def transformar_tiempo_cuidados(self) -> DataFrame:
        """
        Limpieza de TVAR_CREA y cruce de prestaciones desde TMODULO:
        - Variables de tiempo de cuidado: CUID_INT_0A5_SIN_CP y CUID_INT_0A5_CON_CP.
        - Estandarización de COND_AEE, edad, sexo, entidad (CDMX / Edomex / Resto).
        - Prestaciones laborales P5_6_*: vacaciones, servicio médico, AFORE, guardería.
        """
        tvar = self.datos["var_crea"]
        tmod = self.datos["modulo"]

        self.exigir_columnas(
            tvar,
            ["LLAVEMOD", "LLAVEVIV", "LLAVEHOG", "SEXO", "EDAD", "CVE_ENT", "COND_AEE", "FAC_PER",
             "CUID_INT_0A5_SIN_CP", "CUID_INT_0A5_CON_CP"],
            "tvar_crea"
        )

        cols_int = ["SEXO", "EDAD", "CVE_ENT", "COND_AEE", "TLOC"]
        cols_double = ["FAC_PER", "CUID_INT_0A5_SIN_CP", "CUID_INT_0A5_CON_CP"]
        tvar = a_double(a_int(tvar, cols_int), cols_double)

        # Extraer prestaciones de tmodulo (vacaciones, sar/afore, médico, guardería)
        prest_cols = ["P5_6_1", "P5_6_2", "P5_6_4", "P5_6_5", "P5_6_7"]
        sub_mod = tmod.select(
            "LLAVEMOD",
            *[F.col(c).cast("int").alias(c.lower()) for c in prest_cols if c in tmod.columns]
        )

        cui_enut = tvar.join(sub_mod, "LLAVEMOD", "left")

        cui_enut = (
            cui_enut
            .withColumn("sexo_desc", etiqueta("SEXO", SEXO))
            .withColumn("cond_aee_desc", etiqueta("COND_AEE", COND_AEE_DESC))
            .withColumn("entidad_desc",
                F.when(F.col("CVE_ENT") == 9, "Ciudad de México")
                .when(F.col("CVE_ENT") == 15, "Estado de México")
                .otherwise("Otra entidad")
            )
            .withColumn("es_cuidador_0a5",
                (F.coalesce(F.col("CUID_INT_0A5_SIN_CP") > 0, F.lit(False)) |
                 F.coalesce(F.col("CUID_INT_0A5_CON_CP") > 0, F.lit(False))).cast("int")
            )
            .withColumn("grupo_edad",
                F.when(F.col("EDAD") <= 17, "15 a 17 años")
                .when(F.col("EDAD") <= 29, "18 a 29 años")
                .when(F.col("EDAD") <= 44, "30 a 44 años")
                .when(F.col("EDAD") <= 59, "45 a 59 años")
                .otherwise("60 años y más")
            )
            # Prestaciones laborales indicadoras (1 = Sí, 2 = No)
            .withColumn("tiene_vacaciones", F.when(F.col("p5_6_2") == 1, 1).when(F.col("p5_6_2") == 2, 0))
            .withColumn("tiene_afore", F.when(F.col("p5_6_4") == 1, 1).when(F.col("p5_6_4") == 2, 0))
            .withColumn("tiene_guarderia", F.when(F.col("p5_6_5") == 1, 1).when(F.col("p5_6_5") == 2, 0))
            .withColumn("tiene_servicio_medico", F.when(F.col("p5_6_7") == 1, 1).when(F.col("p5_6_7") == 2, 0))
        )

        self.guardar_silver(cui_enut, "cuidadores_tiempo")
        self.silver_dfs["cuidadores_tiempo"] = cui_enut
        return cui_enut

    def procesar_todo(self):
        """Ejecuta la canalización Silver de ENUT 2024."""
        print("\n--- INICIANDO PROCESAMIENTO SILVER ENUT 2024 ---")
        self.cargar_bronze()
        self.transformar_viviendas()
        self.transformar_hogares()
        self.transformar_residentes()
        self.transformar_tiempo_cuidados()
        print("--- CAPA SILVER ENUT COMPLETADA CON ÉXITO ---\n")
