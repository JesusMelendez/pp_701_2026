from functools import reduce
from pyspark.sql import DataFrame, functions as F
from .common import (
    SilverBase,
    ENASIC_TABLAS,
    a_int,
    a_double,
    etiqueta,
)

SEXO = {1: "Hombre", 2: "Mujer"}

GRUPOS = {
    "DISC": dict(pn="PN_CDISC", filtro="P4_1",  cuid=["P4_2_1", "P4_2_2", "P4_2_3", "P4_3"],     otro="P4_4"),
    "0A5":  dict(pn="PN_C0005", filtro="P4_16", cuid=["P4_17_1", "P4_17_2", "P4_17_3", "P4_18"], otro="P4_19"),
    "6A17": dict(pn="PN_C0617", filtro="P4_27", cuid=["P4_28_1", "P4_28_2", "P4_28_3", "P4_29"], otro="P4_30"),
    "60YM": dict(pn="PN_C60MA", filtro="P4_42", cuid=["P4_43_1", "P4_43_2", "P4_43_3", "P4_44"], otro="P4_45"),
    "TEM":  dict(pn="PN_CETEM", filtro="P4_58", cuid=["P4_59_1", "P4_59_2", "P4_59_3", "P4_60"], otro="P4_61"),
}

NOMBRE_GRUPO = {
    "DISC": "Discapacidad o dependencia",
    "0A5": "0 a 5 años",
    "6A17": "6 a 17 años",
    "60YM": "60 años y más",
    "TEM": "Enfermedad temporal",
}

PARENTESCO_OTRO_HOGAR = {
    1: "Madre", 2: "Padre", 3: "Abuela", 4: "Abuelo", 5: "Tía", 6: "Tío", 7: "Otro",
    8: "Trabajador(a) doméstico(a)", 9: "Enfermera(o) o persona cuidadora",
    10: "Otro parentesco (mujer)", 11: "Otro parentesco (hombre)",
    12: "Sin parentesco (mujer)", 13: "Sin parentesco (hombre)"
}

NIVEL_ESC = {
    1: "Sin escolaridad", 2: "Primaria incompleta", 3: "Primaria completa",
    4: "Secundaria incompleta", 5: "Secundaria completa",
    6: "Preparatoria o equivalente", 7: "Licenciatura", 8: "Posgrado"
}

HIJOS = {
    1: "Sin hijos(as)", 2: "1 hijo(a)", 3: "2 hijos(as)",
    4: "3 hijos(as)", 5: "4 hijos(as)", 6: "5 o más hijos(as)"
}

MOTIVO = {
    1: "Obligación", 2: "Decisión familiar", 3: "Era la única persona que podía",
    4: "Petición de quien necesita el cuidado", 5: "Iniciativa propia"
}


class EnasicSilver(SilverBase):
    """Procesamiento de la fuente ENASIC 2022 para la capa Silver."""

    def __init__(self, observatorio):
        super().__init__(observatorio, ENASIC_TABLAS, "enasic_2022")
        self.silver_dfs = {}

    def transformar_viviendas(self) -> DataFrame:
        """Limpieza y tipado de la tabla de viviendas."""
        df = self.datos["vivienda"]
        self.exigir_columnas(df, ["LLAVEVIV"], "TVIVIENDA")
        df = df.dropDuplicates(["LLAVEVIV"])
        df = a_double(df, ["FAC_VIV"])
        self.guardar_silver(df, "viviendas")
        self.silver_dfs["viviendas"] = df
        return df

    def transformar_hogares(self) -> DataFrame:
        """Limpieza y tipado de la tabla de hogares."""
        df = self.datos["hogar"]
        self.exigir_columnas(df, ["LLAVEHOG", "FAC_HOG"], "THOGAR")
        df = a_double(
            a_int(df, ["SEXO_JEFE", "EDAD_JEFE", "P2_4_1", "HN_C0005", "HOG_UNI"]),
            ["FAC_HOG"]
        )
        self.guardar_silver(df, "hogares")
        self.silver_dfs["hogares"] = df
        return df

    def transformar_residentes(self) -> DataFrame:
        """Procesa TCSDEMPO: subpoblaciones, susceptibilidad y recepción de cuidados (0 a 11 y otros)."""
        tsd = self.datos["residentes"]
        self.exigir_columnas(
            tsd,
            ["LLAVEVIV", "LLAVEHOG", "LLAVESDE", "EDAD", "SEXO", "FAC_HOG", "UPM_DIS", "EST_DIS"],
            "TCSDEMPO"
        )

        p3_7 = [f"P3_7_{i}" for i in range(1, 8)]
        p4_69 = [f"P4_69_{i}" for i in range(1, 7)]
        p4_ninos = ["P4_20", "P4_21", "P4_22", "P4_23", "P4_24"] + [f"P4_25_{i}" for i in range(1, 7)]

        enteras = (
            ["EDAD", "SEXO", "P3_8A"] + p3_7 + p4_69 + p4_ninos
            + [v for g in GRUPOS.values() for v in [g["pn"], g["filtro"], g["otro"]] + g["cuid"]]
        )
        self.exigir_columnas(tsd, p3_7 + p4_69 + [g["pn"] for g in GRUPOS.values()], "TCSDEMPO")
        tsd = a_double(a_int(tsd, enteras), ["FAC_HOG"])
        tsd = tsd.withColumn("sexo_desc", etiqueta("SEXO", SEXO))

        # Población susceptible de cuidado (INEGI)
        disc = reduce(lambda a, b: a | b, [F.coalesce(F.col(c).isin(3, 4), F.lit(False)) for c in p3_7])
        if "P3_8A" in tsd.columns:
            disc = disc | F.coalesce(F.col("P3_8A") == 1, F.lit(False))

        tsd = (
            tsd.withColumn("susc_disc", disc.cast("int"))
            .withColumn("susc_0a5", F.col("EDAD").between(0, 5).cast("int"))
            .withColumn("susc_6a17", F.col("EDAD").between(6, 17).cast("int"))
            .withColumn("susc_6a11", F.col("EDAD").between(6, 11).cast("int"))
            .withColumn("susc_0a11", F.col("EDAD").between(0, 11).cast("int"))
            .withColumn("susc_60ym", F.col("EDAD").between(60, 97).cast("int"))
        )

        # Población cuidadora (Cuadro 6)
        tsd = tsd.withColumn(
            "es_cuidadora",
            F.greatest(*[F.coalesce(F.col(c).isin(1, 9), F.lit(False)).cast("int") for c in p4_69])
        )

        # Edad de la persona cuidadora
        lk = tsd.select(
            F.col("LLAVEHOG").alias("_lh"),
            F.substring(F.col("LLAVESDE"), -2, 2).cast("int").alias("_nr"),
            F.col("EDAD").alias("_ed")
        )

        def con_edad_cuidador(df, col):
            j = df.join(F.broadcast(lk), (F.col("LLAVEHOG") == F.col("_lh")) & (F.col(col) == F.col("_nr")), "left")
            return j.withColumn(f"{col}_ED", F.col("_ed")).drop("_lh", "_nr", "_ed")

        for g, d in GRUPOS.items():
            if not all(c in tsd.columns for c in d["cuid"] + [d["filtro"], d["otro"]]):
                continue
            for c in d["cuid"]:
                tsd = con_edad_cuidador(tsd, c)
            adulto = {c: F.coalesce(F.col(f"{c}_ED").between(15, 98), F.lit(False)) for c in d["cuid"]}
            alguno = reduce(lambda a, b: a | b, adulto.values())
            principal = adulto[d["cuid"][-1]]
            cond = (F.col(d["pn"]) == 1) & (
                ((F.col(d["filtro"]) == 1) & alguno) |
                ((F.col(d["filtro"]) == 2) & principal) |
                (F.col(d["otro"]) == 1)
            )
            tsd = tsd.withColumn(f"recibe_{g.lower()}", F.coalesce(cond.cast("int"), F.lit(0)))
            tsd = tsd.drop(*[f"{c}_ED" for c in d["cuid"]])

        recibe_cols = [f"recibe_{g.lower()}" for g in GRUPOS if f"recibe_{g.lower()}" in tsd.columns]
        sin_tem = [c for c in recibe_cols if c != "recibe_tem"]
        tsd = tsd.withColumn("recibe_cuidados", F.greatest(*[F.col(c) for c in sin_tem]))

        # Flag de recepción de cuidado para 0 a 11 años según lineamiento PP7_avance:
        # Para 0-5 se toma recibe_0a5; para 6-11 se toma de TCSDemPO (recibe_6a17 filtrado a edad 6-11)
        tsd = tsd.withColumn(
            "recibe_infantil_0a11",
            F.when(F.col("EDAD").between(0, 5), F.col("recibe_0a5"))
            .when(F.col("EDAD").between(6, 11), F.col("recibe_6a17"))
            .otherwise(F.lit(0))
        )

        self.guardar_silver(tsd, "residentes")
        self.silver_dfs["residentes"] = tsd
        return tsd

    def transformar_cuidadores(self) -> DataFrame:
        """Procesa TPOB_CUI: características de la población cuidadora y prestaciones laborales."""
        cui = self.datos["cuidadores"]
        self.exigir_columnas(cui, ["LLAVEVIV", "LLAVEMOD", "FAC_CUI", "FILTRO6_1"], "TPOB_CUI")
        filtros6 = [f"FILTRO6_{i}" for i in (1, 2, 3, 6, 8)]
        p6_tareas = [f"P6_14_{i:02d}" for i in range(1, 11)]

        cols_int = (
            ["EDAD", "SEXO", "P5_6", "P5_8", "CPEA", "NIVEL", "CT_NHA",
             "P5_10_1", "P5_10_2", "P5_10_3", "P5_10_4", "P5_10_5", "P5_10_6", "P5_13",
             "P5_22_2", "P5_28_7", "P5_35_4", "P6_38", "P6_39_1", "P6_39_2", "P6_39_3", "P6_42"]
            + filtros6 + p6_tareas
        )
        cui = a_double(a_int(cui, cols_int), ["FAC_CUI"])

        if "SEXO" not in cui.columns or "EDAD" not in cui.columns:
            tsd = self.silver_dfs.get("residentes") or self.spark.table(f"{self.database}.silver_{self.fuente}_residentes")
            aux = tsd.select(F.col("LLAVESDE").alias("LLAVEMOD"), "SEXO", "EDAD")
            cui = cui.drop("SEXO", "EDAD").join(aux, "LLAVEMOD", "left")

        cui = cui.withColumn("sexo_desc", etiqueta("SEXO", SEXO))
        filtros = [F.coalesce(F.col(c) == 1, F.lit(False)) for c in filtros6 if c in cui.columns]
        cui = cui.withColumn("es_cuidadora", reduce(lambda a, b: a | b, filtros).cast("int"))
        cui = cui.withColumn("cuida_0a5", F.coalesce(F.col("FILTRO6_2") == 1, F.lit(False)).cast("int"))

        if "CPEA" in cui.columns:
            cui = cui.withColumn("pea", F.coalesce(F.col("CPEA") == 1, F.lit(False)).cast("int"))
        else:
            pea = (F.col("P5_6").isin(1, 2, 3)) | (F.col("P5_6").isin(4, 5, 6, 8) & F.col("P5_8").between(1, 6))
            cui = cui.withColumn("pea", F.coalesce(pea, F.lit(False)).cast("int"))

        # Prestaciones laborales estándar (PP7_avance: Vacaciones, Servicios médicos, AFORE)
        cui = (
            cui
            .withColumn("tiene_vacaciones", F.when(F.col("P5_10_2") == 1, 1).when(F.col("P5_10_2") == 2, 0))
            .withColumn("tiene_afore", F.when(F.col("P5_10_3") == 1, 1).when(F.col("P5_10_3") == 2, 0))
            .withColumn("tiene_guarderia", F.when(F.col("P5_10_4") == 1, 1).when(F.col("P5_10_4") == 2, 0))
            .withColumn("tiene_servicio_medico", F.when(F.col("P5_10_6") == 1, 1).when(F.col("P5_10_6") == 2, 0))
            .withColumn("es_remunerado", F.when(F.col("pea") == 1, "Remunerado").otherwise("No remunerado"))
        )

        self.guardar_silver(cui, "cuidadores")
        self.silver_dfs["cuidadores"] = cui
        return cui

    def transformar_ninos_0a5(self) -> DataFrame:
        """Construye tabla especializada de primera infancia (0 a 5 años)."""
        tsd = self.silver_dfs.get("residentes") or self.spark.table(f"{self.database}.silver_{self.fuente}_residentes")
        p4_ninos = ["P4_20", "P4_21", "P4_22", "P4_23", "P4_24"] + [f"P4_25_{i}" for i in range(1, 7)]
        self.exigir_columnas(
            tsd,
            ["PN_C0005", "P4_16", "P4_17_1", "P4_17_2", "P4_17_3", "P4_18", "P4_19", "recibe_0a5"] + p4_ninos[:5],
            "TCSDEMPO (0 a 5 años)"
        )

        lk2 = tsd.select(
            F.col("LLAVEHOG").alias("_h2"),
            F.substring(F.col("LLAVESDE"), -2, 2).cast("int").alias("_n2"),
            F.col("EDAD").alias("cp_edad"),
            F.col("SEXO").alias("cp_sexo")
        )
        nin = tsd.filter(F.col("EDAD").between(0, 5))
        renglones = [F.when(F.col(c).between(1, 12), F.col(c)) for c in ["P4_17_1", "P4_17_2", "P4_17_3", "P4_18"]]
        nin = nin.withColumn("n_cuidadores", F.size(F.array_distinct(F.filter(F.array(*renglones), lambda x: x.isNotNull()))))
        nin = (
            nin.join(F.broadcast(lk2), (F.col("LLAVEHOG") == F.col("_h2")) & (F.col("P4_18") == F.col("_n2")), "left")
            .drop("_h2", "_n2")
        )

        nin = (
            nin
            .withColumn("edad_nino", F.col("EDAD").cast("string"))
            .withColumn("sexo_nino", F.col("sexo_desc"))
            .withColumn("necesita_cuidados", (F.col("PN_C0005") == 1).cast("int"))
            .withColumn("cat_num_cuidadores", F.when(F.col("n_cuidadores") >= 3, "3 o más").otherwise(F.col("n_cuidadores").cast("string")))
            .withColumn("cat_cuidador_principal", F.when(F.col("P4_18") == 0, "Nadie del hogar")
                        .when(F.col("cp_edad").isNotNull(), "Persona del hogar")
                        .otherwise("No especificado"))
            .withColumn("cp_sexo_desc", etiqueta("cp_sexo", SEXO))
            .withColumn("cp_grupo_edad",
                        F.when(F.col("cp_edad").isNull(), None)
                         .when(F.col("cp_edad") < 15, "Menor de 15")
                         .when(F.col("cp_edad") <= 17, "15 a 17")
                         .when(F.col("cp_edad") <= 29, "18 a 29")
                         .when(F.col("cp_edad") <= 44, "30 a 44")
                         .when(F.col("cp_edad") <= 59, "45 a 59")
                         .otherwise("60 y más"))
            .withColumn("cuidado_otro_hogar", F.when(F.col("P4_19") == 1, "Sí").when(F.col("P4_19") == 2, "No"))
            .withColumn("parentesco_otro_hogar", etiqueta("P4_20", PARENTESCO_OTRO_HOGAR))
            .withColumn("pago_otro_hogar", F.when(F.col("P4_21") == 1, "Sí").when(F.col("P4_21") == 2, "No"))
            .withColumn("monto_semanal_otro_hogar", F.when(F.col("P4_22").between(1, 97999), F.col("P4_22").cast("double")))
            .withColumn("tiempo_solo", F.when(F.col("P4_23") == 0, "No se queda sin cuidado")
                        .when(F.col("P4_23").between(1, 2), "1 a 2 horas al día")
                        .when(F.col("P4_23").between(3, 5), "3 a 5 horas al día")
                        .when(F.col("P4_23") >= 6, "6 horas o más al día"))
            .withColumn("horas_solo", F.when(F.col("P4_23") > 0, F.col("P4_23").cast("double")))
            .withColumn("necesita_mas_cuidado", F.when(F.col("P4_24") == 1, "Sí").when(F.col("P4_24") == 2, "No"))
        )

        self.guardar_silver(nin, "ninos_0a5")
        self.silver_dfs["ninos_0a5"] = nin
        return nin

    def transformar_cuidadores_0a5(self) -> DataFrame:
        """Construye tabla especializada de cuidadores de primera infancia."""
        cui = self.silver_dfs.get("cuidadores") or self.spark.table(f"{self.database}.silver_{self.fuente}_cuidadores")
        c05 = (
            cui.filter(F.col("cuida_0a5") == 1)
            .withColumn("sexo_cuidador", F.col("sexo_desc"))
            .withColumn("grupo_edad", F.when(F.col("EDAD") >= 98, "No especificado")
                        .when(F.col("EDAD") <= 17, "15 a 17").when(F.col("EDAD") <= 29, "18 a 29")
                        .when(F.col("EDAD") <= 44, "30 a 44").when(F.col("EDAD") <= 59, "45 a 59")
                        .otherwise("60 y más"))
            .withColumn("escolaridad", etiqueta("NIVEL", NIVEL_ESC))
            .withColumn("numero_hijos", etiqueta("CT_NHA", HIJOS))
            .withColumn("condicion_actividad", F.when(F.col("pea") == 1, "Económicamente activa(o)").otherwise("No económicamente activa(o)"))
            .withColumn("motivo_cuidado", etiqueta("P6_38", MOTIVO))
            .withColumn("horas_trabajadas", F.when(F.col("P5_13").between(0, 98), F.col("P5_13").cast("double")))
        )
        self.guardar_silver(c05, "cuidadores_0a5")
        self.silver_dfs["cuidadores_0a5"] = c05
        return c05

    def transformar_hogares_0a5(self) -> DataFrame:
        """Construye tabla de hogares vinculada a necesidades de cuidado 0 a 5 años."""
        tsd = self.silver_dfs.get("residentes") or self.spark.table(f"{self.database}.silver_{self.fuente}_residentes")
        hog = self.silver_dfs.get("hogares") or self.spark.table(f"{self.database}.silver_{self.fuente}_hogares")

        por_hogar = tsd.groupBy("LLAVEHOG").agg(
            F.max((F.col("PN_C0005") == 1).cast("int")).alias("tiene_0a5"),
            F.max(((F.col("PN_C0005") == 1) & (F.col("recibe_0a5") == 0)).cast("int")).alias("sin_cuidado_adulto")
        )
        h05 = (
            hog.join(por_hogar, "LLAVEHOG", "left")
            .fillna(0, subset=["tiene_0a5", "sin_cuidado_adulto"])
            .withColumn("jefatura", F.when(F.col("SEXO_JEFE") == 2, "Mujer").when(F.col("SEXO_JEFE") == 1, "Hombre"))
        )
        self.guardar_silver(h05, "hogares_0a5")
        self.silver_dfs["hogares_0a5"] = h05
        return h05

    def procesar_todo(self):
        """Ejecuta toda la canalización Silver de ENASIC."""
        print("\n--- INICIANDO PROCESAMIENTO SILVER ENASIC 2022 ---")
        self.cargar_bronze()
        self.transformar_viviendas()
        self.transformar_hogares()
        self.transformar_residentes()
        self.transformar_cuidadores()
        self.transformar_ninos_0a5()
        self.transformar_cuidadores_0a5()
        self.transformar_hogares_0a5()
        print("--- CAPA SILVER ENASIC COMPLETADA CON ÉXITO ---\n")