import os
from functools import reduce
from pyspark.sql import DataFrame, functions as F
from silver_layer.common import (
    suma_condicional,
    rotular_total,
    distribucion,
    binarios,
    promedios,
    eq,
    en,
    aplicar_protocolo_publicacion,
)

SUSC = {
    "Discapacidad o dependencia": "susc_disc",
    "0 a 5 años": "susc_0a5",
    "6 a 11 años": "susc_6a11",
    "0 a 11 años": "susc_0a11",
    "6 a 17 años": "susc_6a17",
    "60 años y más": "susc_60ym"
}

GRUPOS_RECI = {
    "DISC": dict(pn="PN_CDISC", nombre="Discapacidad o dependencia"),
    "0A5":  dict(pn="PN_C0005", nombre="0 a 5 años"),
    "6A17": dict(pn="PN_C0617", nombre="6 a 17 años"),
    "60YM": dict(pn="PN_C60MA", nombre="60 años y más"),
    "TEM":  dict(pn="PN_CETEM", nombre="Enfermedad temporal"),
}

TIPOS_CUIDADO = [
    "Discapacidad o dependencia",
    "0 a 5 años",
    "6 a 17 años",
    "60 años y más",
    "Enfermedad temporal",
    "Otro hogar"
]

APOYOS_0A5 = {
    "P4_25_1": "Niñera(o)",
    "P4_25_2": "Educación inicial, estancia, guardería o preescolar",
    "P4_25_3": "Vecinas(os) o amistades",
    "P4_25_4": "Familiares de otra vivienda",
    "P4_25_5": "Más apoyo de integrantes del hogar",
    "P4_25_6": "Estimulación temprana, natación u otras actividades"
}

TAREAS_CUIDADORES_0A5 = {
    "P6_14_01": "Dar de comer",
    "P6_14_02": "Bañar, asear, vestir o arreglar",
    "P6_14_03": "Cargar o acostar",
    "P6_14_04": "Llevar o recoger de guardería, kínder o escuela",
    "P6_14_05": "Dar terapia especial o física",
    "P6_14_06": "Ayudar con tareas escolares",
    "P6_14_07": "Llevar a atención de salud",
    "P6_14_08": "Hacer compañía o estar al pendiente",
    "P6_14_09": "Platicar, jugar o esparcimiento",
    "P6_14_10": "Otra"
}


class EnasicGold:
    """Generación de indicadores agregados y tablas Gold para ENASIC 2022."""

    def __init__(self, observatorio, export_dir: str = None):
        self.spark = observatorio.spark
        self.database = observatorio.base_datos
        self.base_path = observatorio.base_path
        self.fuente = "enasic_2022"
        self.export_dir = export_dir or str(observatorio.directorio_datasets / "export" / self.fuente)
        os.makedirs(self.export_dir, exist_ok=True)

    def _tabla_silver(self, nombre: str) -> DataFrame:
        return self.spark.table(f"{self.database}.silver_{self.fuente}_{nombre}")

    def guardar_gold(self, df: DataFrame, nombre: str, orden: list[str] = None) -> DataFrame:
        """Escribe tabla Gold en Parquet/Hive y exporta copia en CSV."""
        if orden:
            df = df.orderBy(*orden)

        tabla = f"gold_{self.fuente}_{nombre}"
        ruta = f"{self.base_path}/gold/{tabla}"

        (
            df.write
            .mode("overwrite")
            .format("parquet")
            .option("path", ruta)
            .saveAsTable(f"{self.database}.{tabla}")
        )

        try:
            ruta_csv = os.path.join(self.export_dir, f"{nombre}.csv")
            df.toPandas().to_csv(ruta_csv, index=False, encoding="utf-8-sig")
            print(f"Gold generado: {tabla} -> exportado en {ruta_csv}")
        except Exception as e:
            print(f"Gold generado: {tabla} (Exportación CSV omitida: {e})")

        return df

    def generar_demanda_cuidados(self, tsd: DataFrame) -> DataFrame:
        """1. Demanda de cuidados: población susceptible entre población total."""
        dims = ["sexo_desc"]
        wide = tsd.cube(*dims).agg(
            F.sum("FAC_HOG").alias("pob_total"),
            *[suma_condicional(F.col(c) == 1, "FAC_HOG").alias(f"x{i}") for i, c in enumerate(SUSC.values())],
            F.count(F.lit(1)).alias("casos_muestrales")
        )
        demanda = reduce(
            DataFrame.unionByName,
            [
                wide.select(*dims, "pob_total", F.lit(g).alias("grupo"), F.col(f"x{i}").alias("pob_susceptible"), "casos_muestrales")
                for i, g in enumerate(SUSC)
            ]
        ).withColumn("demanda_pct", F.round(F.col("pob_susceptible") / F.col("pob_total") * 100, 2))
        res = rotular_total(demanda, dims)
        res = aplicar_protocolo_publicacion(res, "casos_muestrales", umbral=5, cols_valor=["pob_total", "pob_susceptible", "demanda_pct"])
        return self.guardar_gold(res, "demanda_cuidados", dims)

    def generar_recibe_cuidados(self, tsd: DataFrame) -> DataFrame:
        """2. Proporción de personas que reciben cuidados por grupo y sexo."""
        dims = ["sexo_desc"]
        partes = []
        for g, d in GRUPOS_RECI.items():
            col_recibe = f"recibe_{g.lower()}"
            if col_recibe not in tsd.columns:
                continue
            partes.append(
                tsd.cube(*dims).agg(
                    suma_condicional(F.col(d["pn"]) == 1, "FAC_HOG").alias("pob_susceptible"),
                    suma_condicional(F.col(col_recibe) == 1, "FAC_HOG").alias("pob_recibe_cuidados"),
                    F.count(F.lit(1)).alias("casos_muestrales")
                ).withColumn("grupo", F.lit(d["nombre"]))
            )
        recibe = reduce(DataFrame.unionByName, partes).withColumn(
            "recibe_pct", F.round(F.col("pob_recibe_cuidados") / F.col("pob_susceptible") * 100, 2)
        )
        res = rotular_total(recibe, dims)
        res = aplicar_protocolo_publicacion(res, "casos_muestrales", umbral=5, cols_valor=["pob_susceptible", "pob_recibe_cuidados", "recibe_pct"])
        return self.guardar_gold(res, "recibe_cuidados", dims)

    def generar_brecha_recepcion_infantil_0a11(self, tsd: DataFrame) -> DataFrame:
        """2.1 Brecha de recepción de cuidado infantil (0 a 11 años) - Requerimiento PP7."""
        dims = ["sexo_desc"]
        inf = tsd.filter(F.col("EDAD").between(0, 11))
        agg_inf = inf.cube(*dims).agg(
            F.sum("FAC_HOG").alias("pob_infantil_0a11"),
            suma_condicional(F.col("recibe_infantil_0a11") == 1, "FAC_HOG").alias("pob_recibe_cuidados"),
            suma_condicional(F.col("recibe_infantil_0a11") == 0, "FAC_HOG").alias("brecha_no_recibe_cuidados"),
            F.count(F.lit(1)).alias("casos_muestrales")
        ).withColumn(
            "cobertura_cuidado_pct",
            F.round(F.col("pob_recibe_cuidados") / F.col("pob_infantil_0a11") * 100, 2)
        ).withColumn(
            "brecha_desatencion_pct",
            F.round(F.col("brecha_no_recibe_cuidados") / F.col("pob_infantil_0a11") * 100, 2)
        )
        res = rotular_total(agg_inf, dims)
        res = aplicar_protocolo_publicacion(
            res,
            "casos_muestrales",
            umbral=5,
            cols_valor=["pob_infantil_0a11", "pob_recibe_cuidados", "brecha_no_recibe_cuidados", "cobertura_cuidado_pct", "brecha_desatencion_pct"]
        )
        return self.guardar_gold(res, "brecha_recepcion_infantil_0a11", dims)

    def generar_prestaciones_laborales_cuidadores(self, cui: DataFrame) -> DataFrame:
        """3. Prestaciones laborales y carga de quien cuida (PP7: Vacaciones, Servicios médicos, AFORE)."""
        dims = ["sexo_desc", "es_remunerado"]
        items = [
            ("Prestaciones laborales", "Vacaciones con goce de sueldo", eq("tiene_vacaciones", 1), en("tiene_vacaciones", [0, 1])),
            ("Prestaciones laborales", "Fondo para el retiro (AFORE / SAR)", eq("tiene_afore", 1), en("tiene_afore", [0, 1])),
            ("Prestaciones laborales", "Servicio médico (IMSS, ISSSTE u otro)", eq("tiene_servicio_medico", 1), en("tiene_servicio_medico", [0, 1])),
            ("Prestaciones laborales", "Guardería o estancia infantil", eq("tiene_guarderia", 1), en("tiene_guarderia", [0, 1])),
        ]
        res = binarios(cui.filter(F.col("es_cuidadora") == 1), dims, items, "FAC_CUI")
        return self.guardar_gold(res, "prestaciones_cuidadores", ["indicador"] + dims)

    def generar_participacion_cuidados(self, tsd: DataFrame) -> DataFrame:
        """4. Tasa de participación en cuidados (población de 15 años y más)."""
        dims = ["sexo_desc"]
        base15 = tsd.filter(F.col("EDAD").between(15, 98))
        p4_69 = [f"P4_69_{i}" for i in range(1, 7)]

        agg = base15.cube(*dims).agg(
            F.sum("FAC_HOG").alias("pob_15_y_mas"),
            suma_condicional(F.col("es_cuidadora") == 1, "FAC_HOG").alias("pob_cuidadora"),
            *[suma_condicional(en(c, [1, 9]), "FAC_HOG").alias(f"t{i}") for i, c in enumerate(p4_69)],
            F.count(F.lit(1)).alias("casos_muestrales")
        )
        part = rotular_total(
            agg.withColumn(
                "tasa_participacion_pct",
                F.round(F.col("pob_cuidadora") / F.col("pob_15_y_mas") * 100, 2)
            ),
            dims
        )
        part = aplicar_protocolo_publicacion(part, "casos_muestrales", umbral=5, cols_valor=["pob_15_y_mas", "pob_cuidadora", "tasa_participacion_pct"])
        self.guardar_gold(
            part.select(*dims, "pob_15_y_mas", "pob_cuidadora", "tasa_participacion_pct", "estatus_publicacion"),
            "participacion_cuidados",
            dims
        )

        tipos = reduce(
            DataFrame.unionByName,
            [
                agg.select(*dims, "pob_15_y_mas", F.lit(t).alias("tipo_cuidado"), F.col(f"t{i}").alias("pob_cuidadora"), "casos_muestrales")
                for i, t in enumerate(TIPOS_CUIDADO)
            ]
        ).withColumn("tasa_pct", F.round(F.col("pob_cuidadora") / F.col("pob_15_y_mas") * 100, 2))
        res_tipos = rotular_total(tipos, dims)
        res_tipos = aplicar_protocolo_publicacion(res_tipos, "casos_muestrales", umbral=5, cols_valor=["pob_15_y_mas", "pob_cuidadora", "tasa_pct"])
        return self.guardar_gold(res_tipos, "participacion_cuidados_por_tipo", dims)

    def generar_participacion_economica_cuidadores(self, cui: DataFrame) -> DataFrame:
        """5. Participación económica de la población cuidadora."""
        dims = ["sexo_desc"]
        econ = (
            cui.filter(F.col("es_cuidadora") == 1)
            .cube(*dims)
            .agg(
                F.sum("FAC_CUI").alias("pob_cuidadora"),
                suma_condicional(F.col("pea") == 1, "FAC_CUI").alias("pea_cuidadora"),
                F.count(F.lit(1)).alias("casos_muestrales")
            )
            .withColumn(
                "tasa_participacion_economica_pct",
                F.round(F.col("pea_cuidadora") / F.col("pob_cuidadora") * 100, 2)
            )
        )
        res = rotular_total(econ, dims)
        res = aplicar_protocolo_publicacion(res, "casos_muestrales", umbral=5, cols_valor=["pob_cuidadora", "pea_cuidadora", "tasa_participacion_economica_pct"])
        return self.guardar_gold(res, "participacion_economica_cuidadores", dims)

    def generar_indicadores_ninos_0a5(self, nin: DataFrame, tsd: DataFrame) -> dict[str, DataFrame]:
        """6. Indicadores de niños y niñas de 0 a 5 años."""
        d2 = ["edad_nino", "sexo_nino"]
        sn = ["sexo_nino"]
        total_pob = tsd.agg(F.sum("FAC_HOG")).first()[0]

        # 6.1 Población de 0 a 5 años
        pob05 = nin.cube(*d2).agg(
            F.sum("FAC_HOG").alias("pob_0a5"),
            suma_condicional(F.col("necesita_cuidados") == 1, "FAC_HOG").alias("pob_sin_discapacidad"),
            F.count(F.lit(1)).alias("casos_muestrales")
        )
        pob05 = rotular_total(
            pob05.withColumn("pct_poblacion_total", F.round(F.col("pob_0a5") / F.lit(total_pob) * 100, 2)),
            d2
        )
        pob05 = aplicar_protocolo_publicacion(pob05, "casos_muestrales", umbral=5, cols_valor=["pob_0a5", "pob_sin_discapacidad", "pct_poblacion_total"])
        self.guardar_gold(pob05, "n05_poblacion", d2)

        # 6.2 Recibe cuidados
        rec05 = nin.filter(F.col("necesita_cuidados") == 1).cube(*d2).agg(
            F.sum("FAC_HOG").alias("pob_necesita_cuidados"),
            suma_condicional(F.col("recibe_0a5") == 1, "FAC_HOG").alias("pob_recibe_cuidados"),
            F.count(F.lit(1)).alias("casos_muestrales")
        )
        rec05 = rotular_total(
            rec05.withColumn(
                "recibe_pct",
                F.round(F.col("pob_recibe_cuidados") / F.col("pob_necesita_cuidados") * 100, 2)
            ),
            d2
        )
        rec05 = aplicar_protocolo_publicacion(rec05, "casos_muestrales", umbral=5, cols_valor=["pob_necesita_cuidados", "pob_recibe_cuidados", "recibe_pct"])
        self.guardar_gold(rec05, "n05_recibe_cuidados", d2)

        # 6.3 Quién y cómo los cuidan
        nc = nin.filter(F.col("necesita_cuidados") == 1)
        dists = [
            distribucion(nc, sn, "cat_num_cuidadores", "FAC_HOG", "Número de personas del hogar que lo cuidan"),
            distribucion(nc, sn, "cat_cuidador_principal", "FAC_HOG", "Cuenta con persona cuidadora principal en el hogar"),
            distribucion(nc, sn, "cp_sexo_desc", "FAC_HOG", "Sexo de la persona cuidadora principal"),
            distribucion(nc, sn, "cp_grupo_edad", "FAC_HOG", "Edad de la persona cuidadora principal"),
            distribucion(nc, sn, "cuidado_otro_hogar", "FAC_HOG", "Cuidado por una persona de otro hogar"),
            distribucion(nc.filter(F.col("P4_19") == 1), sn, "parentesco_otro_hogar", "FAC_HOG", "Parentesco de la persona de otro hogar"),
            distribucion(nc.filter(F.col("P4_19") == 1), sn, "pago_otro_hogar", "FAC_HOG", "El cuidado de otro hogar fue a cambio de un pago"),
            distribucion(nc, sn, "tiempo_solo", "FAC_HOG", "Tiempo que se queda sin cuidado en el hogar"),
            distribucion(nc, sn, "necesita_mas_cuidado", "FAC_HOG", "Necesita más tiempo de cuidado u otro tipo de atención"),
        ]
        self.guardar_gold(reduce(DataFrame.unionByName, dists), "n05_distribuciones", ["indicador", "sexo_nino", "categoria"])

        # 6.4 Apoyos deseados
        mas = nc.filter(F.col("P4_24") == 1)
        items_apoyos = [
            ("Apoyo que complementaría el cuidado", etq, eq(c, 1), en(c, [1, 2]))
            for c, etq in APOYOS_0A5.items()
        ]
        self.guardar_gold(binarios(mas, sn, items_apoyos, "FAC_HOG").drop("tema"), "n05_apoyos_deseados", ["indicador", "sexo_nino"])

        # 6.5 Promedios
        prom = promedios(
            nc,
            sn,
            [
                ("Tiempo sin cuidado", "Horas diarias que se queda sin cuidado (quienes se quedan)", "horas_solo", F.lit(True)),
                ("Cuidado de otro hogar", "Pago semanal por el cuidado de otro hogar, en pesos (quienes pagan)", "monto_semanal_otro_hogar", eq("P4_21", 1)),
            ],
            "FAC_HOG"
        )
        self.guardar_gold(prom, "n05_promedios", ["indicador", "sexo_nino"])

    def generar_indicadores_cuidadores_0a5(self, c05: DataFrame, cui: DataFrame) -> dict[str, DataFrame]:
        """7. Perfil, participación económica y tareas de cuidadores de 0 a 5 años."""
        sc = ["sexo_cuidador"]

        # Perfil
        perf = [
            distribucion(c05, [], "sexo_cuidador", "FAC_CUI", "Sexo"),
            distribucion(c05, sc, "grupo_edad", "FAC_CUI", "Edad"),
            distribucion(c05, sc, "escolaridad", "FAC_CUI", "Escolaridad"),
            distribucion(c05, sc, "numero_hijos", "FAC_CUI", "Número de hijas e hijos"),
            distribucion(c05, sc, "condicion_actividad", "FAC_CUI", "Condición de actividad económica"),
            distribucion(c05, sc, "motivo_cuidado", "FAC_CUI", "Motivo por el que cuida"),
        ]
        perf = [p if "sexo_cuidador" in p.columns else p.withColumn("sexo_cuidador", F.lit("Total")) for p in perf]
        self.guardar_gold(
            reduce(DataFrame.unionByName, [p.select("indicador", "sexo_cuidador", "categoria", "poblacion", "pct", "casos_muestrales", "estatus_publicacion") for p in perf]),
            "cui05_perfil",
            ["indicador", "sexo_cuidador", "categoria"]
        )

        # Participación económica comparativa
        cui_s = cui.withColumn("sexo_cuidador", F.col("sexo_desc"))
        tpe = reduce(
            DataFrame.unionByName,
            [
                cui_s.filter(F.col("cuida_0a5") == 1).cube(*sc).agg(
                    F.sum("FAC_CUI").alias("pob_cuidadora"),
                    suma_condicional(F.col("pea") == 1, "FAC_CUI").alias("pea_cuidadora"),
                    F.count(F.lit(1)).alias("casos_muestrales")
                ).withColumn("grupo", F.lit("Cuida a niñas y niños de 0 a 5 años")),
                cui_s.filter(F.col("es_cuidadora") == 1).cube(*sc).agg(
                    F.sum("FAC_CUI").alias("pob_cuidadora"),
                    suma_condicional(F.col("pea") == 1, "FAC_CUI").alias("pea_cuidadora"),
                    F.count(F.lit(1)).alias("casos_muestrales")
                ).withColumn("grupo", F.lit("Todas las personas cuidadoras")),
            ]
        ).withColumn("tasa_participacion_economica_pct", F.round(F.col("pea_cuidadora") / F.col("pob_cuidadora") * 100, 2))
        res_tpe = rotular_total(tpe, sc)
        res_tpe = aplicar_protocolo_publicacion(res_tpe, "casos_muestrales", umbral=5, cols_valor=["pob_cuidadora", "pea_cuidadora", "tasa_participacion_economica_pct"])
        self.guardar_gold(res_tpe, "cui05_participacion_economica", ["grupo", "sexo_cuidador"])

        # Tareas, condiciones laborales y percepción
        items_cui = [("Tareas de cuidado de 0 a 5 años", t, eq(c, 1), en(c, [1, 2])) for c, t in TAREAS_CUIDADORES_0A5.items()]
        items_cui += [
            ("Condiciones laborales", "Su trabajo le da guardería o estancia infantil", eq("P5_10_4", 1), en("P5_10_4", [1, 2])),
            ("Condiciones laborales", "Su trabajo permite tiempo de lactancia", eq("P5_28_7", 1), en("P5_28_7", [1, 2])),
            ("Condiciones laborales", "Cambiaría su horario para dedicar más tiempo a sus hijas(os)", eq("P5_22_2", 1), en("P5_22_2", [1, 2])),
            ("Condiciones laborales", "Pediría guardería como condición para trabajar", eq("P5_35_4", 1), en("P5_35_4", [1, 2])),
            ("Condiciones laborales", "Trabajó menos de 35 horas la semana pasada", F.col("P5_13") < 35, F.col("P5_13").between(0, 98)),
            ("Percepción", "Está satisfecha(o) con su tiempo libre", eq("P6_39_1", 1), en("P6_39_1", [1, 2])),
            ("Percepción", "Está satisfecha(o) con el tiempo de cuidado que dedica", eq("P6_39_2", 1), en("P6_39_2", [1, 2])),
            ("Percepción", "Está satisfecha(o) con la ayuda que recibe para cuidar", eq("P6_39_3", 1), en("P6_39_3", [1, 2])),
            ("Percepción", "Ha buscado guardería, casa del abuelo o asilo", eq("P6_42", 1), en("P6_42", [1, 2])),
        ]
        self.guardar_gold(binarios(c05, sc, items_cui, "FAC_CUI"), "cui05_binarios", ["tema", "indicador", "sexo_cuidador"])
        self.guardar_gold(
            promedios(c05, sc, [("Condiciones laborales", "Horas trabajadas la semana pasada", "horas_trabajadas", F.lit(True))], "FAC_CUI"),
            "cui05_promedios",
            ["indicador", "sexo_cuidador"]
        )

    def procesar_todo(self):
        """Ejecuta la generación completa de la capa Gold."""
        print("\n--- INICIANDO PROCESAMIENTO GOLD ENASIC 2022 ---")
        tsd = self._tabla_silver("residentes")
        cui = self._tabla_silver("cuidadores")
        nin = self._tabla_silver("ninos_0a5")
        c05 = self._tabla_silver("cuidadores_0a5")

        self.generar_demanda_cuidados(tsd)
        self.generar_recibe_cuidados(tsd)
        self.generar_brecha_recepcion_infantil_0a11(tsd)
        self.generar_prestaciones_laborales_cuidadores(cui)
        self.generar_participacion_cuidados(tsd)
        self.generar_participacion_economica_cuidadores(cui)
        self.generar_indicadores_ninos_0a5(nin, tsd)
        self.generar_indicadores_cuidadores_0a5(c05, cui)
        print("--- CAPA GOLD ENASIC COMPLETADA CON ÉXITO ---\n")
