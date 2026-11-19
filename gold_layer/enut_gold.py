import os
from functools import reduce
from pyspark.sql import DataFrame, functions as F
from silver_layer.common import (
    suma_condicional,
    rotular_total,
    binarios,
    promedios,
    eq,
    en,
    aplicar_protocolo_publicacion,
)


class EnutGold:
    """
    Generación de indicadores agregados Gold para ENUT 2024:
    - Carga de quien cuida: horas dedicadas a cuidados (0 a 5 años, con/sin cuidados pasivos).
    - Desagregaciones solicitadas en PP7: sexo, grupo de edad, COND_AEE y entidad (CDMX / Edomex).
    - Prestaciones laborales de cuidadores en ENUT: vacaciones, servicio médico, AFORE, guardería.
    - Aplicación de umbral de 5 casos y supresión por confidencialidad.
    """

    def __init__(self, observatorio, export_dir: str = None):
        self.spark = observatorio.spark
        self.database = observatorio.base_datos
        self.base_path = observatorio.base_path
        self.fuente = "enut_2024"
        self.export_dir = export_dir or str(observatorio.directorio_datasets / "export" / self.fuente)
        os.makedirs(self.export_dir, exist_ok=True)

    def _tabla_silver(self, nombre: str) -> DataFrame:
        return self.spark.table(f"{self.database}.silver_{self.fuente}_{nombre}")

    def guardar_gold(self, df: DataFrame, nombre: str, orden: list[str] = None) -> DataFrame:
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

    def generar_horas_cuidado_0a5(self, df: DataFrame) -> DataFrame:
        """
        Carga de quien cuida: promedio y total de horas semanales dedicadas a niñas/niños de 0 a 5 años
        (con y sin cuidados pasivos), por sexo, edad y entidad (CDMX / EdoMex).
        """
        dims = ["sexo_desc", "grupo_edad", "entidad_desc"]
        cui_activos = df.filter(F.col("es_cuidador_0a5") == 1)

        items_horas = [
            ("Carga de cuidado 0 a 5 años", "Horas semanales dedicadas al cuidado (sin cuidados pasivos)",
             "CUID_INT_0A5_SIN_CP", F.col("CUID_INT_0A5_SIN_CP") > 0),
            ("Carga de cuidado 0 a 5 años", "Horas semanales dedicadas al cuidado (con cuidados pasivos)",
             "CUID_INT_0A5_CON_CP", F.col("CUID_INT_0A5_CON_CP") > 0),
        ]
        res = promedios(cui_activos, dims, items_horas, "FAC_PER")
        return self.guardar_gold(res, "carga_horas_cuidado_0a5", ["indicador"] + dims)

    def generar_horas_por_condicion_actividad(self, df: DataFrame) -> DataFrame:
        """
        Horas semanales de cuidado según condición de actividad económica (COND_AEE), sexo y entidad.
        """
        dims = ["cond_aee_desc", "sexo_desc", "entidad_desc"]
        cui_activos = df.filter(F.col("es_cuidador_0a5") == 1)

        items_horas = [
            ("Cuidado e inserción laboral", "Horas de cuidado 0 a 5 años por condición de actividad (sin CP)",
             "CUID_INT_0A5_SIN_CP", F.col("CUID_INT_0A5_SIN_CP") > 0),
            ("Cuidado e inserción laboral", "Horas de cuidado 0 a 5 años por condición de actividad (con CP)",
             "CUID_INT_0A5_CON_CP", F.col("CUID_INT_0A5_CON_CP") > 0),
        ]
        res = promedios(cui_activos, dims, items_horas, "FAC_PER")
        return self.guardar_gold(res, "carga_horas_por_actividad_economica", ["indicador"] + dims)

    def generar_prestaciones_laborales_cuidadores(self, df: DataFrame) -> DataFrame:
        """
        Prestaciones laborales de la población que cuida a niñas/niños de 0 a 5 años (vacaciones, AFORE, IMSS/ISSSTE, guardería).
        """
        dims = ["sexo_desc", "entidad_desc"]
        cui_ocupados = df.filter((F.col("es_cuidador_0a5") == 1) & (F.col("COND_AEE") == 1))

        items_prest = [
            ("Prestaciones laborales", "Vacaciones con goce de sueldo", eq("tiene_vacaciones", 1), en("tiene_vacaciones", [0, 1])),
            ("Prestaciones laborales", "Fondo para el retiro (AFORE / SAR)", eq("tiene_afore", 1), en("tiene_afore", [0, 1])),
            ("Prestaciones laborales", "Servicio médico (IMSS, ISSSTE u otro)", eq("tiene_servicio_medico", 1), en("tiene_servicio_medico", [0, 1])),
            ("Prestaciones laborales", "Guardería o estancia infantil", eq("tiene_guarderia", 1), en("tiene_guarderia", [0, 1])),
        ]
        res = binarios(cui_ocupados, dims, items_prest, "FAC_PER")
        return self.guardar_gold(res, "prestaciones_cuidadores_enut", ["indicador"] + dims)

    def procesar_todo(self):
        print("\n--- INICIANDO PROCESAMIENTO GOLD ENUT 2024 ---")
        df_cui = self._tabla_silver("cuidadores_tiempo")
        self.generar_horas_cuidado_0a5(df_cui)
        self.generar_horas_por_condicion_actividad(df_cui)
        self.generar_prestaciones_laborales_cuidadores(df_cui)
        print("--- CAPA GOLD ENUT COMPLETADA CON ÉXITO ---\n")
