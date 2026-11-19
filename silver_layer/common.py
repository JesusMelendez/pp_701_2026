from functools import reduce
from pyspark.sql import DataFrame, Window, functions as F


def a_int(df: DataFrame, cols: list[str]) -> DataFrame:
    """Convierte columnas especificadas a tipo Integer."""
    for c in cols:
        if c in df.columns:
            df = df.withColumn(c, F.col(c).cast("int"))
    return df


def a_double(df: DataFrame, cols: list[str]) -> DataFrame:
    """Convierte columnas especificadas a tipo Double."""
    for c in cols:
        if c in df.columns:
            df = df.withColumn(c, F.col(c).cast("double"))
    return df


def etiqueta(col_name: str, mapping_dict: dict) -> F.Column:
    """Mapea valores de columna usando un diccionario Spark Map."""
    m = F.create_map(*[x for k, v in mapping_dict.items() for x in (F.lit(k), F.lit(v))])
    return m[F.col(col_name)]


def suma_condicional(cond, peso_col: str):
    """Suma ponderada condicional: SUM(CASE WHEN cond THEN peso ELSE 0 END)."""
    return F.sum(F.when(cond, F.col(peso_col)).otherwise(F.lit(0.0)))


def rotular_total(df: DataFrame, dims: list[str]) -> DataFrame:
    """Rellena nulos generados por cube/rollup con 'Total'."""
    for d in dims:
        df = df.withColumn(d, F.coalesce(F.col(d), F.lit("Total")))
    return df


def eq(col_name: str, val):
    return F.coalesce(F.col(col_name) == val, F.lit(False))


def en(col_name: str, vals: list):
    return F.coalesce(F.col(col_name).isin(*vals), F.lit(False))


def aplicar_protocolo_publicacion(df: DataFrame, col_muestra: str, umbral: int = 5, cols_valor: list[str] = None):
    """
    Protocolo de publicación (PP7_avance):
    - Umbral mínimo: 5 casos muestrales (10 si incluye discapacidad/enfermedad).
    - Celdas con conteo muestral < umbral se suprimen y marcan como 'No publicable por confidencialidad'.
    - Los datos suprimidos no se convierten en ceros.
    """
    cols_valor = cols_valor or []
    cond_confidencial = F.col(col_muestra) < umbral
    for c in cols_valor:
        if c in df.columns:
            df = df.withColumn(
                c,
                F.when(cond_confidencial, F.lit(None)).otherwise(F.col(c))
            )
    df = df.withColumn(
        "estatus_publicacion",
        F.when(cond_confidencial, F.lit("No publicable por confidencialidad")).otherwise(F.lit("Publicable"))
    )
    return df


def distribucion(df: DataFrame, dims: list[str], cat: str, peso: str, indicador: str) -> DataFrame:
    """Reparto porcentual de una variable categórica dentro de cada apertura."""
    g = (
        df.filter(F.col(cat).isNotNull())
        .cube(*dims, cat)
        .agg(
            F.sum(peso).alias("poblacion"),
            F.count(F.lit(1)).alias("casos_muestrales")
        )
        .filter(F.col(cat).isNotNull())
    )
    w = Window.partitionBy(*dims) if dims else Window.partitionBy(F.lit(1))
    g = (
        g.withColumn("pct", F.round(F.col("poblacion") / F.sum("poblacion").over(w) * 100, 2))
        .withColumnRenamed(cat, "categoria")
        .withColumn("indicador", F.lit(indicador))
    )
    g = rotular_total(g.select("indicador", *dims, "categoria", "poblacion", "pct", "casos_muestrales"), dims)
    return aplicar_protocolo_publicacion(g, "casos_muestrales", umbral=5, cols_valor=["poblacion", "pct"])


def binarios(df: DataFrame, dims: list[str], items: list, peso: str) -> DataFrame:
    """items: (tema, indicador, condición 'sí', condición base). Porcentaje de 'sí' sobre base."""
    aggs = []
    for i, (_, _, flag, base) in enumerate(items):
        aggs += [
            suma_condicional(base & flag, peso).alias(f"s{i}"),
            suma_condicional(base, peso).alias(f"b{i}"),
            F.sum(F.when(base, F.lit(1)).otherwise(F.lit(0))).alias(f"n_muestra_{i}")
        ]
    wide = df.cube(*dims).agg(*aggs)
    partes = [
        wide.select(
            F.lit(t).alias("tema"),
            F.lit(ind).alias("indicador"),
            *dims,
            F.col(f"s{i}").alias("poblacion_si"),
            F.col(f"b{i}").alias("poblacion_base"),
            F.col(f"n_muestra_{i}").alias("casos_muestrales")
        )
        for i, (t, ind, _, _) in enumerate(items)
    ]
    out = reduce(DataFrame.unionByName, partes).withColumn(
        "pct_si", F.round(F.col("poblacion_si") / F.col("poblacion_base") * 100, 2)
    )
    out = rotular_total(out, dims)
    return aplicar_protocolo_publicacion(out, "casos_muestrales", umbral=5, cols_valor=["poblacion_si", "poblacion_base", "pct_si"])


def promedios(df: DataFrame, dims: list[str], items: list, peso: str) -> DataFrame:
    """items: (tema, indicador, columna numérica, condición base). Promedio ponderado."""
    aggs = []
    for i, (_, _, val, base) in enumerate(items):
        aggs += [
            F.sum(F.when(base & F.col(val).isNotNull(), F.col(val) * F.col(peso))).alias(f"n{i}"),
            suma_condicional(base & F.col(val).isNotNull(), peso).alias(f"d{i}"),
            F.sum(F.when(base & F.col(val).isNotNull(), F.lit(1)).otherwise(F.lit(0))).alias(f"n_muestra_{i}")
        ]
    wide = df.cube(*dims).agg(*aggs)
    partes = [
        wide.select(
            F.lit(t).alias("tema"),
            F.lit(ind).alias("indicador"),
            *dims,
            F.round(F.col(f"n{i}") / F.col(f"d{i}"), 2).alias("promedio"),
            F.col(f"d{i}").alias("poblacion_base"),
            F.col(f"n_muestra_{i}").alias("casos_muestrales")
        )
        for i, (t, ind, _, _) in enumerate(items)
    ]
    res = rotular_total(reduce(DataFrame.unionByName, partes), dims)
    return aplicar_protocolo_publicacion(res, "casos_muestrales", umbral=5, cols_valor=["promedio", "poblacion_base"])


ENASIC_TABLAS = {
    "vivienda": "bronze_enasic_2022_tvivienda",
    "hogar": "bronze_enasic_2022_thogar",
    "hogar_unipersonal": "bronze_enasic_2022_thog_unip",
    "personas": "bronze_enasic_2022_tper_ele",
    "residentes": "bronze_enasic_2022_tcsdempo",
    "cuidadores": "bronze_enasic_2022_tpob_cui",
}

ENUT_TABLAS = {
    "vivienda": "bronze_enut_2024_tvivienda",
    "hogar": "bronze_enut_2024_thogar",
    "modulo": "bronze_enut_2024_tmodulo",
    "residentes": "bronze_enut_2024_tsdem",
    "var_crea": "bronze_enut_2024_tvar_crea",
}


class SilverBase:
    """Clase base para procesadores de la capa Silver."""

    def __init__(self, observatorio, tablas: dict[str, str], fuente: str):
        self.spark = observatorio.spark
        self.database = observatorio.base_datos
        self.base_path = observatorio.base_path
        self.fuente = fuente
        self.tablas = tablas
        self.datos = {}

    def cargar_bronze(self):
        """Carga las tablas Bronze registradas en Hive."""
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
    def exigir_columnas(df: DataFrame, columnas: list[str], tabla: str):
        """Comprueba que existan las columnas necesarias."""
        faltantes = [columna for columna in columnas if columna not in df.columns]
        if faltantes:
            raise ValueError(f"Faltan columnas en {tabla}: {faltantes}")

    def guardar_silver(self, df: DataFrame, nombre: str):
        """Escribe los resultados Silver en Parquet y Hive."""
        tabla = f"silver_{self.fuente}_{nombre}"
        ruta = f"{self.base_path}/silver/{tabla}"

        (
            df.write
            .mode("overwrite")
            .format("parquet")
            .option("path", ruta)
            .saveAsTable(f"{self.database}.{tabla}")
        )
        print(f"Silver generado: {tabla}")
        return df
