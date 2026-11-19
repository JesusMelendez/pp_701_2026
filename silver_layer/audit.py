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


def distribucion(df: DataFrame, dims: list[str], cat: str, peso: str, indicador: str) -> DataFrame:
    """Reparto porcentual de una variable categórica dentro de cada apertura."""
    g = (
        df.filter(F.col(cat).isNotNull())
        .cube(*dims, cat)
        .agg(F.sum(peso).alias("poblacion"))
        .filter(F.col(cat).isNotNull())
    )
    w = Window.partitionBy(*dims) if dims else Window.partitionBy(F.lit(1))
    g = (
        g.withColumn("pct", F.round(F.col("poblacion") / F.sum("poblacion").over(w) * 100, 2))
        .withColumnRenamed(cat, "categoria")
        .withColumn("indicador", F.lit(indicador))
    )
    return rotular_total(g.select("indicador", *dims, "categoria", "poblacion", "pct"), dims)


def binarios(df: DataFrame, dims: list[str], items: list, peso: str) -> DataFrame:
    """items: (tema, indicador, condición 'sí', condición base). Porcentaje de 'sí' sobre base."""
    aggs = []
    for i, (_, _, flag, base) in enumerate(items):
        aggs += [
            suma_condicional(base & flag, peso).alias(f"s{i}"),
            suma_condicional(base, peso).alias(f"b{i}")
        ]
    wide = df.cube(*dims).agg(*aggs)
    partes = [
        wide.select(
            F.lit(t).alias("tema"),
            F.lit(ind).alias("indicador"),
            *dims,
            F.col(f"s{i}").alias("poblacion_si"),
            F.col(f"b{i}").alias("poblacion_base")
        )
        for i, (t, ind, _, _) in enumerate(items)
    ]
    out = reduce(DataFrame.unionByName, partes).withColumn(
        "pct_si", F.round(F.col("poblacion_si") / F.col("poblacion_base") * 100, 2)
    )
    return rotular_total(out, dims)


def promedios(df: DataFrame, dims: list[str], items: list, peso: str) -> DataFrame:
    """items: (tema, indicador, columna numérica, condición base). Promedio ponderado."""
    aggs = []
    for i, (_, _, val, base) in enumerate(items):
        aggs += [
            F.sum(F.when(base & F.col(val).isNotNull(), F.col(val) * F.col(peso))).alias(f"n{i}"),
            suma_condicional(base & F.col(val).isNotNull(), peso).alias(f"d{i}")
        ]
    wide = df.cube(*dims).agg(*aggs)
    partes = [
        wide.select(
            F.lit(t).alias("tema"),
            F.lit(ind).alias("indicador"),
            *dims,
            F.round(F.col(f"n{i}") / F.col(f"d{i}"), 2).alias("promedio"),
            F.col(f"d{i}").alias("poblacion_base")
        )
        for i, (t, ind, _, _) in enumerate(items)
    ]
    return rotular_total(reduce(DataFrame.unionByName, partes), dims)
