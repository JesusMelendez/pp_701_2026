class BronzeAuditor:
    def __init__(self, observatorio):
        self.observatorio = observatorio

    def auditar_bronze(self):
        obs = self.observatorio
        """Audita la ingesta Bronze y compara origen con destino."""

        print("\n========== AUDITORÍA BRONZE ==========\n")

        # 1. Recuperar tablas registradas
        tablas = obs.spark.sql("""
            SHOW TABLES IN cuidados_observatorio
        """).collect()

        nombres_tablas = {
            tabla.tableName for tabla in tablas
        }

        print(f"Tablas registradas: {len(nombres_tablas)}")

        # 2. Recorrer archivos originales y comparar con destino
        resultados = []

        for nombre_fuente in obs.fuentes:

            directorio = (
                obs.directorio_datasets
                / nombre_fuente
                / "csv"
            )

            for ruta in sorted(directorio.glob("*.csv")):

                nombre_tabla = (
                    f"bronze_{nombre_fuente}_{ruta.stem.lower()}"
                )

                print(f"\n--- {nombre_tabla} ---")

                if nombre_tabla not in nombres_tablas:
                    print("ERROR: tabla no registrada")
                    resultados.append({
                        "tabla": nombre_tabla,
                        "estado": "TABLA AUSENTE"
                    })
                    continue

                # Leer archivo CSV original
                origen = (
                    obs.spark.read
                    .option("header", True)
                    .option("inferSchema", False)
                    .csv(str(ruta))
                )

                # Leer tabla Bronze
                destino = obs.spark.table(
                    f"cuidados_observatorio.{nombre_tabla}"
                )

                # Conteos
                filas_origen = origen.count()
                filas_destino = destino.count()

                # Estructura
                columnas_origen = origen.columns
                columnas_destino = destino.columns

                estructura_correcta = (
                    columnas_origen == columnas_destino
                )

                conteo_correcto = (
                    filas_origen == filas_destino
                )

                # Información de almacenamiento
                descripcion = obs.spark.sql(
                    f"""
                    DESCRIBE FORMATTED
                    cuidados_observatorio.{nombre_tabla}
                    """
                ).collect()

                metadatos = {
                    fila.col_name.strip(): fila.data_type.strip()
                    for fila in descripcion
                    if fila.col_name and fila.data_type
                }

                proveedor = metadatos.get("Provider", "No identificado")
                ubicacion = metadatos.get("Location", "No identificada")

                # Verificar existencia física en MinIO
                path = obs.spark._jvm.org.apache.hadoop.fs.Path(
                    ubicacion
                )

                filesystem = path.getFileSystem(
                    obs.spark._jsc.hadoopConfiguration()
                )

                existe_fisicamente = filesystem.exists(path)

                # Resultados
                print(f"Filas originales: {filas_origen}")
                print(f"Filas Bronze: {filas_destino}")
                print(f"Columnas originales: {len(columnas_origen)}")
                print(f"Columnas Bronze: {len(columnas_destino)}")
                print(f"Estructura coincidente: {estructura_correcta}")
                print(f"Conteo coincidente: {conteo_correcto}")
                print(f"Proveedor: {proveedor}")
                print(f"Ubicación: {ubicacion}")
                print(f"Ruta física existente: {existe_fisicamente}")

                estado = (
                    "CORRECTO"
                    if (
                        conteo_correcto
                        and estructura_correcta
                        and proveedor.lower() == "parquet"
                        and existe_fisicamente
                    )
                    else "REVISAR"
                )

                resultados.append({
                    "tabla": nombre_tabla,
                    "filas_origen": filas_origen,
                    "filas_bronze": filas_destino,
                    "columnas_origen": len(columnas_origen),
                    "columnas_bronze": len(columnas_destino),
                    "estructura_correcta": estructura_correcta,
                    "conteo_correcto": conteo_correcto,
                    "proveedor": proveedor,
                    "ubicacion": ubicacion,
                    "existe_fisicamente": existe_fisicamente,
                    "estado": estado
                })

        # 3. Resumen final
        print("\n========== RESUMEN ==========\n")

        for resultado in resultados:
            print(
                resultado["tabla"],
                "->",
                resultado["estado"]
            )

        correctas = sum(
            r["estado"] == "CORRECTO"
            for r in resultados
        )

        print(f"\nTablas auditadas: {len(resultados)}")
        print(f"Correctas: {correctas}")
        print(f"Revisar: {len(resultados) - correctas}")

        return resultados


