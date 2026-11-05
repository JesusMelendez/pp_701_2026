#%%

import sys
sys.path.insert(0, "/usr/local/spark/python")
sys.path.insert(0, "/usr/local/spark/python/lib/py4j-0.10.9.7-src.zip")

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("cuidados_observatorio")
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "admin")
    .config("spark.hadoop.fs.s3a.secret.key", "password123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083")
    .enableHiveSupport()
    .getOrCreate()
)

spark.sql("CREATE DATABASE IF NOT EXISTS cuidados_observatorio LOCATION 's3a://lakehouse/cuidados_observatorio.db'")
spark.sql("USE cuidados_observatorio")
#%%
import urllib.request
import zipfile
from pathlib import Path

base = Path("/home/joyvan/work/cuidados_observatorio/datasets")
fuentes = {
    "enut_2024": "https://www.inegi.org.mx/contenidos/programas/enut/2024/microdatos/enut_2024_bd_csv.zip",
    "enasic_2022": "https://www.inegi.org.mx/contenidos/programas/enasic/2022/microdatos/enasic_2022_bd_csv.zip",
}

for nombre_fuente, url in fuentes.items():
    carpeta = base / nombre_fuente
    carpeta.mkdir(parents=True, exist_ok=True)
    zip_path = carpeta / f"{nombre_fuente}.zip"
    if not zip_path.exists():
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=300) as respuesta, open(zip_path, "wb") as salida:
            while True:
                bloque = respuesta.read(1024 * 1024)
                if not bloque:
                    break
                salida.write(bloque)
        print("descargado", zip_path.name)

    destino = carpeta / "csv"
    destino.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(destino)
    print("descomprimido", zip_path.name)

    for ruta in sorted(destino.rglob("*.csv")):
        nombre = f"{nombre_fuente}_{ruta.stem.lower()}"
        df = spark.read.option("header", True).option("inferSchema", False).csv(str(ruta))
        (
            df.write.mode("overwrite")
            .format("parquet")
            .option("path", f"s3a://lakehouse/cuidados_observatorio/bronze/{nombre}")
            .saveAsTable(f"cuidados_observatorio.bronze_{nombre}")
        )
        print(nombre, df.count())
