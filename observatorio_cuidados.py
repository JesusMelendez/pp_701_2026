#%%

import sys
import urllib.request
import zipfile
from pathlib import Path
sys.path.insert(0, "/usr/local/spark/python")
sys.path.insert(0, "/usr/local/spark/python/lib/py4j-0.10.9.7-src.zip")

from pyspark.sql import SparkSession

class ObservatorioCuidados:
    """Pipeline del Observatorio de Cuidados."""
    def __init__(self):
        self.spark = self._crear_sesion()
        self.base_datos = "cuidados_observatorio"
        self.bucket = "lakehouse"
        self.base_path = f"s3a://{self.bucket}/{self.base_datos}.db"
        self.fuentes = {
            "enut_2024": "https://www.inegi.org.mx/contenidos/programas/enut/2024/microdatos/enut_2024_bd_csv.zip",
            "enasic_2022": "https://www.inegi.org.mx/contenidos/programas/enasic/2022/microdatos/enasic_2022_bd_csv.zip",
        }
        self.directorio_datasets = Path("/home/joyvan/work/cuidados_observatorio/datasets")

    def _crear_sesion(self) ->SparkSession:
        """Creamos y retornamos la sesión Spark configurada"""
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

    def inicializar_base_datos(self):
        self.spark.sql("CREATE DATABASE IF NOT EXISTS {self.base_datos} LOCATION '{self.base_path}'")
        self.spark.sql("USE {self.base_datos}")

    def descargar_fuente(self, nombre_fuente: str, url:str):
        """Descarga y descomprime una fuente del inegi"""
        carpeta = self.directorio_datasets / nombre_fuente 
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

        return destino

    def ingestar_csvs(self, nombre_fuente: str, destino: Path):
        """Lee CSVs y los guarda como Parquet en la Capa bronce."""
        for ruta in sorted(destino.rglob("*.csv")):
                nombre = f"{nombre_fuente}_{ruta.stem.lower()}"
                df =self.spark.read.option("header", True).option("inferSchema", False).csv(str(ruta))
                (
                    df.write.mode("overwrite")
                    .format("parquet")
                    .option("path", f"{self.base_path}/bronze/{nombre}")
                    .saveAsTable(f"{self.base_datos}.bronze_{nombre}")
                )
                print(nombre, df.count())
         
# %%
