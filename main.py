from observatorio_cuidados import ObservatorioCuidados
from pyspark.sql import SparkSession
from bronze_layer.audit import BronzeAuditor

def main():

    obs = ObservatorioCuidados()
    
    try:
        obs.inicializar_base_datos()

        #Provisional
        obs.spark.sql("""
            SHOW TABLES IN cuidados_observatorio
        """).show(truncate=False)
        for nombre in obs.fuentes:
            destino = (
                obs.directorio_datasets
                / nombre
                /"csv"
            )

            if not destino.is_dir():
                raise FileNotFoundError(
                    f"No existe el directorio: {destino}"
                )
            obs.ingestar_csvs(nombre, destino)

        auditoria = BronzeAuditor(obs)
        resultados = auditoria.auditar_bronze()

    finally:
        obs.spark.stop()

if __name__ == "__main__":
    main()

