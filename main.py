from observatorio_cuidados import ObservatorioCuidados
from pyspark.sql import SparkSession
from bronze_layer.audit import BronzeAuditor
from silver_layer.enasic_schema import EnasicSchema
from silver_layer.enasic import EnasicSilver

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
        
        # Auditoríaa Bronze
        auditoria = BronzeAuditor(obs)
        resultados = auditoria.auditar_bronze()

        #Inspección del esquema ENASIC
        esquema = EnasicSchema(obs)
        esquema.cargar_bronze()
        esquema.mostrar_arbol()
        #Preparación de Silver ENASIC
        enasic = EnasicSilver(obs)
        enasic.cargar_bronze()
        enasic.transformar_viviendas()
        enasic.transformar_hogares()
        
        enasic.datos["residentes"].printSchema()
        enasic.datos["residentes"].show(5, truncate=False)

    finally:
        obs.spark.stop()

if __name__ == "__main__":
    main()

