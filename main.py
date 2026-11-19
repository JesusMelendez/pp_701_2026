from observatorio_cuidados import ObservatorioCuidados
from bronze_layer.audit import BronzeAuditor
from silver_layer.enasic_schema import EnasicSchema
from silver_layer.enasic import EnasicSilver
from silver_layer.enut import EnutSilver
from gold_layer.gold import EnasicGold
from gold_layer.enut_gold import EnutGold

def main():

    obs = ObservatorioCuidados()
    
    try:
        obs.inicializar_base_datos()

        # Ingesta Bronze
        for nombre in obs.fuentes:
            destino = (
                obs.directorio_datasets
                / nombre
                / "csv"
            )

            if not destino.is_dir():
                raise FileNotFoundError(
                    f"No existe el directorio: {destino}"
                )
            obs.ingestar_csvs(nombre, destino)
        
        # Auditoría Bronze
        auditoria = BronzeAuditor(obs)
        resultados = auditoria.auditar_bronze()

        # Inspección del esquema ENASIC
        esquema = EnasicSchema(obs)
        esquema.cargar_bronze()
        esquema.mostrar_arbol()

        # Capa Silver ENASIC 2022
        enasic_silver = EnasicSilver(obs)
        enasic_silver.procesar_todo()

        # Capa Silver ENUT 2024
        enut_silver = EnutSilver(obs)
        enut_silver.procesar_todo()

        # Capa Gold ENASIC 2022
        enasic_gold = EnasicGold(obs)
        enasic_gold.procesar_todo()

        # Capa Gold ENUT 2024
        enut_gold = EnutGold(obs)
        enut_gold.procesar_todo()

    finally:
        obs.spark.stop()

if __name__ == "__main__":
    main()
