from observatorio_cuidados import ObservatorioCuidados

def main():
    obs = ObservatorioCuidados()
    obs.inicializar_base_datos()
    for nombre, url in obs.fuentes.items():
        destino = obs.descargar_fuente(nombre, url)
        obs.ingestar_csvs(nombre, destino)

    obs.spark.stop()

if __name__ == "__main__":
    main()

