from bronze import BronzeLayer

bronze = BronzeLayer(
    staging_dir="datasets"
)

archivos = [
    # ENASIC 2022: archivo original
    ("datasets/enasic_2022/enasic_2022.zip", "ENASIC 2022"),

    # ENASIC 2022: microdatos
    ("datasets/enasic_2022/csv/TCSDEMPO.csv", "ENASIC 2022"),
    ("datasets/enasic_2022/csv/THOGAR.csv", "ENASIC 2022"),
    ("datasets/enasic_2022/csv/THOG_UNIP.csv", "ENASIC 2022"),
    ("datasets/enasic_2022/csv/TPER_ELE.csv", "ENASIC 2022"),
    ("datasets/enasic_2022/csv/TPOB_CUI.csv", "ENASIC 2022"),
    ("datasets/enasic_2022/csv/TVIVIENDA.csv", "ENASIC 2022"),

    # ENUT 2024: archivo original
    ("datasets/enut_2024/enut_2024.zip", "ENUT 2024"),

    # ENUT 2024: microdatos
    ("datasets/enut_2024/csv/thogar.csv", "ENUT 2024"),
    ("datasets/enut_2024/csv/tmodulo.csv", "ENUT 2024"),
    ("datasets/enut_2024/csv/tsdem.csv", "ENUT 2024"),
    ("datasets/enut_2024/csv/tvar_crea.csv", "ENUT 2024"),
    ("datasets/enut_2024/csv/tvivienda.csv", "ENUT 2024"),

    # ENUT 2024: catálogo
    ("datasets/enut_2024/csv/enut_2024_cat.xlsx", "Catálogo ENUT 2024")
]

manifiesto = bronze.generar_manifiesto(archivos)

print(f"Manifiesto generado: {manifiesto}")