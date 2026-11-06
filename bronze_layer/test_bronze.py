from bronze_layer.bronze import BronzeLayer

bronze = BronzeLayer(
    staging_dir="datasets"
)

archivos = [
    ("datasets/enut_2024/enut_2024.zip", "ENUT 2024"),
    ("datasets/enasic_2022/enasic_2022.zip", "ENASIC 2022")
]

manifiesto = bronze.generar_manifiesto(archivos)

print(f"Manifiesto generado: {manifiesto}")