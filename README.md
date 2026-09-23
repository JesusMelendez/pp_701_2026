# Observatorio de Datos - Economía del Cuidado

Este proyecto implementa un Observatorio de Datos para analizar necesidades y brechas en la economía del cuidado[cite: 1]. Utiliza una arquitectura Medallion procesada con PySpark para garantizar la calidad y disponibilidad de la información[cite: 1].

## Estructura del Proyecto

El repositorio está organizado siguiendo un flujo de procesamiento por capas[cite: 2]:

*   **`data/`**: Almacenamiento de los conjuntos de datos en sus diferentes etapas[cite: 2].
    *   `bronze/`: Datos crudos e ingesta inicial[cite: 2].
    *   `silver/`: Datos limpios, anonimizados y estandarizados[cite: 2].
    *   `gold/`: Datos agregados e indicadores (KPIs) listos para consumo[cite: 2].
*   **`db/`**: Scripts y configuraciones relacionadas con la base de datos[cite: 2].
*   **`docs/`**: Documentación del proyecto (protocolos de privacidad, diccionarios, manuales)[cite: 2].
*   **`notebooks\EDA/`**: Cuadernos de Jupyter para el Análisis Exploratorio de Datos[cite: 2].
    *   `eda_enasic.ipynb`: Exploración de datos (ej. Encuesta ENASIC)[cite: 2].
*   **`pipelines/`**: Canalizaciones de código para la transformación de datos[cite: 2].
    *   `bronze/`: Scripts de extracción y carga inicial[cite: 2].
    *   `silver/`: Scripts de limpieza y depuración[cite: 2].
    *   `gold/`: Scripts para la construcción del modelo de negocio e indicadores[cite: 2].
    *   `sync/`: Tareas de orquestación y sincronización de datos[cite: 2].
*   **`tests/`**: Pruebas de calidad y validación del código[cite: 2].
    *   `eda_denue.ipynb`: Cuaderno de análisis o validaciones adicionales[cite: 2].