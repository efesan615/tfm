# TFM – Análisis de tendencias y opiniones en Reddit con Inteligencia Artificial

## Descripción

Este proyecto implementa un pipeline completo que, a partir de un **input** (una frase o concepto), obtiene publicaciones de Reddit, las analiza con modelos de inteligencia artificial entrenados dentro del propio proyecto y genera **resúmenes, métricas e insights** sobre las opiniones y tendencias detectadas.

La IA no se basa únicamente en modelos preexistentes: se entrena con datos propios y supervisión manual para aprender a clasificar **sentimiento**, **postura (stance)** y **tópicos**.

---

## Flujo general

1. **Entrada del usuario:** una frase o concepto (por ejemplo: “zapatillas de running de carbono”).
2. **Generación de consultas:** se crean variaciones semánticas y se identifican subreddits relevantes.
3. **Extracción de datos:** se descargan posts de Reddit usando su API pública.
4. **Normalización:** limpieza de texto, detección de idioma y eliminación de duplicados.
5. **Procesamiento IA:**
   - Creación de *embeddings* con Sentence-Transformers.
   - Agrupación en tópicos (clustering).
   - Clasificación de sentimiento (positivo, neutro o negativo).
   - Clasificación de postura (a favor, en contra o neutral respecto al concepto).
   - Generación de resúmenes y comparación de posturas.
6. **Cálculo de métricas:** volumen de conversación, velocidad, sentimiento medio, controversia, entidades más mencionadas y temas principales.
7. **Síntesis final:** generación de un informe con conclusiones y ejemplos reales.

---

## Entrenamiento de los modelos

El proyecto incluye su propio sistema de entrenamiento y evaluación para los modelos de IA.

### Tareas
- **Sentiment:** analiza la polaridad emocional (de negativo a positivo).
- **Stance:** determina si la publicación está a favor, en contra o es neutral.
- **Topic labeling:** etiqueta los clústeres temáticos detectados por el modelo.

### Proceso
1. Construcción de un dataset etiquetado a partir de los datos obtenidos (formato `LabeledDoc`).
2. Etiquetado automático inicial y revisión manual.
3. Entrenamiento supervisado con modelos tipo BERT.
4. Evaluación del rendimiento (F1, precisión, recall).
5. Registro de la versión del modelo en `configs/models.yaml`.

Los modelos entrenados se utilizan después en la etapa de inferencia del pipeline.

---

## Resultados esperados

- Identificación automática de tendencias y opiniones sobre el concepto de entrada.
- Modelos entrenados y versionados dentro del proyecto.
- Métricas cuantitativas (volumen, sentimiento, controversia, novedad).
- Informes interpretables con ejemplos reales de publicaciones.

## Autor

**Fernando Santamaría Fernández**  
Máster Universitario en Inteligencia Artificial  
**Universidad Internacional de Valencia (VIU)**  
Trabajo Fin de Máster – Curso 2024 / 2025  

Proyecto desarrollado bajo la línea de investigación:  
**Análisis de tendencias y opiniones mediante técnicas de Inteligencia Artificial aplicadas a datos de redes sociales.**



