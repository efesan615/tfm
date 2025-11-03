 ┌────────────────────────┐
 │        Entrada          │
 │ Producto | Target | País│
 └──────────┬──────────────┘
            │
            ▼
 ┌────────────────────────┐
 │  1. Generación de queries │
 │ - Expansión semántica (sinónimos, usos, problemas) │
 │ - Detección de subreddits relevantes │
 └──────────┬──────────────┘
            │
            ▼
 ┌────────────────────────┐
 │  2. Extracción de datos (API Reddit) │
 │ - Búsqueda por palabras clave │
 │ - Recogida de posts y comentarios │
 │ - Filtrado y deduplicado │
 └──────────┬──────────────┘
            │
            ▼
 ┌────────────────────────┐
 │  3. Procesamiento y normalización │
 │ - Detección de idioma │
 │ - Limpieza de texto │
 │ - Extracción de entidades (producto, marca, atributos) │
 └──────────┬──────────────┘
            │
            ▼
 ┌────────────────────────┐
 │  4. Análisis de tendencias │
 │ - Métricas: volumen, aceleración, sentimiento │
 │ - Detección de temas (BERTopic / embeddings) │
 │ - Identificación de temas emergentes (novedad) │
 │ - Drivers de conversación: calidad, precio, envío, etc. │
 └──────────┬──────────────┘
            │
            ▼
 ┌────────────────────────┐
 │  5. Generación de insights (IA) │
 │ - Modelo LLM interpreta métricas │
 │ - Resume hallazgos y genera guías de acción │
 │ - Cita ejemplos y enlaces reales │
 └──────────┬──────────────┘
            │
            ▼
 ┌────────────────────────┐
 │  6. Output │
 │ - Informe estructurado (JSON/Markdown/PDF) │
 │ - Dashboard visual con gráficos y narrativa IA │
 └────────────────────────┘
