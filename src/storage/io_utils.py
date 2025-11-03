from typing import Optional, Dict, List
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

DEFAULT_PARQUET_OPTS = dict(
    compression="zstd",
    compression_level=7,
    use_dictionary=True,
    data_page_size=128 * 1024,  # ~128 KB
)

def pandas_to_table(df: pd.DataFrame, schema: Optional[pa.Schema] = None) -> pa.Table:
    """
    Convierte un DataFrame a pyarrow.Table preservando tipos compactos.
    Si pasas 'schema' garantizas consistencia entre archivos.
    """
    if schema is not None:
        # intenta ajustar tipos a schema
        table = pa.Table.from_pandas(df, preserve_index=False, schema=schema, safe=False)
    else:
        table = pa.Table.from_pandas(df, preserve_index=False)
    return table

def write_parquet(path: str, table: pa.Table, parquet_opts: Optional[Dict] = None) -> None:
    opts = {**DEFAULT_PARQUET_OPTS, **(parquet_opts or {})}
    pq.write_table(table, path, **opts)

def enforce_dtypes(df: pd.DataFrame, dtype_map: Dict[str, str]) -> pd.DataFrame:
    """
    Asegura tipos consistentes antes de escribir (evita 'object' inesperados).
    dtype_map: {"col": "int32"|"float32"|"category"|...}
    """
    out = df.copy()
    for col, dt in dtype_map.items():
        if col in out.columns:
            if dt == "category":
                out[col] = out[col].astype("category")
            else:
                out[col] = pd.to_numeric(out[col], errors="coerce") if "int" in dt or "float" in dt else out[col].astype(dt)
    return out
