import os
import time
import glob
from dataclasses import dataclass
from typing import Optional, Dict, List
import pandas as pd
import duckdb
import pyarrow as pa

from .io_utils import pandas_to_table, write_parquet

# -----------------------------------------------------------
# ParquetStorage: guarda DataFrames ultra-compactos en Parquet (ZSTD)
# -----------------------------------------------------------

@dataclass
class ParquetStorage:
    base_dir: str  # p.ej. "data/curated/reddit"
    dataset: str   # p.ej. "posts" | "comments"
    schema: Optional[pa.Schema] = None  # opcional: schema consistente
    ensure_dirs: bool = True

    def __post_init__(self):
        self.dataset_dir = os.path.join(self.base_dir, self.dataset)
        if self.ensure_dirs:
            os.makedirs(self.dataset_dir, exist_ok=True)

    def _timestamp(self) -> str:
        return time.strftime("%Y%m%d_%H%M%S", time.gmtime())

    def _file_path(self, suffix: Optional[str] = None) -> str:
        stamp = self._timestamp()
        name = f"{self.dataset}_{stamp}"
        if suffix:
            name += f"_{suffix}"
        return os.path.join(self.dataset_dir, f"{name}.parquet")

    @staticmethod
    def dedup(df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return df
        if "id" in df.columns:
            return df.drop_duplicates(subset=["id"]).reset_index(drop=True)
        return df

    def save_df(self, df: pd.DataFrame, suffix: Optional[str] = None) -> str:
        """
        Escribe un DataFrame como un único Parquet comprimido (ZSTD).
        No duplica datos en DB; solo disco. Devuelve la ruta escrita.
        """
        df = self.dedup(df)
        if df is None or df.empty:
            raise ValueError("DataFrame vacío; nada que guardar.")
        table = pandas_to_table(df, schema=self.schema)
        path = self._file_path(suffix)
        write_parquet(path, table)
        return path

    def latest_paths(self, n: int = 5) -> List[str]:
        files = sorted(glob.glob(os.path.join(self.dataset_dir, f"{self.dataset}_*.parquet")))
        return files[-n:]

    def scan_paths(self, pattern: str = "*.parquet") -> List[str]:
        return sorted(glob.glob(os.path.join(self.dataset_dir, pattern)))

# -----------------------------------------------------------
# DuckDBIndex: vistas que consultan Parquet sin importarlos
# -----------------------------------------------------------

@dataclass
class DuckDBIndex:
    db_path: str                 # p.ej. "data/reddit.duckdb"
    base_dir: str                # p.ej. "data/curated/reddit"
    read_only: bool = False

    def connect(self):
        return duckdb.connect(self.db_path, read_only=self.read_only)

    def create_view_for_dataset(self, dataset: str, view_name: Optional[str] = None) -> None:
        """
        Crea/actualiza una VIEW que lee todos los parquet de un dataset
        usando parquet_scan(). No duplica datos.
        """
        view = view_name or f"vw_{dataset}"
        pattern = os.path.join(self.base_dir, dataset, f"{dataset}_*.parquet").replace("\\","/")
        sql = f"CREATE OR REPLACE VIEW {view} AS SELECT * FROM parquet_scan('{pattern}');"
        with self.connect() as con:
            con.execute(sql)

    def query(self, sql: str) -> pd.DataFrame:
        with self.connect() as con:
            return con.execute(sql).df()
