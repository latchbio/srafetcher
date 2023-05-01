from pathlib import Path

import pandas as pd
import pysradb as sra

from latch.registry.table import Table
from latch.types import LatchDir

sra_project = "SRP396626"
metadata_table_id = "358"

sra_db = sra.SRAweb()
df: pd.DataFrame = sra_db.sra_metadata(sra_project)

sra_index = df.columns.get_loc("run_accession")
srx_index = df.columns.get_loc("experiment_accession")

output_dir = Path("/root/pysradb_downloads")
output_dir.mkdir(exist_ok=True)


sra_db = sra.SRAweb()
df: pd.DataFrame = sra_db.sra_metadata(sra_project)

sra_index = df.columns.get_loc("run_accession")
srx_index = df.columns.get_loc("experiment_accession")

columns_to_idxs = {str(column): df.columns.get_loc(column) for column in df}

t = Table(metadata_table_id)

with t.update() as u:
    for column in df:
        u.upsert_column(str(column), str)
    u.upsert_column("Downloaded", LatchDir)


with t.update() as u:
    for row in df.itertuples(index=False):
        srx_id = row[srx_index]
        sra_id = row[sra_index]
        u.upsert_record(
            sra_id,
            Downloaded=LatchDir(f"latch:///SRA FASTQs/{sra_project}/{srx_id}"),
            **{column: str(row[idx]) for column, idx in columns_to_idxs.items()},
        )
