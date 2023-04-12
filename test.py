import subprocess
import sys
from pathlib import Path

import pysradb as sra

sra_project = "SRP396626"

sra_db = sra.SRAweb()
df = sra_db.sra_metadata(sra_project)
# df = sra_db.download(
#     sra_project,
#     out_dir="/root/pysradb_downloads",
#     skip_confirmation=True,
# )


sra_index = df.columns.get_loc("run_accession")
srx_index = df.columns.get_loc("experiment_accession")

output_dir = Path("/root/pysradb_downloads")
output_dir.mkdir()

for row in df.itertuples(index=False):
    srx_id = row[srx_index]
    sra_id = row[sra_index]

    output_dir = Path(f"/root/pysradb_downloads/{sra_project}/{srx_id}")
    output_dir.mkdir(parents=True, exist_ok=True)

    subprocess.run(
        [
            "fasterq-dump",
            f"{sra_id}",
            "--outdir",
            str(output_dir),
            "--split-files",
            "--include-technical",
            "--verbose",
            "--mem",
            "10000MB",
            "--threads",
            "8",
        ],
        check=True,
    )
