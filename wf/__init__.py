import functools
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

import pandas as pd
import pysradb as sra

from latch import large_task, map_task, small_task, workflow
from latch.registry.table import Table
from latch.resources.launch_plan import LaunchPlan
from latch.types import (
    Fork,
    ForkBranch,
    LatchAuthor,
    LatchDir,
    LatchMetadata,
    LatchParameter,
    Params,
)


@dataclass
class DownloadData:
    sra_id: str
    output_location: LatchDir


@small_task
def generate_downloads(
    download_type_fork: str,
    sra_project: Optional[str],
    sra_ids: Optional[List[str]],
    output_location: LatchDir,
) -> List[DownloadData]:
    if download_type_fork == "sra_ids" and sra_ids is not None:
        return [DownloadData(sra_id, output_location) for sra_id in sra_ids]

    if sra_project is None:
        raise ValueError("Must specify either a set of SRA IDs or an SRP ID.")

    outputs: List[DownloadData] = []

    sra_db = sra.SRAweb()
    df: pd.DataFrame = sra_db.sra_metadata(sra_project)

    sra_index = df.columns.get_loc("run_accession")
    srx_index = df.columns.get_loc("experiment_accession")

    for row in df.itertuples(index=False):
        srx_id = row[srx_index]
        sra_id = row[sra_index]

        outputs.append(
            DownloadData(
                sra_id,
                LatchDir(f"{output_location.remote_directory}{sra_project}/{srx_id}"),
            )
        )

    return outputs


@large_task
def download(data: DownloadData) -> LatchDir:
    output_dir = Path("downloaded")
    output_dir.mkdir(exist_ok=True)
    subprocess.run(
        [
            "fasterq-dump",
            f"{data.sra_id}",
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
    return LatchDir(output_dir, data.output_location.remote_directory)


@small_task
def write_to_registry(
    download_type_fork: str,
    sra_project: Optional[str],
    output_location: LatchDir,
    metadata_table_id: Optional[str],
    barrier: List[LatchDir],  # hack to make sure this runs after the map task is done
) -> LatchDir:
    if (
        metadata_table_id is None
        or download_type_fork == "sra_ids"
        or sra_project is None
    ):
        return output_location

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

    for row in df.itertuples(index=False):
        with t.update() as u:
            srx_id = row[srx_index]
            sra_id = row[sra_index]

            u.upsert_record(
                sra_id,
                Downloaded=LatchDir(
                    f"{output_location.remote_directory}{sra_project}/{srx_id}"
                ),
                **{column: str(row[idx]) for column, idx in columns_to_idxs.items()},
            )

    return output_location


"""The metadata included here will be injected into your interface."""
metadata = LatchMetadata(
    display_name="SRA FASTQ Downloader",
    author=LatchAuthor(
        name="Aidan Abdulali",
    ),
    license="MIT",
    parameters={
        "download_type_fork": LatchParameter(
            display_name="Download Type",
            description="Either download a whole SRA Project or a set of Runs",
        ),
        "sra_ids": LatchParameter(
            display_name="Run IDs",
            description="A list of SRA Run IDs to download",
            batch_table_column=True,
        ),
        "sra_project": LatchParameter(
            display_name="Project ID",
            description="An SRA Project ID to download",
            batch_table_column=True,
        ),
        "output_location": LatchParameter(
            display_name="Output Location",
            description="Location to download the SRA files to",
            batch_table_column=True,
            output=True,
        ),
        "metadata_table_id": LatchParameter(
            display_name="Output Metadata Table",
            description="Optional Table to store Project Metadata",
            batch_table_column=True,
        ),
    },
    tags=[],
    flow=[
        Fork(
            fork="download_type_fork",
            display_name="Download Type",
            sra_ids=ForkBranch(
                "Specify Run IDs",
                Params("sra_ids"),
                _tmp_unwrap_optionals=["sra_ids"],
            ),
            sra_project=ForkBranch(
                "Specify Project ID",
                Params("sra_project", "metadata_table_id"),
                _tmp_unwrap_optionals=["sra_project"],
            ),
        ),
        Params("output_location"),
    ],
)


@workflow(metadata)
def sra_fetcher(
    download_type_fork: str,
    sra_project: Optional[str],
    sra_ids: Optional[List[str]],
    output_location: LatchDir = LatchDir("latch:///SRA FASTQs/"),
    metadata_table_id: Optional[str] = None,
) -> LatchDir:
    """Download .sra file from accession number, unpack it into FASTQs, gunzip them, and deposit them into Latch Data.

    # SRA FASTQ Fetcher
    """
    data = generate_downloads(
        download_type_fork=download_type_fork,
        sra_project=sra_project,
        sra_ids=sra_ids,
        output_location=output_location,
    )

    barrier = map_task(download)(data=data)

    return write_to_registry(
        download_type_fork=download_type_fork,
        sra_project=sra_project,
        output_location=output_location,
        metadata_table_id=metadata_table_id,
        barrier=barrier,
    )


"""
Add test data with a LaunchPlan. Provide default values in a dictionary with
the parameter names as the keys. These default values will be available under
the 'Test Data' dropdown at console.latch.bio.
"""
LaunchPlan(
    sra_fetcher,
    "SRR8984431",
    {
        "download_type_fork": "sra_ids",
        "sra_project": None,
        "sra_ids": ["SRR8984431"],
    },
)
