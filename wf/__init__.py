import subprocess
from multiprocessing import Pool
from pathlib import Path
from typing import List, Optional

import pysradb as sra

from latch import medium_task, workflow
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


def generate_fastqs_for_sra(sra_id: str) -> None:
    print("Currently downloading: " + sra_id)
    prefetch = f"prefetch {sra_id} --max-size 10000GB"
    print("The command used is: " + prefetch)
    subprocess.run(prefetch, shell=True, check=True)

    print("Generating fastq for: " + sra_id)
    fastq_dump = (
        f"fasterq-dump {sra_id} --outdir /root/fastq/{sra_id} --split-files"
        " --include-technical --verbose --mem 10000MB --threads 8"
    )
    print("The command is: " + fastq_dump)
    subprocess.run(fastq_dump, shell=True, check=True)


def gzip_fastq(file: Path) -> None:
    pigz = f"pigz {str(file)}"
    subprocess.run(pigz, shell=True, check=True)


def download_sras(sra_list: List[str], output_location: LatchDir) -> LatchDir:
    with Pool() as p:
        p.map(generate_fastqs_for_sra, sra_list)

    print(
        "Finished downloading and converting all SRA files\nGunzipping result files..."
    )
    output_files = Path("/root/fastq").glob("**/*.fastq")
    with Pool() as p:
        p.map(gzip_fastq, output_files)

    return LatchDir("/root/fastq", output_location.remote_path)


@medium_task
def download(
    download_type_fork: str,
    sra_project: Optional[str],
    sra_ids: Optional[List[str]],
    output_location: LatchDir,
) -> LatchDir:
    if download_type_fork == "sra_ids":
        if sra_ids is None:
            raise ValueError("no SRA runs to download")
        return download_sras(sra_ids, output_location)

    if sra_project is None:
        raise ValueError("no SRA project to download")

    sra_db = sra.SRAweb()
    df = sra_db.sra_metadata(sra_project)

    sra_index = df.columns.get_loc("run_accession")
    srx_index = df.columns.get_loc("experiment_accession")

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

    return LatchDir(
        "/root/pysradb_downloads",
        output_location.remote_directory,
    )


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
                Params("sra_project"),
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
) -> LatchDir:
    """Download .sra file from accession number, unpack it into FASTQs, gunzip them, and deposit them into Latch Data.

    # SRA FASTQ Fetcher
    """
    return download(
        download_type_fork=download_type_fork,
        sra_project=sra_project,
        sra_ids=sra_ids,
        output_location=output_location,
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
