"""
Assemble and sort some COVID reads...
"""

import subprocess
from multiprocessing import Pool
from pathlib import Path

from typing import List
from latch import medium_task, workflow
from latch.resources.launch_plan import LaunchPlan
from latch.types import LatchAuthor, LatchDir, LatchMetadata, LatchParameter
import gzip
import shutil


def generate_fastqs_for_sra(sra_id: str) -> None:
    print ("Currently downloading: " + sra_id)
    prefetch = f"prefetch {sra_id} --max-size 10000GB"
    print ("The command used is: " + prefetch)
    subprocess.run(prefetch, shell=True, check=True)

    print ("Generating fastq for: " + sra_id)
    fastq_dump = f"fasterq-dump {sra_id} --outdir /root/fastq/{sra_id} --split-files --include-technical --verbose --mem 10000MB --threads 8"
    print ("The command is: " + fastq_dump)
    subprocess.run(fastq_dump, shell=True, check=True)

def gzip_fastq(file: Path) -> None:

    pigz = f"pigz {str(file)}"
    subprocess.run(pigz, shell=True, check=True)

@medium_task
def download_sra(sra_list: List[str], output_location: LatchDir) -> LatchDir:

    with Pool(4) as p:
        p.map(generate_fastqs_for_sra, sra_list)
    
    print("Finished downloading and converting all SRA files\nGunzipping result files...")
    output_files = Path("/root/fastq").glob("**/*.fastq")
    with Pool(2) as p:
        p.map(gzip_fastq, output_files)

    return LatchDir("/root/fastq", output_location.remote_path)


"""The metadata included here will be injected into your interface."""
metadata = LatchMetadata(
    display_name="SRA FASTQ Downloader",
    author=LatchAuthor(
        name="Aidan Abdulali",
    ),
    license="MIT",
    parameters={
        "sra_list": LatchParameter(
            display_name="SRA Accession",
            description="List of SRA accessions to download",
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
)


@workflow(metadata)
def sra_fetcher(
    sra_list: List[str],
    output_location: LatchDir = LatchDir("latch:///SRA FASTQs/"),
) -> LatchDir:
    """Download .sra file from accession number, unpack it into FASTQs, gunzip them, and deposit them into Latch Data.

    # SRA FASTQ Fetcher
    """
    return download_sra(sra_list=sra_list, output_location=output_location)


"""
Add test data with a LaunchPlan. Provide default values in a dictionary with
the parameter names as the keys. These default values will be available under
the 'Test Data' dropdown at console.latch.bio.
"""
LaunchPlan(
    sra_fetcher,
    "Test Data",
    {
        "sra_list": ["SRR8984431"],
    },
)
