import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

import pandas as pd
import pysradb as sra
from latch.registry.project import Project
from latch.registry.table import Table
from latch.resources.launch_plan import LaunchPlan
from latch.resources.map_tasks import map_task
from latch.resources.tasks import large_task, small_task
from latch.resources.workflow import workflow
from latch.types import Fork
from latch.types import ForkBranch as OGForkBranch
from latch.types import LatchAuthor, LatchDir, LatchMetadata, LatchParameter, Params
from latch.types.metadata import FlowBase


@dataclass(frozen=True, init=False)
class ForkBranch(OGForkBranch):
    def __init__(
        self,
        display_name: str,
        *flow: FlowBase,
        _tmp_unwrap_optionals: Optional[List[str]] = None,
    ):
        object.__setattr__(self, "display_name", display_name)
        object.__setattr__(self, "flow", list(flow))
        if _tmp_unwrap_optionals is not None:
            object.__setattr__(self, "_tmp_unwrap_optionals", _tmp_unwrap_optionals)


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
def download(data: DownloadData) -> Optional[LatchDir]:
    output_dir = Path("downloaded")
    output_dir.mkdir(exist_ok=True)
    try:
        subprocess.run(
            [
                "fasterq-dump",
                f"{data.sra_id}",
                "--outdir",
                str(output_dir),
                "--split-files",
                "--include-technical",
                "--verbose",
                "--progress",
                "--mem",
                f"{170_000}MB",
                "--threads",
                "80",
            ],
            check=True,
        )
        return LatchDir(output_dir, data.output_location.remote_path)
    except subprocess.CalledProcessError:
        print(f"Failed to download {data.sra_id} - skipping.")
        return None


@small_task
def write_to_registry(
    download_type_fork: str,
    table_type_fork: str,
    new_table_project_id: Optional[str],
    new_table_name: Optional[str],
    sra_project: Optional[str],
    output_location: LatchDir,
    existing_table_id: Optional[str],
    # hack to make sure this runs after the map task is done
    barrier: List[Optional[LatchDir]],
) -> LatchDir:
    if download_type_fork == "sra_ids" or sra_project is None:
        return output_location
    if table_type_fork == "no_table":
        return output_location
    if existing_table_id is None and table_type_fork == "use_existing":
        return output_location
    if (
        new_table_project_id is None or new_table_name is None
    ) and table_type_fork == "create_new":
        return output_location

    sra_db = sra.SRAweb()
    df: pd.DataFrame = sra_db.sra_metadata(sra_project)

    sra_index = df.columns.get_loc("run_accession")
    srx_index = df.columns.get_loc("experiment_accession")

    columns_to_idxs = {str(column): df.columns.get_loc(column) for column in df}

    t = None
    if table_type_fork == "use_existing":
        assert existing_table_id is not None

        t = Table(existing_table_id)
    elif table_type_fork == "create_new":
        assert new_table_project_id is not None
        assert new_table_name is not None

        p = Project(new_table_project_id)
        with p.update() as u:
            u.upsert_table(new_table_name)
        tables = p.list_tables()
        for x in tables:
            if x.get_display_name() == new_table_name:
                t = x
                break

    if t is None:
        return output_location

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
        "table_type_fork": LatchParameter(
            display_name="Registry Output Type",
            description="Either write to an existing table or create a new one",
        ),
        "new_table_project_id": LatchParameter(
            display_name="Registry Project ID",
            description="ID of the project to create the new table in.",
        ),
        "new_table_name": LatchParameter(
            display_name="Registry Table Name",
            description="Name of the new table.",
        ),
        "sra_ids": LatchParameter(
            display_name="SRA Run IDs",
            description="A list of SRA Run IDs to download",
            batch_table_column=True,
        ),
        "sra_project": LatchParameter(
            display_name="SRA Project ID",
            description="An SRA Project ID to download",
            batch_table_column=True,
        ),
        "output_location": LatchParameter(
            display_name="Output Location",
            description="Location to download the SRA files to",
            batch_table_column=True,
            output=True,
        ),
        "existing_table_id": LatchParameter(
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
                Params("sra_project"),
                Fork(
                    fork="table_type_fork",
                    display_name="",
                    use_existing=ForkBranch(
                        "Use Existing Table",
                        Params("existing_table_id"),
                        _tmp_unwrap_optionals=["existing_table_id"],
                    ),
                    create_new=ForkBranch(
                        "Create New Table",
                        Params("new_table_project_id", "new_table_name"),
                        _tmp_unwrap_optionals=[
                            "new_table_project_id",
                            "new_table_name",
                        ],
                    ),
                    no_table=ForkBranch(
                        "No Registry Output",
                    ),
                ),
                _tmp_unwrap_optionals=["sra_project"],
            ),
        ),
        Params("output_location"),
    ],
)


@workflow(metadata)
def sra_fetcher(
    download_type_fork: str,
    table_type_fork: str,
    new_table_project_id: Optional[str],
    new_table_name: Optional[str],
    sra_project: Optional[str],
    sra_ids: Optional[List[str]],
    output_location: LatchDir = LatchDir("latch:///SRA FASTQs/"),
    existing_table_id: Optional[str] = None,
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
        table_type_fork=table_type_fork,
        new_table_project_id=new_table_project_id,
        new_table_name=new_table_name,
        sra_project=sra_project,
        output_location=output_location,
        existing_table_id=existing_table_id,
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
