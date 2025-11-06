import io
import logging
import tarfile
import tempfile
from pathlib import Path
from time import time
from typing import NamedTuple

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)


__all__ = ["OSMDataPackage"]


class OSMDataPackage(NamedTuple):
    nodes: pd.DataFrame
    node_tags: pd.DataFrame
    ways: pd.DataFrame
    way_tags: pd.DataFrame
    relation_members: pd.DataFrame
    relation_tags: pd.DataFrame

    def save(self, path: str | Path):
        with tarfile.open(path, "w") as tar:
            _add_parquet_to_tar(tar, "nodes.parquet", self.nodes)
            _add_parquet_to_tar(tar, "node_tags.parquet", self.node_tags)
            _add_parquet_to_tar(tar, "ways.parquet", self.ways)
            _add_parquet_to_tar(tar, "way_tags.parquet", self.way_tags)
            _add_parquet_to_tar(tar, "relation_members.parquet", self.relation_members)
            _add_parquet_to_tar(tar, "relation_tags.parquet", self.relation_tags)

    @staticmethod
    def load(path: str | Path) -> "OSMDataPackage":
        objects = {}
        with tarfile.open(path, "r") as tar:
            for member in tar:
                if member.isfile():
                    file_name = Path(member.name)
                    fileobj = tar.extractfile(member)
                    table = pq.read_table(fileobj).to_pandas()
                    objects[file_name.stem] = table

        print(objects.keys())
        return OSMDataPackage(
            nodes=objects.get("node"),
            node_tags=objects.get("node_tag"),
            ways=objects.get("way"),
            way_tags=objects.get("way_tag"),
            relation_members=objects.get("relation"),
            relation_tags=objects.get("relation_tag"),
        )

    def __repr__(self) -> str:
        return (
            f"OSMDataPackage("
            f"nodes/tags={len(self.nodes):,d}/{len(self.node_tags):,d}, "
            f"ways/tags={len(self.ways):,d}/{len(self.way_tags):,d}, "
            f"relations/tags={len(self.relation_members):,d}/{len(self.relation_tags):,d}"
            f")"
        )


def _add_parquet_to_tar(
    tar: tarfile.TarFile, arcname: str, df: pd.DataFrame, mem_limit=64 * 1024 * 1024
):
    with tempfile.SpooledTemporaryFile(max_size=mem_limit) as buf:
        table = pa.Table.from_pandas(df, preserve_index=True)
        pq.write_table(table, buf)
        buf.seek(0, io.SEEK_END)
        size = buf.tell()
        buf.seek(0)

        info = tarfile.TarInfo(arcname)
        info.size = size
        info.mtime = int(time())

        buf.seek(0, 2)
        info.size = buf.tell()
        buf.seek(0)
        tar.addfile(info, fileobj=buf)
