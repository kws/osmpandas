import io
import logging
import tarfile
import tempfile
from collections.abc import Callable, NamedTuple
from pathlib import Path
from time import time

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
        with tarfile.open(path, "r") as tar:
            for member in tar:
                if member.name == "nodes.parquet":
                    df_nodes = pq.read_table(member.fileobj).to_pandas()
                elif member.name == "node_tags.parquet":
                    df_node_tags = pq.read_table(member.fileobj).to_pandas()
                elif member.name == "ways.parquet":
                    df_ways = pq.read_table(member.fileobj).to_pandas()
                elif member.name == "way_tags.parquet":
                    df_way_tags = pq.read_table(member.fileobj).to_pandas()
                elif member.name == "relation_members.parquet":
                    df_relation_members = pq.read_table(member.fileobj).to_pandas()
                elif member.name == "relation_tags.parquet":
                    df_relation_tags = pq.read_table(member.fileobj).to_pandas()
                else:
                    logger.warning(f"Unknown member: {member.name}")
        return OSMDataPackage(
            nodes=df_nodes,
            node_tags=df_node_tags,
            ways=df_ways,
            way_tags=df_way_tags,
            relation_members=df_relation_members,
            relation_tags=df_relation_tags,
        )

    @staticmethod
    def from_osm(
        pbf_file: str | Path, progress_callback: Callable[[str, int], None] | None = None
    ) -> "OSMDataPackage":
        from osmpandas.osm_parser import ParquetGraphHandler

        handler = ParquetGraphHandler(progress_callback=progress_callback)
        handler.apply_file(pbf_file, locations=True)
        return handler.to_osm_data_package()

    def __repr__(self) -> str:
        return (
            f"OSMDataPackage("
            f"nodes={len(self.nodes)}, "
            f"node_tags={len(self.node_tags)}, "
            f"ways={len(self.ways)}, "
            f"way_tags={len(self.way_tags)}, "
            f"relation_members={len(self.relation_members)}, "
            f"relation_tags={len(self.relation_tags)})"
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
