import logging
import tarfile
from pathlib import Path
from typing import NamedTuple

import pandas as pd
import pyarrow.parquet as pq

from .pandas import OSMDataFrame

logger = logging.getLogger(__name__)


__all__ = ["OSMDataPackage"]


class OSMDataPackage(NamedTuple):
    nodes: pd.DataFrame
    node_tags: pd.DataFrame
    ways: pd.DataFrame
    way_tags: pd.DataFrame
    relation_members: pd.DataFrame
    relation_tags: pd.DataFrame

    @staticmethod
    def load(path: str | Path) -> "OSMDataPackage":
        logger.debug(f"Loading OSMDataPackage from {path}")

        objects = {}
        with tarfile.open(path, "r") as tar:
            for member in tar:
                if member.isfile():
                    file_name = Path(member.name)
                    fileobj = tar.extractfile(member)
                    table = pq.read_table(fileobj).to_pandas()
                    objects[file_name.stem] = table

        logger.debug(f"Loaded objects: {', '.join(objects.keys())}")
        if "node" and "node_tag" in objects:
            logger.debug("Creating node OSMDataFrame")
            objects["node"] = OSMDataFrame(objects["node"])
            objects["node"].tag_dataframe = objects["node_tag"]
        if "way" and "way_tag" in objects:
            logger.debug("Creating way OSMDataFrame")
            objects["way"] = OSMDataFrame(objects["way"])
            objects["way"].tag_dataframe = objects["way_tag"]
        if "relation" and "relation_tag" in objects:
            logger.debug("Creating relation OSMDataFrame")
            objects["relation"] = OSMDataFrame(objects["relation"])
            objects["relation"].tag_dataframe = objects["relation_tag"]

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


def to_excel(
    output_file: str,
    data: OSMDataPackage,
    *,
    export_nodes: bool = True,
    export_ways: bool = True,
    export_relations: bool = True,
):
    if not (export_nodes | export_ways | export_relations):
        raise ValueError(
            "At least one of export_nodes, export_ways, or export_relations must be True"
        )

    with pd.ExcelWriter(output_file) as writer:
        if export_nodes:
            logger.info("Writing nodes to Excel")
            nodes = data.nodes.osm.expand_tags("*")
            logger.debug("Writing %s rows with %s tags", *nodes.shape)
            nodes.to_excel(writer, sheet_name="nodes")
        if export_ways:
            logger.info("Writing ways to Excel")
            ways = data.ways.osm.expand_tags("*")
            logger.debug("Writing %s rows with %s tags", *ways.shape)
            ways.to_excel(writer, sheet_name="ways")
        if export_relations:
            logger.info("Writing relations to Excel")
            relations = data.relation_members.osm.expand_tags("*")
            logger.debug("Writing %s rows with %s tags", *relations.shape)
            relations.to_excel(writer, sheet_name="relations")
