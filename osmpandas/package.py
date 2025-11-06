import logging
import tarfile
from pathlib import Path
from typing import Any

import geopandas as gpd
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import shapely

from .pandas import OSMDataFrame

logger = logging.getLogger(__name__)


__all__ = ["OSMDataPackage"]


class OSMDataPackage:
    def __init__(self, **kwargs):
        self.__data = dict(kwargs)

    def __getattr__(self, name: str) -> Any:
        if name in self.__data:
            value = self.__data[name]
            if hasattr(value, "copy"):
                return value.copy()
            return value
        return object.__getattribute__(self, name)

    def get_ways(self, ways: pd.DataFrame | None = None) -> gpd.GeoDataFrame:
        """
        The OSM Data Package format stores ways as single segments. This function converts them
        into a GeoDataFrame of LineStrings or MultiLineStrings.
        """
        ways = self.ways if ways is None else ways.copy()

        nodes = self.nodes.set_index("id")
        df = ways.merge(nodes, left_on="u", right_index=True, how="left").merge(
            nodes, left_on="v", right_index=True, how="inner", suffixes=("_u", "_v")
        )

        start_points = np.stack([df.lon_u, df.lat_u], axis=1)
        end_points = np.stack([df.lon_v, df.lat_v], axis=1)
        coords = np.stack([start_points, end_points], axis=1)
        df["geometry"] = shapely.linestrings(coords, handle_nan="error")
        df.drop(columns=["u", "v", "lon_u", "lat_u", "lon_v", "lat_v"], inplace=True)

        df = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")
        df = df.dissolve(by=["id"])
        return df

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
            df = OSMDataFrame(objects["node"])
            df.tag_dataframe = objects.get("node_tag")
            objects["node"] = df
        if "way" and "way_tag" in objects:
            logger.debug("Creating way OSMDataFrame")
            df = OSMDataFrame(objects["way"])
            df.tag_dataframe = objects.get("way_tag")
            objects["way"] = df
        if "relation" and "relation_tag" in objects:
            logger.debug("Creating relation OSMDataFrame")
            df = OSMDataFrame(objects["relation"])
            df.tag_dataframe = objects.get("relation_tag")
            objects["relation"] = df

        return OSMDataPackage(
            nodes=objects.get("node"),
            node_tags=objects.get("node_tag"),
            ways=objects.get("way"),
            way_tags=objects.get("way_tag"),
            relation_members=objects.get("relation"),
            relation_tags=objects.get("relation_tag"),
        )

    def __repr__(self) -> str:
        parts = {}
        for obj_key, tags_key in [
            ("nodes", "node_tags"),
            ("ways", "way_tags"),
            ("relation_members", "relation_tags"),
        ]:
            obj_count = len(self.__data.get(obj_key, pd.DataFrame()))
            tags_count = len(self.__data.get(tags_key, pd.DataFrame()))
            parts[obj_key] = f"{obj_count:,d}/{tags_count:,d}"

        return f"OSMDataPackage({', '.join([f'{k}: {v}' for k, v in parts.items()])})"


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
