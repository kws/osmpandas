import logging
import sys
import tarfile
from difflib import get_close_matches
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

DF_NAME_NODE = "node"
DF_NAME_NODE_TAG = "node_tag"
DF_NAME_WAY = "way"
DF_NAME_WAY_TAG = "way_tag"
DF_NAME_RELATION = "relation"
DF_NAME_RELATION_TAG = "relation_tag"

NODE_PARS = (DF_NAME_NODE, DF_NAME_NODE_TAG)
WAY_PARS = (DF_NAME_WAY, DF_NAME_WAY_TAG)
RELATION_PARS = (DF_NAME_RELATION, DF_NAME_RELATION_TAG)

ALL_DF_NAMES = [
    DF_NAME_NODE,
    DF_NAME_NODE_TAG,
    DF_NAME_WAY,
    DF_NAME_WAY_TAG,
    DF_NAME_RELATION,
    DF_NAME_RELATION_TAG,
]


class OSMDataPackage:
    def __init__(self, **kwargs):
        # Check that we get recognised names
        assert set(kwargs.keys()) == set(
            ALL_DF_NAMES
        ), f"Invalid keys: {set(kwargs.keys()) - set(ALL_DF_NAMES)}"
        self.__data = dict(kwargs)

    def __getattr__(self, name: str) -> Any:
        if name in self.__data:
            value = self.__data[name]
            if hasattr(value, "copy"):
                return value.copy()
            return value
        try:
            return object.__getattribute__(self, name)
        except AttributeError:
            close_matches = get_close_matches(name, ALL_DF_NAMES)
            if close_matches:
                # Trim the traceback to hide this frame
                _exc_type, _exc, tb = sys.exc_info()
                if tb and tb.tb_next:
                    tb = tb.tb_next  # drop the current frame
                raise AttributeError(
                    f"Invalid attribute: {name}. Did you mean {close_matches[0]}?"
                ).with_traceback(tb) from None
            raise

    def get_ways(self, ways: pd.DataFrame | None = None) -> gpd.GeoDataFrame:
        """
        The OSM Data Package format stores ways as single segments. This function converts them
        into a GeoDataFrame of LineStrings or MultiLineStrings.
        """
        ways = self.way if ways is None else ways.copy()

        nodes = self.node.set_index("id")
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
        if DF_NAME_NODE and DF_NAME_NODE_TAG in objects:
            logger.debug("Creating node OSMDataFrame")
            df = OSMDataFrame(objects[DF_NAME_NODE])
            df.tag_dataframe = objects.get(DF_NAME_NODE_TAG)
            objects[DF_NAME_NODE] = df
        if DF_NAME_WAY and DF_NAME_WAY_TAG in objects:
            logger.debug("Creating way OSMDataFrame")
            df = OSMDataFrame(objects[DF_NAME_WAY])
            df.tag_dataframe = objects.get(DF_NAME_WAY_TAG)
            objects[DF_NAME_WAY] = df
        if DF_NAME_RELATION and DF_NAME_RELATION_TAG in objects:
            logger.debug("Creating relation OSMDataFrame")
            df = OSMDataFrame(objects[DF_NAME_RELATION])
            df.tag_dataframe = objects.get(DF_NAME_RELATION_TAG)
            objects[DF_NAME_RELATION] = df

        return OSMDataPackage(**objects)

    def __repr__(self) -> str:
        parts = {}
        for obj_key, tags_key in [NODE_PARS, WAY_PARS, RELATION_PARS]:
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
