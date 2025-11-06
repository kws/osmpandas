from collections.abc import Callable
from typing import Literal, NamedTuple

import osmium as osm
import pandas as pd
from osmium.osm import Node, Relation, Way

from osmpandas import data

__all__ = ["ParquetGraphHandler"]


class NodeTuple(NamedTuple):
    id: int
    lon: float
    lat: float
    tags: dict | None


class WayTuple(NamedTuple):
    id: int
    u: int
    v: int
    tags: dict | None


class RelationTuple(NamedTuple):
    id: int
    ref: int
    type: Literal["n", "w", "r"]
    role: str


class TagTuple(NamedTuple):
    owner_id: int
    key: str
    value: str


class ParquetGraphHandler(osm.SimpleHandler):
    def __init__(
        self,
        progress_callback: Callable[[Literal["nodes", "ways", "relations"], int], None]
        | None = None,
    ):
        super().__init__()

        self.nodes: list[NodeTuple] = []
        self.node_tags: list[TagTuple] = []
        self.ways: list[WayTuple] = []
        self.way_tags: list[TagTuple] = []
        self.relation_members: list[RelationTuple] = []
        self.relation_tags: list[TagTuple] = []

        self.progress_callback = progress_callback or (lambda x: None)

    def node(self, n: Node):
        self.nodes.append(NodeTuple(id=n.id, lon=n.location.lon, lat=n.location.lat, tags=None))
        for tag in n.tags:
            self.node_tags.append(TagTuple(owner_id=n.id, key=tag.k, value=tag.v))

        self.progress_callback(nodes=1)

    def way(self, w: Way):
        # only process ways that have nodes
        if len(w.nodes) < 2:
            return
        # Build pairwise edges
        node_ids = [n.ref for n in w.nodes]
        for u, v in zip(node_ids[:-1], node_ids[1:], strict=False):
            self.ways.append(WayTuple(id=w.id, u=u, v=v, tags=None))
        for tag in w.tags:
            self.way_tags.append(TagTuple(owner_id=w.id, key=tag.k, value=tag.v))

        self.progress_callback(ways=1)

    def relation(self, r: Relation):
        relation_id = r.id

        members = [
            RelationTuple(id=relation_id, ref=member.ref, type=member.type, role=member.role)
            for member in r.members
        ]
        self.relation_members.extend(members)
        tags = [TagTuple(owner_id=relation_id, key=tag.k, value=tag.v) for tag in r.tags]
        self.relation_tags.extend(tags)

        self.progress_callback(relations=1)

    def to_osm_data_package(self) -> data.OSMDataPackage:
        df_nodes = (
            pd.DataFrame(self.nodes, columns=["id", "lon", "lat", "tags"])
            .set_index("id")
            .drop(columns=["tags"])
        )
        df_node_tags = pd.DataFrame(self.node_tags, columns=["owner_id", "key", "value"]).set_index(
            ["owner_id", "key"]
        )
        df_ways = (
            pd.DataFrame(self.ways, columns=["id", "u", "v", "tags"])
            .set_index(["id", "u", "v"])
            .drop(columns=["tags"])
        )
        df_way_tags = pd.DataFrame(self.way_tags, columns=["owner_id", "key", "value"]).set_index(
            ["owner_id", "key"]
        )
        df_relation_members = pd.DataFrame(
            self.relation_members, columns=["id", "ref", "type", "role"]
        ).set_index(["id", "ref"])
        df_relation_tags = pd.DataFrame(
            self.relation_tags, columns=["owner_id", "key", "value"]
        ).set_index(["owner_id", "key"])

        return data.OSMDataPackage(
            nodes=df_nodes,
            node_tags=df_node_tags,
            ways=df_ways,
            way_tags=df_way_tags,
            relation_members=df_relation_members,
            relation_tags=df_relation_tags,
        )
