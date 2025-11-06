import tarfile
import tempfile
from collections.abc import Callable
from pathlib import Path

import osmium as osm
import pyarrow as pa
import pyarrow.parquet as pq

NODE_SCHEMA = pa.schema(
    [
        ("id", pa.int64()),
        ("lon", pa.float64()),
        ("lat", pa.float64()),
    ]
)
WAY_SCHEMA = pa.schema(
    [
        ("id", pa.int64()),
        ("u", pa.int64()),
        ("v", pa.int64()),
    ]
)
RELATION_SCHEMA = pa.schema(
    [
        ("id", pa.int64()),
        ("owner_id", pa.int64()),
        ("type", pa.utf8()),
        ("role", pa.utf8()),
    ]
)
TAG_SCHEMA = pa.schema(
    [
        ("ref", pa.int64()),
        ("key", pa.utf8()),
        ("value", pa.utf8()),
    ]
)


class TableWriter:
    def __init__(
        self,
        path: Path,
        name: str,
        schema: pa.Schema,
        *,
        batch: int = 500_000,
        progress_callback: Callable[..., None] | None = None,
    ):
        self._progress_callback = progress_callback
        self._log_batch = 1_000
        self._log_counter = 0
        self._path = path
        self._batch = batch
        self._name = name
        self._schema = schema
        self._writer = pq.ParquetWriter(
            (path / name).with_suffix(".parquet"), schema, compression="zstd"
        )
        self._data = {k: [] for k in schema.names}

    def add(self, *args):
        for k, v in zip(self._schema.names, args, strict=True):
            self._data[k].append(v)
        if self._progress_callback is not None:
            self._log_counter += 1
            if self._log_counter >= self._log_batch:
                self._log_progress()
        if len(self._data[k]) >= self._batch:
            self.flush()

    def _log_progress(self):
        self._progress_callback(**{self._name: self._log_counter})
        self._log_counter = 0

    def flush(self):
        n = len(self._data[self._schema.names[0]])
        if n == 0:
            return
        batch = pa.Table.from_arrays(
            [pa.array(self._data[k]) for k in self._schema.names], names=self._schema.names
        )
        self._writer.write_table(batch)
        self._data = {k: [] for k in self._schema.names}
        if self._progress_callback is not None:
            self._log_progress()

    def close(self):
        self.flush()
        self._writer.close()


class ObjectWriter:
    def __init__(
        self,
        path,
        name,
        schema,
        *,
        batch: int = 500_000,
        progress_callback: Callable[..., None] | None = None,
    ):
        self._object_writer = TableWriter(
            path, name, schema, batch=batch, progress_callback=progress_callback
        )
        self._tag_writer = TableWriter(
            path,
            f"{name}_tag",
            TAG_SCHEMA,
            batch=batch,
        )

    def add(self, *args):
        self._object_writer.add(*args)

    def add_tag(self, *args):
        self._tag_writer.add(*args)

    def close(self):
        self._object_writer.close()
        self._tag_writer.close()


class StreamHandler(osm.SimpleHandler):
    def __init__(
        self, path, *, progress_callback: Callable[..., None] | None = None, batch=500_000
    ):
        super().__init__()
        self.progress_callback = progress_callback
        self.batch = batch
        self.node_writer = ObjectWriter(
            path, "node", NODE_SCHEMA, batch=batch, progress_callback=progress_callback
        )
        self.way_writer = ObjectWriter(
            path, "way", WAY_SCHEMA, batch=batch, progress_callback=progress_callback
        )
        self.relation_writer = ObjectWriter(
            path, "relation", RELATION_SCHEMA, batch=batch, progress_callback=progress_callback
        )

    def node(self, n):
        if not n.location.valid():  # safety
            return
        for t in n.tags:
            self.node_writer.add_tag(n.id, t.k, t.v)
        self.node_writer.add(n.id, n.location.lon, n.location.lat)

    def way(self, w):
        if len(w.nodes) < 2:
            return

        for t in w.tags:
            self.way_writer.add_tag(w.id, t.k, t.v)

        node_ids = [nd.ref for nd in w.nodes]
        wid = w.id
        for u, v in zip(node_ids[:-1], node_ids[1:], strict=True):
            self.way_writer.add(wid, u, v)

    def relation(self, r):
        for t in r.tags:
            self.relation_writer.add_tag(r.id, t.k, t.v)
        for m in r.members:
            self.relation_writer.add(r.id, m.ref, m.type, m.role)

    def close(self):
        self.node_writer.close()
        self.way_writer.close()
        self.relation_writer.close()


def convert_osm_to_parquet(
    pbf_file: str | Path,
    output_file: str | Path,
    progress_callback: Callable[..., None] | None = None,
):
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        handler = StreamHandler(tmpdir, progress_callback=progress_callback)
        handler.apply_file(pbf_file, locations=True)
        handler.close()

        with tarfile.open(output_file, "w") as tar:
            for name in sorted(tmpdir.glob("*.parquet")):
                tar.add(name, arcname=name.name)
