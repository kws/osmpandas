# osmpandas

Utilities for working with OpenStreetMap (OSM) data in pandas DataFrames.

`osmpandas` provides a convenient way to convert OSM PBF (Protocol Buffer Format) files into structured pandas DataFrames, making it easy to analyze and manipulate OSM data using Python's data science ecosystem.

## Features

- **Convert OSM PBF to pandas**: Parse OSM PBF files and convert them into structured pandas DataFrames
- **Efficient storage**: Store converted data in a custom `.osmpkg` format using Parquet compression
- **CLI tools**: Command-line interface for filtering and converting OSM data
- **Railway-focused filtering**: Built-in support for filtering railway-related OSM data using osmium
- **Structured data model**: Organized data into nodes, ways, relations, and their associated tags

## Installation

### Using pipx (Recommended)

Install directly from GitHub using `pipx`:

```bash
pipx install git+https://github.com/kws/osmpandas.git
```

### Using pip

```bash
pip install git+https://github.com/kws/osmpandas.git
```

### Development Installation

Clone the repository and install in editable mode:

```bash
git clone https://github.com/kws/osmpandas.git
cd osmpandas
pip install -e .
```

## Requirements

- Python >= 3.12
- [osmium-tool](https://osmcode.org/osmium-tool/) - Must be installed separately on your system

### Installing osmium-tool

**macOS:**
```bash
brew install osmium-tool
```

**Ubuntu/Debian:**
```bash
sudo apt-get install osmium-tool
```

**Other systems:**
See the [osmium-tool installation guide](https://osmcode.org/osmium-tool/manual.html#installation).

## Quick Start

### Command Line Interface

#### Check osmium installation

```bash
osmpandas check
```

#### Filter OSM data (railway by default)

```bash
osmpandas filter input.osm.pbf
```

This creates a filtered file `input-railway.osm.pbf` containing only railway-related features.

Options:
- `--force`: Force overwrite of existing output file
- `--no-progress`: Hide the progress bar
- `--file-suffix`: Custom suffix for the output file (default: `-railway`)

#### Convert OSM PBF to osmpkg format

```bash
osmpandas convert input.osm.pbf output.osmpkg
```

If no output file is specified, it will create `input.osmpkg` in the same directory.

### Python API

#### Convert OSM file to pandas DataFrames

```python
from osmpandas import OSMDataPackage
from pathlib import Path

# Convert OSM PBF file to OSMDataPackage
data = OSMDataPackage.from_osm("data.osm.pbf")

# Access the data
print(data.nodes.head())          # Node coordinates
print(data.node_tags.head())      # Node tags (key-value pairs)
print(data.ways.head())           # Way edges (connections between nodes)
print(data.way_tags.head())       # Way tags
print(data.relation_members.head())  # Relation members
print(data.relation_tags.head())  # Relation tags

# Save to osmpkg format
data.save("data.osmpkg")
```

#### Load saved osmpkg file

```python
from osmpandas import OSMDataPackage

# Load previously saved data
data = OSMDataPackage.load("data.osmpkg")

# Work with the DataFrames
nodes_df = data.nodes
ways_df = data.ways
```

#### Filter OSM files programmatically

```python
from osmpandas.osmium_wrapper import tags_filter

# Filter with default railway tags
output_path = tags_filter("input.osm.pbf")

# Filter with custom tags
custom_tags = ["nwr/amenity=restaurant", "nwr/amenity=cafe"]
output_path = tags_filter("input.osm.pbf", tags=custom_tags, file_suffix="-restaurants")
```

## Data Structure

The `OSMDataPackage` contains six pandas DataFrames:

1. **`nodes`**: Node coordinates
   - Index: `id` (node ID)
   - Columns: `lon`, `lat`

2. **`node_tags`**: Tags associated with nodes
   - Index: `owner_id`, `key`
   - Columns: `value`

3. **`ways`**: Way edges (connections between nodes)
   - Index: `id`, `u`, `v` (way ID, source node, target node)
   - Columns: (none)

4. **`way_tags`**: Tags associated with ways
   - Index: `owner_id`, `key`
   - Columns: `value`

5. **`relation_members`**: Members of relations
   - Index: `id`, `ref`
   - Columns: `type` (n/w/r), `role`

6. **`relation_tags`**: Tags associated with relations
   - Index: `owner_id`, `key`
   - Columns: `value`

## Examples

### Analyze railway network

```python
from osmpandas import OSMDataPackage

# Load railway-filtered OSM data
data = OSMDataPackage.from_osm("railway.osm.pbf")

# Count nodes and ways
print(f"Nodes: {len(data.nodes)}")
print(f"Ways: {len(data.ways)}")

# Find all railway stations
stations = data.node_tags[
    (data.node_tags.index.get_level_values('key') == 'railway') &
    (data.node_tags['value'] == 'station')
]
print(f"Railway stations: {len(stations)}")
```

### Filter and convert workflow

```python
from osmpandas.osmium_wrapper import tags_filter
from osmpandas import OSMDataPackage

# Step 1: Filter OSM file for railway data
filtered_file = tags_filter("great-britain-latest.osm.pbf", file_suffix="-railway")

# Step 2: Convert to pandas format
data = OSMDataPackage.from_osm(filtered_file)

# Step 3: Save for later use
data.save("gb-railway.osmpkg")
```

## Project Structure

```
osmpandas/
├── osmpandas/
│   ├── __init__.py          # Package exports
│   ├── __main__.py          # Entry point
│   ├── _cli.py              # CLI commands
│   ├── data.py              # OSMDataPackage class
│   ├── osm_parser.py        # OSM PBF parser
│   └── osmium_wrapper.py    # osmium-tool wrapper
├── pyproject.toml           # Project configuration
└── README.md                # This file
```

## Development

### Setup development environment

```bash
git clone https://github.com/kws/osmpandas.git
cd osmpandas
poetry install
```

### Running tests

```bash
pytest
```

### Code formatting and linting

The project uses `ruff` for formatting and linting:

```bash
ruff check .
ruff format .
```

## License

MIT License - see [LICENSE](LICENSE) file for details.

## Author

Kaj Siebert - [kaj@k-si.com](mailto:kaj@k-si.com)

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
