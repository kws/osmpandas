# osmpandas

Utilities for working with OpenStreetMap (OSM) data in pandas DataFrames.

`osmpandas` provides a convenient way to convert OSM PBF (Protocol Buffer Format) files into structured pandas DataFrames, making it easy to analyze and manipulate OSM data using Python's data science ecosystem.

## Features

- **Convert OSM PBF to pandas**: Parse OSM PBF files and convert them into structured pandas DataFrames
- **Efficient storage**: Store converted data in a custom `.osmpkg` format using Parquet compression (Zstd)
- **CLI tools**: Command-line interface for filtering, converting, and loading OSM data
- **Railway-focused filtering**: Built-in support for filtering railway-related OSM data using osmium-tool
- **Structured data model**: Organized data into nodes, ways, relations, and their associated tags
- **Progress tracking**: Built-in progress bars for long-running operations

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
poetry install
```

## Requirements

- Python >= 3.10
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

Verifies that `osmium-tool` is installed and accessible.

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

Converts an OSM PBF file to the `.osmpkg` format (a tar archive containing Parquet files).

If no output file is specified, it will create `input.osmpkg` in the same directory (replacing `.osm.pbf` extension with `.osmpkg`).

#### Load and inspect osmpkg file

```bash
osmpandas load data.osmpkg
```

Loads a `.osmpkg` file and displays summary statistics about the contained data.

### Python API

#### Convert OSM file to osmpkg format

```python
from osmpandas.osm_parser import convert_osm_to_parquet
from pathlib import Path

# Convert OSM PBF file to osmpkg format
convert_osm_to_parquet("data.osm.pbf", "data.osmpkg")
```

#### Load saved osmpkg file

```python
from osmpandas.package import OSMDataPackage

# Load previously saved data
data = OSMDataPackage.load("data.osmpkg")

# Access the data
print(data.nodes.head())          # Node coordinates
print(data.node_tags.head())      # Node tags (key-value pairs)
print(data.ways.head())           # Way edges (connections between nodes)
print(data.way_tags.head())       # Way tags
print(data.relation_members.head())  # Relation members
print(data.relation_tags.head())  # Relation tags

# Display summary
print(data)  # Shows counts of nodes/tags, ways/tags, relations/tags
```

#### Filter OSM files programmatically

```python
from osmpandas.osmium_wrapper import tags_filter

# Filter with default railway tags
output_path = tags_filter("input.osm.pbf")

# Filter with custom tags
custom_tags = ["nwr/amenity=restaurant", "nwr/amenity=cafe"]
output_path = tags_filter(
    "input.osm.pbf",
    tags=custom_tags,
    file_suffix="-restaurants",
    force=True,  # Overwrite existing file
    progress=True  # Show progress bar
)
```

## Data Structure

The `OSMDataPackage` is a NamedTuple containing six pandas DataFrames:

1. **`nodes`**: Node coordinates
   - Columns: `id`, `lon`, `lat`

2. **`node_tags`**: Tags associated with nodes
   - Columns: `ref` (node ID), `key`, `value`

3. **`ways`**: Way edges (connections between nodes)
   - Columns: `id` (way ID), `u` (source node), `v` (target node)
   - Each way is represented as a series of edges connecting consecutive nodes

4. **`way_tags`**: Tags associated with ways
   - Columns: `ref` (way ID), `key`, `value`

5. **`relation_members`**: Members of relations
   - Columns: `id` (relation ID), `owner_id` (member ID), `type` (n/w/r), `role`

6. **`relation_tags`**: Tags associated with relations
   - Columns: `ref` (relation ID), `key`, `value`

## Examples

### Analyze railway network

```python
from osmpandas.package import OSMDataPackage
from osmpandas.osm_parser import convert_osm_to_parquet

# Step 1: Convert OSM file to osmpkg format
convert_osm_to_parquet("railway.osm.pbf", "railway.osmpkg")

# Step 2: Load the data
data = OSMDataPackage.load("railway.osmpkg")

# Count nodes and ways
print(f"Nodes: {len(data.nodes):,}")
print(f"Ways: {len(data.ways):,}")

# Find all railway stations
stations = data.node_tags[
    (data.node_tags['key'] == 'railway') &
    (data.node_tags['value'] == 'station')
]
print(f"Railway stations: {len(stations):,}")
```

### Filter and convert workflow

```python
from osmpandas.osmium_wrapper import tags_filter
from osmpandas.osm_parser import convert_osm_to_parquet
from osmpandas.package import OSMDataPackage

# Step 1: Filter OSM file for railway data
filtered_file = tags_filter("great-britain-latest.osm.pbf", file_suffix="-railway")

# Step 2: Convert to osmpkg format
convert_osm_to_parquet(filtered_file, "gb-railway.osmpkg")

# Step 3: Load and analyze
data = OSMDataPackage.load("gb-railway.osmpkg")
print(data)
```

### Working with progress callbacks

```python
from osmpandas.osm_parser import convert_osm_to_parquet

def progress_callback(**kwargs):
    """Custom progress callback"""
    for key, value in kwargs.items():
        print(f"{key}: {value:,}")

convert_osm_to_parquet(
    "large-file.osm.pbf",
    "output.osmpkg",
    progress_callback=progress_callback
)
```

## Project Structure

```
osmpandas/
├── osmpandas/
│   ├── __init__.py          # Package exports
│   ├── __main__.py          # Entry point
│   ├── _cli.py              # CLI commands
│   ├── package.py           # OSMDataPackage class
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
