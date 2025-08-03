# DBT Graph Loader

> Transform your DBT project's lineage and metadata into queryable knowledge graphs

DBT Graph Loader is a Python library that loads DBT (Data Build Tool) metadata into graph databases, enabling you to explore, query, and visualize your data lineage as an interactive knowledge graph.

## 🚀 Features

- **🔄 Multiple Graph Databases**: Native support for Neo4j and FalkorDB
- **📊 Complete DBT Coverage**: Models, sources, tests, macros, seeds, snapshots, and operations
- **🔗 Rich Relationships**: Dependencies, references, macro usage, and test coverage mapping
- **📁 Flexible Input**: Load from `manifest.json` and `catalog.json` files or strings
- **🛠️ Easy CLI**: Simple command-line interface for batch operations
- **🐍 Python API**: Programmatic access for integration into data pipelines
- **📈 Graph Analytics**: Built-in statistics and insights about your data lineage
- **🐳 Docker Ready**: Easy containerization and deployment

## 📦 Installation

### Using Poetry (Recommended)
```bash
poetry add dbt-graph-loader
```

### Using pip
```bash
pip install dbt-graph-loader
```

### Development Installation
```bash
# Clone the repository
git clone https://github.com/hipposys-ltd/dbt-graph-loader.git
cd dbt-graph-loader

# Install with Poetry
poetry install

# Or with pip
pip install -e .
```

## 🎯 Quick Start

### 1. Generate DBT Metadata Files

First, ensure you have the required DBT files:

```bash
cd your-dbt-project
dbt compile  # Generates manifest.json
dbt docs generate  # Generates catalog.json (optional but recommended)
```

### 2. Load into Neo4j

```bash
# Using CLI
dbt-graph-loader neo4j \
    --uri bolt://localhost:7687 \
    --username neo4j \
    --password your_password \
    --manifest target/manifest.json \
    --catalog target/catalog.json
```

### 3. Load into FalkorDB

```bash
# Using CLI
dbt-graph-loader falkordb \
    --host localhost \
    --port 6379 \
    --graph-name my_dbt_lineage \
    --manifest target/manifest.json \
    --catalog target/catalog.json
```

## 📋 Supported DBT Resources

| Resource Type | Description | Properties Captured |
|---------------|-------------|-------------------|
| **Models** | DBT models and their transformations | Materialization, dependencies, descriptions, tags |
| **Sources** | External data sources | Freshness rules, schemas, descriptions |
| **Seeds** | CSV files loaded as tables | File metadata, configurations |
| **Snapshots** | Slowly changing dimension tables | Strategies, unique keys, timestamps |
| **Tests** | Data quality tests | Severity levels, test parameters, attached nodes |
| **Macros** | Reusable SQL code blocks | Arguments, package info, usage patterns |
| **Operations** | Pre/post hooks and run operations | Execution context, dependencies |

## 🔗 Graph Relationships

The loader creates rich relationships between your DBT resources:

- **`DEPENDS_ON`**: Direct dependencies between any resources
- **`REFERENCES`**: Model-to-model references via `ref()` functions
- **`USES_MACRO`**: Macro usage relationships
- **`TESTS`**: Test-to-resource relationships

## 🛠️ Usage

### Command Line Interface

#### Neo4j Options
```bash
dbt-graph-loader neo4j --help

Options:
  --uri TEXT        Neo4j connection URI (required)
  --username TEXT   Neo4j username (required)  
  --password TEXT   Neo4j password (required)
  --manifest TEXT   Path to manifest.json (required)
  --catalog TEXT    Path to catalog.json (optional)
```

#### FalkorDB Options
```bash
dbt-graph-loader falkordb --help

Options:
  --host TEXT        FalkorDB host (default: localhost)
  --port INTEGER     FalkorDB port (default: 6379)
  --graph-name TEXT  Graph name (default: dbt_graph)
  --username TEXT    FalkorDB username (optional)
  --password TEXT    FalkorDB password (optional)
  --manifest TEXT    Path to manifest.json (required)
  --catalog TEXT     Path to catalog.json (optional)
```

### Python API

#### Neo4j Integration

```python
from dbt_graph_loader.loaders.neo4j_loader import DBTNeo4jLoader

# Initialize the loader
loader = DBTNeo4jLoader(
    neo4j_uri="bolt://localhost:7687",
    username="neo4j",
    password="your_password"
)

try:
    # Load from files
    loader.load_dbt_to_neo4j_from_files(
        manifest_path="target/manifest.json",
        catalog_path="target/catalog.json"
    )
    
    # View statistics
    loader.get_graph_stats()
    
finally:
    loader.close()
```

#### FalkorDB Integration

```python
from dbt_graph_loader.loaders.falkordb_loader import DBTFalkorDBLoader

# Initialize the loader
loader = DBTFalkorDBLoader(
    host="localhost",
    port=6379,
    graph_name="dbt_lineage",
    username="your_username",  # if auth enabled
    password="your_password"   # if auth enabled
)

try:
    # Load from files
    loader.load_dbt_to_falkordb(
        manifest_path="target/manifest.json",
        catalog_path="target/catalog.json"
    )
    
    # Load from strings (useful for APIs)
    with open("target/manifest.json") as f:
        manifest_str = f.read()
    with open("target/catalog.json") as f:
        catalog_str = f.read()
        
    loader.load_dbt_to_falkordb_from_strings(manifest_str, catalog_str)
    
    # View statistics
    loader.get_graph_stats()
    
finally:
    loader.close()
```

#### Convenience Functions

```python
from dbt_graph_loader import load_to_neo4j, load_to_falkordb

# Simple Neo4j loading
load_to_neo4j(
    uri="bolt://localhost:7687",
    username="neo4j",
    password="password",
    manifest_path="target/manifest.json",
    catalog_path="target/catalog.json"
)

# Simple FalkorDB loading
load_to_falkordb(
    host="localhost",
    port=6379,
    graph_name="dbt_lineage",
    manifest_path="target/manifest.json",
    catalog_path="target/catalog.json"
)
```

## 🔍 Example Queries

Once your DBT metadata is loaded, you can query the graph using Cypher (Neo4j) or OpenCypher (FalkorDB).

### Neo4j Cypher Examples

```cypher
// Find all models that depend on a specific source
MATCH (m:Model)-[:DEPENDS_ON]->(s:Source {name: "raw_data.customers"})
RETURN m.name, m.materialized, m.description

// Get the complete downstream lineage from a model
MATCH path = (start:Model {name: "dim_customers"})-[:DEPENDS_ON*]->(downstream)
RETURN path

// Find models without any tests
MATCH (m:Model)
WHERE NOT EXISTS {
    MATCH (t:Test)-[:TESTS]->(m)
}
RETURN m.name, m.schema, m.materialized

// Identify the most referenced models
MATCH (m:Model)<-[:REFERENCES]-(referencing)
RETURN m.name, count(referencing) as reference_count
ORDER BY reference_count DESC
LIMIT 10

// Find macro usage patterns
MATCH (m:Model)-[:USES_MACRO]->(macro:Macro)
RETURN macro.name, count(m) as usage_count
ORDER BY usage_count DESC

// Discover circular dependencies (if any)
MATCH path = (n)-[:DEPENDS_ON*]->(n)
WHERE length(path) > 1
RETURN path
```

### FalkorDB OpenCypher Examples

```cypher
// Models by materialization type
MATCH (m:Model)
RETURN m.materialized, count(m) as model_count
ORDER BY model_count DESC

// Source freshness analysis
MATCH (s:Source)
WHERE s.freshness_warn_after IS NOT NULL
RETURN s.name, s.freshness_warn_after, s.freshness_error_after

// Test coverage by schema
MATCH (m:Model)
OPTIONAL MATCH (t:Test)-[:TESTS]->(m)
RETURN m.schema, 
       count(m) as total_models,
       count(t) as total_tests,
       round(100.0 * count(t) / count(m), 2) as test_coverage_pct
ORDER BY test_coverage_pct DESC
```

## 🐳 Docker Integration

### FastAPI Integration Example

```python
from fastapi import FastAPI, UploadFile, File
from dbt_graph_loader.loaders.neo4j_loader import DBTNeo4jLoader
import os

app = FastAPI()

@app.post("/upload-dbt-metadata/")
async def upload_dbt_metadata(
    manifest_file: UploadFile = File(...),
    catalog_file: UploadFile = File(...)
):
    manifest_content = await manifest_file.read()
    catalog_content = await catalog_file.read()
    
    loader = DBTNeo4jLoader(
        neo4j_uri=os.getenv("NEO4J_URI"),
        username=os.getenv("NEO4J_USERNAME"),
        password=os.getenv("NEO4J_PASSWORD")
    )
    
    try:
        loader.load_dbt_to_neo4j_from_strings(
            manifest_content.decode('utf-8'),
            catalog_content.decode('utf-8')
        )
        return {"status": "success", "message": "DBT metadata loaded"}
    finally:
        loader.close()
```
## 📊 Graph Schema

### Node Properties

**Models**
- `unique_id`, `name`, `database`, `schema`, `materialized`
- `description`, `tags`, `package_name`, `path`, `enabled`
- `language`, `checksum`, `access`, `relation_name`

**Sources**  
- `unique_id`, `name`, `source_name`, `identifier`
- `database`, `schema`, `description`, `loader`
- `freshness_warn_after`, `freshness_error_after`, `columns`

**Tests**
- `unique_id`, `name`, `column_name`, `severity`, `enabled`
- `test_name`, `test_kwargs`, `package_name`

**Macros**
- `unique_id`, `name`, `package_name`, `path`
- `description`, `arguments`

**Seeds**
- `unique_id`, `name`, `database`, `schema`, `path`
- `delimiter`, `materialized`, `enabled`

**Snapshots** 
- `unique_id`, `name`, `database`, `schema`, `strategy`
- `unique_key`, `updated_at`, `materialized`


## 🧪 Development

### Setup Development Environment

```bash
# Clone repository
git clone https://github.com/hipposys-ltd/dbt-graph-loader.git

# Install dependencies
poetry install

# Build package
poetry build
```


## 📋 Prerequisites

### For Neo4j
- Neo4j 4.0+ (local installation or cloud)
- Python 3.8+

### For FalkorDB  
- FalkorDB instance (Redis-compatible graph database)
- Python 3.8+

### DBT Requirements
- DBT project with generated `manifest.json` (required)
- Generated `catalog.json` (optional but recommended for richer metadata)
