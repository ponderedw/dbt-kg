"""Simple command line interface for DBT Graph Loader."""

import click
from . import load_to_neo4j, load_to_falkordb
from .loaders.neo4j_loader import DBTNeo4jLoader
from .loaders.falkordb_loader import DBTFalkorDBLoader

# Get version from package metadata
try:
    from importlib.metadata import version
    __version__ = version("dbt-graph-loader")
except ImportError:
    # Fallback for Python < 3.8
    from pkg_resources import get_distribution
    __version__ = get_distribution("dbt-graph-loader").version
except Exception:
    # Fallback if package not installed
    __version__ = "0.1.0"


@click.group()
@click.version_option(version=__version__)
def main():
    """DBT Graph Loader - Load DBT metadata into graph databases."""
    pass


@main.command()
@click.option('--uri', required=True, help='Neo4j connection URI')
@click.option('--username', required=True, help='Neo4j username')
@click.option('--password', required=True, help='Neo4j password')
@click.option('--manifest', required=True, help='Path to manifest.json')
@click.option('--catalog', help='Path to catalog.json (optional)')
def neo4j(uri: str, username: str, password: str, manifest: str, catalog: str):
    """Load DBT data into Neo4j."""
    try:
        click.echo("Loading into Neo4j...")
        load_to_neo4j(uri, username, password, manifest, catalog)
        click.echo("✅ Neo4j load completed!")
    except Exception as e:
        click.echo(f"❌ Error: {e}")


@main.command()
@click.option('--host', default='localhost', help='FalkorDB host')
@click.option('--port', default=6379, help='FalkorDB port')
@click.option('--graph-name', default='dbt_graph', help='Graph name')
@click.option('--username', help='FalkorDB username')
@click.option('--password', help='FalkorDB password')
@click.option('--manifest', required=True, help='Path to manifest.json')
@click.option('--catalog', help='Path to catalog.json (optional)')
def falkordb(host: str, port: int, graph_name: str, username: str, password: str, manifest: str, catalog: str):
    """Load DBT data into FalkorDB."""
    try:
        click.echo("Loading into FalkorDB...")
        load_to_falkordb(host, port, graph_name, username, password, manifest, catalog)
        click.echo("✅ FalkorDB load completed!")
    except Exception as e:
        click.echo(f"❌ Error: {e}")


if __name__ == '__main__':
    main()