"""DBT Graph Loader - Load DBT metadata into graph databases."""

from .loaders.neo4j_loader import DBTNeo4jLoader
from .loaders.falkordb_loader import DBTFalkorDBLoader


def load_to_neo4j(uri: str, username: str, password: str, manifest_path: str, catalog_path: str = None):
    """Convenience function to load DBT data into Neo4j."""
    loader = DBTNeo4jLoader(uri, username, password)
    try:
        loader.load_dbt_to_neo4j_from_files(manifest_path, catalog_path)
        loader.get_graph_stats()
    finally:
        loader.close()


def load_to_falkordb(host: str = 'localhost', port: int = 6379, graph_name: str = 'dbt_graph',
                    username: str = None, password: str = None, manifest_path: str = None, 
                    catalog_path: str = None):
    """Convenience function to load DBT data into FalkorDB."""
    loader = DBTFalkorDBLoader(host, port, graph_name, username, password)
    # try:
    loader.load_dbt_to_falkordb(manifest_path, catalog_path)
    loader.get_graph_stats()
    # finally:
    #     loader.close()

__all__ = [
    'DBTNeo4jLoader',
    'DBTFalkorDBLoader',
    'load_to_neo4j',
    'load_to_falkordb',
]